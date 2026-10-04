package kafka

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	config "github.com/osrg/gobgp/v4/pkg/config/kafka"
	"github.com/osrg/gobgp/v4/pkg/event"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"golang.org/x/time/rate"
)

const maxInFlight = 256

type sink struct {
	windowSize                         int // zero uses maxInFlight; smaller windows support comparisons in benchmarks.
	destination                        destination
	producers                          map[string]producer
	streamID                           string
	snapshotFailed                     atomic.Bool
	bootstrapCapture                   atomic.Uint32 // 0 collecting, 1 ingress loss, 2 capture complete
	snapshotRecords                    int64
	serializer                         event.Serializer
	name                               string
	config                             config.Sink
	matcher                            *event.Matcher
	store                              *store
	producer                           producer
	metrics                            *metrics
	logger                             *slog.Logger
	logs                               *rate.Limiter
	queue                              chan *record
	wake                               chan struct{}
	writerDone, publisherDone, drained chan struct{}
	storageClosed                      chan struct{}
	storageErr                         error
	ctx                                context.Context
	cancel                             context.CancelFunc
	admission                          sync.RWMutex
	accepting                          bool
	stopOnce, closeOnce                sync.Once
	healthy, up                        atomic.Bool
	collectorID                        string
}

func (s *sink) Name() string                { return s.name }
func (s *sink) Match(m event.Metadata) bool { return s.matcher.Match(m) }
func (s *sink) Offer(v event.Observation) bool {
	value, err := s.serializer.Serialize(v.Message)
	if err != nil {
		s.snapshotFailed.Store(true)
		s.metrics.serializationErrors.WithLabelValues(labels(s, v.Metadata)...).Inc()
		return false
	}
	return s.offer(v, value, uuid.NewString())
}

func (s *sink) offer(v event.Observation, value []byte, id string) bool {
	r := &record{Version: 1, Value: value, Metadata: v.Metadata, Destination: &s.destination}
	peer := v.Metadata.PeerAddress
	if peer == "" {
		peer = "local"
	}
	switch s.config.Key.Strategy {
	case "peer":
		r.Key = []byte(peer)
	case "peer+family":
		family := v.Metadata.Family
		if family == "" {
			family = "none"
		}
		r.Key = []byte(peer + "|" + family)
	}
	r.Headers = []kgo.RecordHeader{
		{Key: "content-type", Value: []byte(s.serializer.ContentType())},
		{Key: "serializer", Value: []byte(s.config.Format)},
		{Key: "schema", Value: []byte(v.Message.ProtoReflect().Descriptor().FullName())},
		{Key: "sink", Value: []byte(s.name)},
		{Key: "gobgp-version", Value: []byte(goBGPVersion())},
		{Key: "source", Value: []byte(v.Metadata.Source)},
		{Key: "event-type", Value: []byte(v.Metadata.EventType)},
		{Key: "observed-at", Value: []byte(v.Metadata.ObservedAt.UTC().Format(time.RFC3339Nano))},
		{Key: "event-id", Value: []byte(id)},
	}
	r.Headers = append(r.Headers, kgo.RecordHeader{Key: "stream-id", Value: []byte(s.streamID)})
	if v.Metadata.SnapshotID != "" {
		r.Headers = append(r.Headers, kgo.RecordHeader{Key: "snapshot-id", Value: []byte(v.Metadata.SnapshotID)}, kgo.RecordHeader{Key: "snapshot-phase", Value: []byte(v.Metadata.SnapshotPhase)})
	}
	if s.collectorID != "" {
		r.Headers = append(r.Headers, kgo.RecordHeader{Key: "collector-id", Value: []byte(s.collectorID)})
	}
	s.admission.RLock()
	defer s.admission.RUnlock()
	if s.accepting && s.healthy.Load() {
		select {
		case s.queue <- r:
			return true
		default:
			s.logError("Kafka sink queue full", fmt.Errorf("non-blocking admission discarded a record"))
		}
	}
	s.snapshotFailed.Store(true)
	s.metrics.dropped.WithLabelValues(labels(s, r.Metadata)...).Inc()
	return false
}

func (s *sink) stopAdmission() {
	s.stopOnce.Do(func() { s.admission.Lock(); s.accepting = false; close(s.queue); s.admission.Unlock() })
}

func (s *sink) logError(message string, err error) {
	if s.logs.Allow() {
		s.logger.Error(message, slog.String("sink", s.name), slog.String("cluster", s.config.Cluster), slog.String("topic", s.config.Topic), slog.String("serializer", s.config.Format), slog.Any("error", err))
	}
}

func (s *sink) storageFailure(err error) {
	s.snapshotFailed.Store(true)
	s.healthy.Store(false)
	s.up.Store(false)
	s.metrics.storageErrors.WithLabelValues(s.config.Cluster, s.name, s.config.Topic).Inc()
	s.logError("Kafka sink durable storage failed; restart required", err)
}

func (s *sink) writeLoop() {
	defer close(s.writerDone)
	for {
		first, ok := <-s.queue
		if !ok {
			return
		}
		batch := []*record{first}
		timer := time.NewTimer(10 * time.Millisecond)
		closed := false
	collect:
		for len(batch) < 1000 {
			select {
			case r, ok := <-s.queue:
				if !ok {
					closed = true
					break collect
				}
				batch = append(batch, r)
			case <-timer.C:
				break collect
			}
		}
		timer.Stop()
		// Commit preceding snapshot data before encoding the completion count.
		segment := []*record{}
		for _, r := range batch {
			if r.Metadata.SnapshotPhase == "end" {
				s.persist(segment)
				segment = nil
				phase := "end"
				if s.snapshotFailed.Load() {
					phase = "failed"
					r.Metadata.SnapshotPhase = phase
					s.metrics.snapshotFailures.WithLabelValues(s.config.Cluster, s.name, s.config.Topic).Inc()
				}
				for i := range r.Headers {
					if r.Headers[i].Key == "snapshot-phase" {
						r.Headers[i].Value = []byte(phase)
					}
				}
				r.Headers = append(r.Headers, kgo.RecordHeader{Key: "snapshot-records", Value: []byte(strconv.FormatInt(s.snapshotRecords, 10))})
				s.persist([]*record{r})
			} else {
				segment = append(segment, r)
			}
		}
		s.persist(segment)
		select {
		case s.wake <- struct{}{}:
		default:
		}
		if closed {
			return
		}
	}
}

func (s *sink) persist(batch []*record) {
	if len(batch) == 0 {
		return
	}
	if s.healthy.Load() {
		if _, err := s.store.append(batch); err != nil {
			s.storageFailure(err)
		}
	}
	for _, r := range batch {
		if r.Sequence != 0 {
			s.metrics.persisted.WithLabelValues(labels(s, r.Metadata)...).Inc()
			if r.Metadata.SnapshotPhase == "data" {
				s.snapshotRecords++
			}
		} else {
			s.snapshotFailed.Store(true)
			s.metrics.dropped.WithLabelValues(labels(s, r.Metadata)...).Inc()
			if s.healthy.Load() {
				s.logError("Kafka sink durable backlog full; preserving pending records", errSpoolFull)
			}
		}
	}
}

type deliveryResult struct {
	record  *record
	err     error
	started time.Time
}

func permanentPublishError(err error) bool {
	var kafkaErr *kerr.Error
	return errors.As(err, &kafkaErr) && !kafkaErr.Retriable
}

func (s *sink) publishLoop() {
	defer close(s.publisherDone)
	paused, attempts := false, 0
	var retry []*record
	for {
		if s.ctx.Err() != nil {
			return
		}
		if !s.healthy.Load() || paused {
			select {
			case <-s.ctx.Done():
				return
			case <-s.wake:
			}
			continue
		}
		batch := retry
		retry = nil
		if len(batch) == 0 {
			var err error
			limit := s.windowSize
			if limit == 0 {
				limit = maxInFlight
			}
			batch, err = s.store.window(limit)
			if err != nil {
				s.storageFailure(err)
				continue
			}
		}
		if len(batch) == 0 {
			select {
			case <-s.writerDone:
				close(s.drained)
				return
			default:
			}
			select {
			case <-s.ctx.Done():
				return
			case <-s.wake:
			case <-s.writerDone:
			}
			continue
		}
		results := make(chan deliveryResult, len(batch))
		var windowFailed atomic.Bool
		// Produce may wait for the shared client's bounded buffer, exclusively
		// in this worker. Native idempotent retries preserve partition ordering.
		// Finish the entire window before retrying its failed records or sending
		// any newer records, including after a delivery timeout.
		for _, r := range batch {
			if windowFailed.Load() {
				results <- deliveryResult{r, errors.New("delivery deferred after window failure"), time.Now()}
				continue
			}
			dest := s.destination
			p := s.producer
			if s.config.ReplayDestination == "original" && r.Destination != nil {
				dest = *r.Destination
				p = s.producers[dest.Cluster]
			}
			if r.Sequence <= s.store.initialSequence {
				s.metrics.replayed.WithLabelValues(deliveryLabels(s, r)...).Inc()
			}
			started := time.Now()
			headers := append([]kgo.RecordHeader(nil), r.Headers...)
			headers = append(headers, kgo.RecordHeader{Key: "spool-sequence", Value: []byte(strconv.FormatUint(r.Sequence, 10))})
			p.Produce(s.ctx, &kgo.Record{Topic: dest.Topic, Key: r.Key, Value: r.Value, Headers: headers}, func(_ *kgo.Record, err error) {
				if err != nil {
					windowFailed.Store(true)
				}
				results <- deliveryResult{r, err, started}
			})
		}
		success := make([]uint64, 0, len(batch))
		failed := map[uint64]bool{}
		for range batch {
			select {
			case <-s.ctx.Done():
				return
			case result := <-results:
				r := result.record
				if result.err == nil {
					success = append(success, r.Sequence)
					s.metrics.published.WithLabelValues(deliveryLabels(s, r)...).Inc()
					s.metrics.latency.WithLabelValues(deliveryLabels(s, r)...).Observe(time.Since(result.started).Seconds())
				} else {
					failed[r.Sequence] = true
					s.metrics.publishErrors.WithLabelValues(deliveryLabels(s, r)...).Inc()
					s.up.Store(false)
					if permanentPublishError(result.err) {
						paused = true
					}
					s.logError("Kafka delivery window failed; pending records retained", fmt.Errorf("destination %s/%s: %w", deliveryLabels(s, r)[0], deliveryLabels(s, r)[2], result.err))
				}
			}
		}
		if err := s.store.removeBatch(success); err != nil {
			s.storageFailure(err)
			continue
		}
		// Never re-submit an earlier failed record behind a confirmed newer
		// record for the same destination/key. Native partition failures normally
		// fail the buffered suffix; this guard also covers admission-time errors.
		failedLanes := map[string]bool{}
		for _, r := range batch {
			lane := string(r.Key)
			if len(r.Key) == 0 {
				lane = fmt.Sprintf("record:%d", r.Sequence)
			}
			if s.config.ReplayDestination == "original" && r.Destination != nil {
				lane = r.Destination.Cluster + "|" + r.Destination.Topic + "|" + lane
			}
			if failed[r.Sequence] {
				failedLanes[lane] = true
			} else if failedLanes[lane] {
				paused = true
				s.snapshotFailed.Store(true)
				s.logError("Kafka delivery gap detected; publishing suspended; restart and reconcile consumer state", errors.New("a newer record was acknowledged after an earlier failure"))
			}
		}
		for _, r := range batch {
			if failed[r.Sequence] {
				retry = append(retry, r)
			}
		}
		if len(retry) == 0 && !paused {
			s.up.Store(true)
			attempts = 0
		}
		if len(retry) > 0 && !paused {
			timer := time.NewTimer(100 * time.Millisecond * time.Duration(1<<min(attempts, 8)))
			attempts++
			select {
			case <-s.ctx.Done():
				timer.Stop()
				return
			case <-timer.C:
			}
		}
	}
}

func (s *sink) Close(ctx context.Context) error {
	s.stopAdmission()
	var err error
	select {
	case <-s.drained:
	case <-ctx.Done():
		err = ctx.Err()
	}
	s.cancel()
	// Storage is closed only when both workers release it. Slow disk operations
	// may outlive the deadline, but Kafka shutdown never waits for them forever.
	s.closeOnce.Do(func() {
		go func() {
			<-s.writerDone
			<-s.publisherDone
			s.storageErr = s.store.close()
			if s.storageErr != nil {
				s.logError("Close Kafka durable queue failed", s.storageErr)
			}
			close(s.storageClosed)
		}()
	})
	select {
	case <-s.storageClosed:
		return errors.Join(err, s.storageErr)
	case <-ctx.Done():
		return errors.Join(err, ctx.Err())
	}
}

var _ event.Sink = (*sink)(nil)
