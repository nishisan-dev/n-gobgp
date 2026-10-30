package kafka

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	config "github.com/osrg/gobgp/v4/pkg/config/kafka"
	"github.com/osrg/gobgp/v4/pkg/event"
	"github.com/osrg/gobgp/v4/pkg/packet/bgp"
	"github.com/prometheus/client_golang/prometheus"
	"golang.org/x/time/rate"
)

type Manager struct {
	source          event.Source
	capture         atomic.Pointer[captureBatch]
	config          config.Config
	metrics         *metrics
	sinks           []*sink
	producers       map[string]producer
	collectors      []prometheus.Collector
	registerer      prometheus.Registerer
	subscription    event.Subscription
	lifecycle       sync.Mutex
	started, closed bool
	closeOnce       sync.Once
	closeDone       chan struct{}
	closeErr        error
}

// New initializes durable storage without requiring brokers to be reachable.
// A nil manager is returned for disabled Kafka, with no initialization at all.
func New(c config.Config, logger *slog.Logger, registerer prometheus.Registerer) (*Manager, error) {
	return newManager(c, logger, registerer, newProducer)
}

func newManager(c config.Config, logger *slog.Logger, registerer prometheus.Registerer, factory producerFactory) (*Manager, error) {
	if !c.Enabled {
		return nil, nil
	}
	if err := c.Normalize(); err != nil {
		return nil, err
	}
	if logger == nil {
		logger = slog.Default()
	}
	m := &Manager{config: c, metrics: newMetrics(), producers: map[string]producer{}, registerer: registerer, closeDone: make(chan struct{})}
	cleanup := func(err error) (*Manager, error) {
		for _, s := range m.sinks {
			s.cancel()
			_ = s.store.close()
		}
		for _, p := range m.producers {
			p.Close()
		}
		if registerer != nil {
			for _, collector := range m.collectors {
				registerer.Unregister(collector)
			}
		}
		return nil, err
	}
	names := make([]string, 0, len(c.Sinks))
	for name := range c.Sinks {
		names = append(names, name)
	}
	slices.Sort(names)
	for _, name := range names {
		cfg := c.Sinks[name]
		if !cfg.IsEnabled() {
			continue
		}
		matcher, err := event.NewMatcher(cfg.Match)
		if err != nil {
			return cleanup(err)
		}
		serializer, err := NewSerializer(cfg.Format)
		if err != nil {
			return cleanup(err)
		}
		ctx, cancel := context.WithCancel(context.Background())
		s := &sink{serializer: serializer, name: name, config: cfg, matcher: matcher, metrics: m.metrics, logger: logger, logs: rate.NewLimiter(rate.Every(10*time.Second), 1), queue: make(chan *record, cfg.Queue.Size), wake: make(chan struct{}, 1), writerDone: make(chan struct{}), publisherDone: make(chan struct{}), drained: make(chan struct{}), storageClosed: make(chan struct{}), ctx: ctx, cancel: cancel, accepting: true, collectorID: c.CollectorID, streamID: uuid.NewString(), producers: m.producers}
		s.destination = destination{Cluster: cfg.Cluster, Topic: cfg.Topic, Brokers: append([]string(nil), c.Clusters[cfg.Cluster].Brokers...)}
		slices.Sort(s.destination.Brokers)
		m.sinks = append(m.sinks, s)
		err = os.MkdirAll(c.SpoolDir, 0o700)
		if err == nil {
			s.store, err = openStore(filepath.Join(c.SpoolDir, name+".db"), cfg.Spool.MaxBytes, cfg.Key.Strategy)
		}
		destinations := []destination{s.destination}
		if err == nil && cfg.ReplayDestination == "original" {
			var old []destination
			old, err = s.store.destinations()
			destinations = append(destinations, old...)
		}
		if err == nil {
			for _, dest := range destinations {
				cluster, exists := c.Clusters[dest.Cluster]
				brokers := append([]string(nil), cluster.Brokers...)
				slices.Sort(brokers)
				if !exists || strings.Join(brokers, "\x00") != strings.Join(dest.Brokers, "\x00") {
					err = fmt.Errorf("original destination %s/%s is unavailable or its broker seeds changed; restore its cluster configuration or explicitly use replayDestination=current", dest.Cluster, dest.Topic)
					break
				}
				if _, exists := m.producers[dest.Cluster]; !exists {
					m.producers[dest.Cluster], err = factory(dest.Cluster, cluster)
					if err != nil {
						delete(m.producers, dest.Cluster)
						break
					}
				}
			}
		}
		if err != nil {
			if cfg.Spool.IsFatal() {
				return cleanup(fmt.Errorf("kafka sink %s: %w", name, err))
			}
			if s.store != nil {
				_ = s.store.close()
				s.store = nil
			}
			s.storageFailure(err)
			continue
		}
		s.producer = m.producers[cfg.Cluster]
		s.healthy.Store(true)
		s.up.Store(true)
	}
	m.collectors = m.metrics.collectors()
	for _, s := range m.sinks {
		gauge := func(name, help string, fn func() float64) prometheus.Collector {
			return prometheus.NewGaugeFunc(prometheus.GaugeOpts{Namespace: "gobgp", Subsystem: "kafka", Name: name, Help: help, ConstLabels: prometheus.Labels{"cluster": s.config.Cluster, "sink": s.name, "topic": s.config.Topic}}, fn)
		}
		m.collectors = append(m.collectors,
			gauge("capture_ready", "One if the current capture snapshot is persisted and no loss has been observed.", func() float64 {
				c := s.capture.Load()
				if c != nil && c.end.Load() != 0 && c.lost.Load() == 0 {
					return 1
				}
				return 0
			}),
			gauge("queue_size", "Records awaiting durable persistence.", func() float64 { return float64(len(s.queue)) }),
			gauge("queue_capacity", "Capacity of the per-sink admission queue.", func() float64 { return float64(cap(s.queue)) }),
			gauge("backlog_records", "Records pending in durable storage.", func() float64 {
				if s.store == nil {
					return 0
				}
				return float64(s.store.count.Load())
			}),
			gauge("backlog_bytes", "Encoded pending records in durable storage.", func() float64 {
				if s.store == nil {
					return 0
				}
				return float64(s.store.bytes.Load())
			}),
			gauge("spool_file_bytes", "Physical durable queue file size, including reusable pages.", func() float64 {
				if s.store == nil {
					return 0
				}
				return float64(s.store.fileSize())
			}),
			gauge("sink_up", "One if the sink has no observed storage or publishing failure.", func() float64 {
				if s.up.Load() {
					return 1
				}
				return 0
			}))
	}
	m.collectors = append(m.collectors, prometheus.NewGaugeFunc(prometheus.GaugeOpts{Namespace: "gobgp", Subsystem: "kafka", Name: "ingress_queue_size", Help: "Queued native watcher notifications."}, func() float64 {
		m.lifecycle.Lock()
		defer m.lifecycle.Unlock()
		if m.subscription == nil {
			return 0
		}
		return float64(m.subscription.QueueSize())
	}))
	if registerer != nil {
		registered := []prometheus.Collector{}
		for _, collector := range m.collectors {
			if err := registerer.Register(collector); err != nil {
				m.collectors = registered
				return cleanup(err)
			}
			registered = append(registered, collector)
		}
	}
	m.newCapture(false)
	return m, nil
}

func (m *Manager) Start(source event.Source) error {
	if m == nil {
		return nil
	}
	m.lifecycle.Lock()
	defer m.lifecycle.Unlock()
	if m.closed || m.started {
		return fmt.Errorf("kafka manager already started or closed")
	}
	m.source = source
	for _, s := range m.sinks {
		if s.store == nil {
			close(s.writerDone)
			close(s.publisherDone)
			close(s.drained)
			continue
		}
		go s.writeLoop()
		go s.publishLoop()
	}
	m.started = true
	sub, err := m.subscribe(source, m.capture.Load())
	if err != nil {
		for _, c := range m.capture.Load().states {
			c.invalidate(lossSubscription)
		}
		return err
	}
	m.subscription = sub
	m.monitorCapture(m.capture.Load(), sub)
	return nil
}

func (m *Manager) subscribe(source event.Source, b *captureBatch) (event.Subscription, error) {
	initialDump := false
	for _, s := range m.sinks {
		if s.healthy.Load() && (b.forced || s.config.InitialDump) {
			initialDump = true
		}
	}
	sources := []string{}
	for _, candidate := range event.Sources {
		for _, s := range m.sinks {
			if s.healthy.Load() && s.matcher.AllowsSource(candidate) {
				sources = append(sources, candidate)
				break
			}
		}
	}

	initialSources := make([]string, 0)
	initialFamilies := make([]string, 0)
	for _, candidate := range event.Sources {
		for _, s := range m.sinks {
			if s.healthy.Load() && (b.forced || s.config.InitialDump) && s.matcher.AllowsSource(candidate) {
				initialSources = append(initialSources, candidate)
				break
			}
		}
	}
	for _, candidate := range bgp.AddressFamilyNameMap {
		for _, s := range m.sinks {
			if s.healthy.Load() && (b.forced || s.config.InitialDump) && s.matcher.AllowsFamily(candidate) {
				initialFamilies = append(initialFamilies, candidate)
				break
			}
		}
	}
	slices.Sort(initialFamilies)
	if len(sources) == 0 {
		return nil, nil
	}
	if source == nil {
		return nil, fmt.Errorf("event source is required")
	}
	return source.SubscribeEvents(event.SubscriptionOptions{InitialDump: initialDump, InitialSources: initialSources, InitialFamilies: initialFamilies, Sources: sources, Capacity: m.config.IngressQueueSize, OnDrop: func() { m.captureDrop(b) }}, func(v event.Observation) { m.routeCapture(b, v) })
}

func (m *Manager) routeCapture(b *captureBatch, v event.Observation) {
	if b == nil || m.capture.Load() != b {
		return
	}
	m.metrics.received.WithLabelValues(metricFamily(v.Metadata.Family), v.Metadata.EventType).Inc()
	matched := []*sink{}
	for _, s := range m.sinks {
		if v.Metadata.SnapshotID != "" && !b.forced && !s.config.InitialDump {
			continue
		}
		if v.Metadata.SnapshotPhase != "" && v.Metadata.SnapshotPhase != "data" || s.Match(v.Metadata) {
			matched = append(matched, s)
			m.metrics.matched.WithLabelValues(labels(s, v.Metadata)...).Inc()
		}
	}
	if len(matched) == 0 {
		return
	}
	type serialized struct {
		value []byte
		err   error
	}
	cache := map[string]serialized{}
	id := uuid.NewString()
	for _, s := range matched {
		encoded, ok := cache[s.config.Format]
		if !ok {
			encoded.err = v.Err
			if encoded.err == nil {
				encoded.value, encoded.err = s.serializer.Serialize(v.Message)
			}
			cache[s.config.Format] = encoded
		}
		if encoded.err != nil {
			s.metrics.serializationErrors.WithLabelValues(labels(s, v.Metadata)...).Inc()
			s.metrics.dropped.WithLabelValues(labels(s, v.Metadata)...).Inc()
			s.snapshotFailed.Store(true)
			b.states[s].invalidate(lossSerialization)
			s.logError("Kafka event serialization failed", encoded.err)
			continue
		}
		s.offerCapture(v, encoded.value, id, b.states[s])
	}
}

// Close detaches observation, drains within the caller's deadline, and leaves
// unacknowledged durable records available for the next process invocation.
func (m *Manager) Close(ctx context.Context) error {
	if m == nil {
		return nil
	}
	m.closeOnce.Do(func() {
		go func() {
			shutdownCtx, cancel := context.WithTimeout(ctx, m.config.ShutdownTimeout)
			defer cancel()
			m.closeErr = m.shutdown(shutdownCtx)
			close(m.closeDone)
		}()
	})
	select {
	case <-m.closeDone:
		return m.closeErr
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (m *Manager) shutdown(ctx context.Context) error {
	m.lifecycle.Lock()
	m.closed = true
	if b := m.capture.Load(); b != nil {
		for _, c := range b.states {
			c.invalidate(lossClosed)
		}
	}
	started := m.started
	sub := m.subscription
	m.lifecycle.Unlock()
	var err error
	if sub != nil {
		sub.Stop()
		select {
		case <-sub.Done():
		case <-ctx.Done():
			err = ctx.Err()
		}
	}
	if started {
		var wg sync.WaitGroup
		var errMu sync.Mutex
		for _, s := range m.sinks {
			wg.Go(func() {
				if e := s.Close(ctx); e != nil {
					errMu.Lock()
					err = errors.Join(err, e)
					errMu.Unlock()
				}
			})
		}
		wg.Wait()
	} else {
		for _, s := range m.sinks {
			s.cancel()
			err = errors.Join(err, s.store.close())
		}
	}
	for _, p := range m.producers {
		err = errors.Join(err, p.Flush(ctx))
		p.Close()
	}
	if m.registerer != nil {
		for _, collector := range m.collectors {
			m.registerer.Unregister(collector)
		}
	}
	return err
}
