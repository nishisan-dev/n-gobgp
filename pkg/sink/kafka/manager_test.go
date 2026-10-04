package kafka

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/osrg/gobgp/v4/api"
	config "github.com/osrg/gobgp/v4/pkg/config/kafka"
	"github.com/osrg/gobgp/v4/pkg/event"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	bolt "go.etcd.io/bbolt"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

type fakeSubscription struct {
	done chan struct{}
	once sync.Once
}

func (s *fakeSubscription) Stop()                 { s.once.Do(func() { close(s.done) }) }
func (s *fakeSubscription) Done() <-chan struct{} { return s.done }
func (s *fakeSubscription) QueueSize() int        { return 0 }

type fakeSource struct {
	callback func(event.Observation)
	options  event.SubscriptionOptions
}

func (s *fakeSource) SubscribeEvents(o event.SubscriptionOptions, fn func(event.Observation)) (event.Subscription, error) {
	s.options = o
	s.callback = fn
	return &fakeSubscription{done: make(chan struct{})}, nil
}

type fakeDelivery struct {
	record   *kgo.Record
	complete func(error)
}
type fakeProducer struct {
	deliveries chan fakeDelivery
	autoAck    bool
	err        error
}

func (p *fakeProducer) Produce(_ context.Context, r *kgo.Record, fn func(*kgo.Record, error)) {
	copyRecord := *r
	p.deliveries <- fakeDelivery{&copyRecord, func(err error) { fn(r, err) }}
	if p.autoAck || p.err != nil {
		fn(r, p.err)
	}
}
func (*fakeProducer) Flush(context.Context) error { return nil }
func (*fakeProducer) Close()                      {}

func testConfig(t *testing.T) config.Config {
	t.Helper()
	return config.Config{Enabled: true, SpoolDir: t.TempDir(), Clusters: map[string]config.Cluster{"primary": {Brokers: []string{"unavailable:9092"}}}, Sinks: map[string]config.Sink{"raw": {Cluster: "primary", Topic: "network.bgp.raw"}}}
}

func testObservation() event.Observation {
	return event.Observation{Message: &api.WatchEventResponse{Event: &api.WatchEventResponse_Table{Table: &api.WatchEventResponse_TableEvent{Paths: []*api.Path{{Family: &api.Family{Afi: api.Family_AFI_IP, Safi: api.Family_SAFI_UNICAST}, SourceAsn: 65001, NeighborIp: "192.0.2.1"}}}}}, Metadata: event.Metadata{EventType: "route", Family: "ipv4-unicast", Source: "adj-in", PeerAddress: "192.0.2.1", PeerASN: 65001, ObservedAt: time.Now()}}
}

func testManager(t *testing.T, c config.Config, p *fakeProducer) (*Manager, *fakeSource) {
	t.Helper()
	m, err := newManager(c, slog.New(slog.NewTextHandler(io.Discard, nil)), prometheus.NewRegistry(), func(string, config.Cluster) (producer, error) { return p, nil })
	require.NoError(t, err)
	src := new(fakeSource)
	require.NoError(t, m.Start(src))
	return m, src
}

func closeManager(t *testing.T, m *Manager) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	require.NoError(t, m.Close(ctx))
}

func delivery(t *testing.T, p *fakeProducer) fakeDelivery {
	t.Helper()
	select {
	case v := <-p.deliveries:
		return v
	case <-time.After(3 * time.Second):
		t.Fatal("missing Kafka delivery")
		return fakeDelivery{}
	}
}

func waitEmpty(t *testing.T, m *Manager) {
	t.Helper()
	require.Eventually(t, func() bool {
		for _, s := range m.sinks {
			if s.store.count.Load() != 0 {
				return false
			}
		}
		return true
	}, 3*time.Second, 10*time.Millisecond)
}

func header(r *kgo.Record, key string) string {
	for _, h := range r.Headers {
		if h.Key == key {
			return string(h.Value)
		}
	}
	return ""
}

func TestFanOutSharesProducerAndPreservesProtobuf(t *testing.T) {
	c := testConfig(t)
	c.Sinks["copy"] = config.Sink{Cluster: "primary", Topic: "network.bgp.copy"}
	c.Sinks["ls-only"] = config.Sink{Cluster: "primary", Topic: "network.bgp.ls", Match: event.MatchConfig{Families: event.Filter[string]{Include: []string{"bgp-ls"}}}}
	p := &fakeProducer{deliveries: make(chan fakeDelivery, 20), autoAck: true}
	calls := 0
	m, err := newManager(c, nil, prometheus.NewRegistry(), func(string, config.Cluster) (producer, error) { calls++; return p, nil })
	require.NoError(t, err)
	require.Equal(t, 1, calls)
	src := new(fakeSource)
	require.NoError(t, m.Start(src))
	defer closeManager(t, m)
	v := testObservation()
	src.callback(v)
	a, b := delivery(t, p), delivery(t, p)
	require.NotEqual(t, a.record.Topic, b.record.Topic)
	require.Equal(t, header(a.record, "event-id"), header(b.record, "event-id"))
	require.Equal(t, "api.WatchEventResponse", header(a.record, "schema"))
	require.Equal(t, "192.0.2.1", string(a.record.Key))
	var decoded api.WatchEventResponse
	require.NoError(t, protojson.Unmarshal(a.record.Value, &decoded))
	require.True(t, proto.Equal(v.Message, &decoded))
	waitEmpty(t, m)
	select {
	case <-p.deliveries:
		t.Fatal("unmatched BGP-LS sink published IPv4")
	default:
	}
}

func TestReplayCurrentMigratesDestinationAndRetainsIdentity(t *testing.T) {
	c := testConfig(t)
	p := &fakeProducer{deliveries: make(chan fakeDelivery, 20)}
	m, src := testManager(t, c, p)
	src.callback(testObservation())
	original := delivery(t, p)
	require.Equal(t, int64(1), m.sinks[0].store.count.Load())
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	require.ErrorIs(t, m.Close(ctx), context.DeadlineExceeded)
	select {
	case <-m.closeDone:
	case <-time.After(time.Second):
		t.Fatal("shutdown did not finish")
	}
	// Wait for asynchronous worker cleanup to release the durable file lock.
	require.Eventually(t, func() bool {
		select {
		case <-m.sinks[0].publisherDone:
			return true
		default:
			return false
		}
	}, time.Second, time.Millisecond)
	c.Clusters = map[string]config.Cluster{"analytics": {Brokers: []string{"new-cluster:9092"}}}
	c.Sinks["raw"] = config.Sink{ReplayDestination: "current", Cluster: "analytics", Topic: "network.bgp.migrated", Match: event.MatchConfig{EventTypes: event.Filter[string]{Include: []string{"peer-state"}}}}
	p2 := &fakeProducer{deliveries: make(chan fakeDelivery, 20), autoAck: true}
	m2, _ := testManager(t, c, p2)
	defer closeManager(t, m2)
	replay := delivery(t, p2)
	require.Equal(t, "network.bgp.migrated", replay.record.Topic)
	require.Equal(t, original.record.Value, replay.record.Value)
	require.Equal(t, original.record.Key, replay.record.Key)
	require.Equal(t, header(original.record, "event-id"), header(replay.record, "event-id"))
	waitEmpty(t, m2)
}

func TestDeliveryWindowBatchesPeerAndRetriesBeforeNewerRecords(t *testing.T) {
	p := &fakeProducer{deliveries: make(chan fakeDelivery, 20)}
	m, src := testManager(t, testConfig(t), p)
	v := testObservation()
	src.callback(v)
	src.callback(v)
	first, second := delivery(t, p), delivery(t, p)
	require.Equal(t, string(first.record.Key), string(second.record.Key))
	require.Equal(t, "1", header(first.record, "spool-sequence"))
	require.Equal(t, "2", header(second.record, "spool-sequence"))
	src.callback(v)
	first.complete(errors.New("broker unavailable"))
	select {
	case <-p.deliveries:
		t.Fatal("retry or newer event sent before window settled")
	case <-time.After(150 * time.Millisecond):
	}
	second.complete(errors.New("partition batch failed"))
	retryFirst, retrySecond := delivery(t, p), delivery(t, p)
	require.Equal(t, header(first.record, "event-id"), header(retryFirst.record, "event-id"))
	require.Equal(t, header(second.record, "event-id"), header(retrySecond.record, "event-id"))
	retrySecond.complete(nil) // Callback arrival order must not change submission order.
	retryFirst.complete(nil)
	newer := delivery(t, p)
	require.Equal(t, "3", header(newer.record, "spool-sequence"))
	newer.complete(nil)
	waitEmpty(t, m)
	closeManager(t, m)
}

func TestSlowStorageAndFullQueueIsolateSinks(t *testing.T) {
	c := testConfig(t)
	raw := c.Sinks["raw"]
	raw.Queue.Size = 1
	c.Sinks["raw"] = raw
	c.Sinks["good"] = config.Sink{Cluster: "primary", Topic: "good"}
	p := &fakeProducer{deliveries: make(chan fakeDelivery, 1000), autoAck: true}
	m, src := testManager(t, c, p)
	var slow *sink
	for _, s := range m.sinks {
		if s.name == "raw" {
			slow = s
		}
	}
	tx, err := slow.store.db.Begin(true)
	require.NoError(t, err)
	completed := make(chan struct{})
	go func() {
		for range 100 {
			src.callback(testObservation())
		}
		close(completed)
	}()
	select {
	case <-completed:
	case <-time.After(time.Second):
		_ = tx.Rollback()
		t.Fatal("routing blocked on disk")
	}
	d := delivery(t, p)
	require.Equal(t, "good", d.record.Topic)
	require.NoError(t, tx.Rollback())
	waitEmpty(t, m)
	closeManager(t, m)
}

func TestPermanentPublishFailureRetainsBacklog(t *testing.T) {
	p := &fakeProducer{deliveries: make(chan fakeDelivery, 20), err: kerr.TopicAuthorizationFailed}
	m, src := testManager(t, testConfig(t), p)
	src.callback(testObservation())
	_ = delivery(t, p)
	require.Eventually(t, func() bool { return !m.sinks[0].up.Load() }, time.Second, time.Millisecond)
	require.Equal(t, int64(1), m.sinks[0].store.count.Load())
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	require.ErrorIs(t, m.Close(ctx), context.DeadlineExceeded)
}

func TestDisabledKafkaDoesNotInitialize(t *testing.T) {
	called := false
	m, err := newManager(config.Config{}, nil, nil, func(string, config.Cluster) (producer, error) { called = true; return nil, errors.New("unexpected") })
	require.NoError(t, err)
	require.Nil(t, m)
	require.False(t, called)
	require.NoError(t, m.Start(nil))
	require.NoError(t, m.Close(context.Background()))
}

func TestInitializationRejectsCorruptSpool(t *testing.T) {
	c := testConfig(t)
	fatal := true
	cfg := c.Sinks["raw"]
	cfg.Spool.FailOnError = &fatal
	c.Sinks["raw"] = cfg
	s, err := openStore(filepath.Join(c.SpoolDir, "raw.db"), 1024)
	require.NoError(t, err)
	require.NoError(t, s.db.Update(func(tx *bolt.Tx) error { return tx.Bucket(metadataBucket).Put([]byte("version"), []byte("invalid")) }))
	require.NoError(t, s.close())
	_, err = newManager(c, nil, nil, func(string, config.Cluster) (producer, error) {
		t.Fatal("producer initialized despite corrupt queue")
		return nil, nil
	})
	require.ErrorContains(t, err, "unsupported spool version")
}

func TestUnavailableBrokerDoesNotPreventStartupOrShutdown(t *testing.T) {
	c := testConfig(t)
	c.Clusters["primary"] = config.Cluster{Brokers: []string{"127.0.0.1:1"}}
	m, err := New(c, slog.New(slog.NewTextHandler(io.Discard, nil)), prometheus.NewRegistry())
	require.NoError(t, err)
	src := new(fakeSource)
	require.NoError(t, m.Start(src))
	src.callback(testObservation())
	require.Eventually(t, func() bool { return m.sinks[0].store.count.Load() == 1 }, time.Second, time.Millisecond)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	require.ErrorIs(t, m.Close(ctx), context.DeadlineExceeded)
	select {
	case <-m.closeDone:
	case <-time.After(time.Second):
		t.Fatal("Kafka close blocked on unavailable broker")
	}
}
