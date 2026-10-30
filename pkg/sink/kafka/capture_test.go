package kafka

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/osrg/gobgp/v4/api"
	config "github.com/osrg/gobgp/v4/pkg/config/kafka"
	"github.com/osrg/gobgp/v4/pkg/event"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
	bolt "go.etcd.io/bbolt"
)

func captureSnapshot(src *fakeSource, id string) {
	v := testObservation()
	v.Message = &api.WatchEventResponse{}
	v.Metadata = event.Metadata{EventType: "snapshot", SnapshotID: id, SnapshotPhase: "begin", ObservedAt: time.Now()}
	src.callback(v)
	v = testObservation()
	v.Metadata.SnapshotID = id
	v.Metadata.SnapshotPhase = "data"
	src.callback(v)
	v.Message = &api.WatchEventResponse{}
	v.Metadata.EventType = "snapshot"
	v.Metadata.SnapshotPhase = "end"
	src.callback(v)
}

func captureStateOf(m *Manager, name string) CaptureStatus {
	for _, s := range m.CaptureStatus().Sinks {
		if s.Sink == name {
			return s
		}
	}
	return CaptureStatus{}
}

func captureConfig(t *testing.T) config.Config {
	c := testConfig(t)
	s := c.Sinks["raw"]
	s.InitialDump = true
	c.Sinks["raw"] = s
	return c
}

func TestCaptureLossAfterBootstrapAndResyncFencesCallbacks(t *testing.T) {
	p := &fakeProducer{deliveries: make(chan fakeDelivery, 32), autoAck: true}
	m, src := testManager(t, captureConfig(t), p)
	defer closeManager(t, m)
	captureSnapshot(src, "first")
	require.Eventually(t, func() bool { return captureStateOf(m, "raw").State == "ready" }, time.Second, time.Millisecond)
	old := m.CaptureStatus().Generation
	oldDrop, oldCallback := src.options.OnDrop, src.callback
	oldDrop()
	require.Equal(t, "invalid", captureStateOf(m, "raw").State)
	require.Contains(t, captureStateOf(m, "raw").LossReasons, "ingress-overflow")
	report, err := m.Resync(context.Background(), old)
	require.NoError(t, err)
	require.NotEqual(t, old, report.Generation)
	require.True(t, src.options.InitialDump)
	captureSnapshot(src, "second")
	require.Eventually(t, func() bool { return captureStateOf(m, "raw").State == "ready" }, time.Second, time.Millisecond)
	oldDrop()
	oldCallback(testObservation())
	require.Equal(t, "ready", captureStateOf(m, "raw").State)
	require.Empty(t, captureStateOf(m, "raw").LossReasons)
	_, err = m.Resync(context.Background(), old)
	require.ErrorIs(t, err, ErrCaptureConflict)
	waitEmpty(t, m)
}

func TestCaptureSinkLossIsScopedAndStatusBypassesFullQueues(t *testing.T) {
	c := testConfig(t)
	raw := c.Sinks["raw"]
	raw.Queue.Size = 1
	raw.Key.Strategy = "none"
	c.Sinks["raw"] = raw
	c.Sinks["copy"] = config.Sink{Cluster: "primary", Topic: "copy", Key: config.Key{Strategy: "none"}, Queue: config.Queue{Size: 4096}}
	p := &fakeProducer{deliveries: make(chan fakeDelivery, 8192), autoAck: true}
	m, src := testManager(t, c, p)
	// This fixture deliberately admits thousands of records; drain them before
	// cleanup rather than applying the small-fixture two-second deadline.
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		require.NoError(t, m.Close(ctx))
	}()
	var rawSink *sink
	for _, s := range m.sinks {
		if s.name == "raw" {
			rawSink = s
		}
	}
	entered, release, done := make(chan struct{}), make(chan struct{}), make(chan struct{})
	go func() {
		defer close(done)
		_ = rawSink.store.db.Update(func(_ *bolt.Tx) error { close(entered); <-release; return nil })
	}()
	<-entered
	defer func() { close(release); <-done }()
	// Exceed the writer collection limit even if it drains admission before
	// its blocked commit. A tiny burst can all fit in the writer batch.
	for range 2048 {
		src.callback(testObservation())
	}
	require.Eventually(t, func() bool { return captureStateOf(m, "raw").State == "invalid" }, time.Second, time.Millisecond)
	require.Contains(t, captureStateOf(m, "raw").LossReasons, "sink-overflow")
	require.Empty(t, captureStateOf(m, "copy").LossReasons)
	request := httptest.NewRequest(http.MethodGet, "/v1/capture", nil)
	response := httptest.NewRecorder()
	m.Handler().ServeHTTP(response, request)
	require.Equal(t, http.StatusOK, response.Code)
	require.Contains(t, response.Body.String(), "sink-overflow")
}

func TestCaptureSerializationAndQuotaLossCannotComplete(t *testing.T) {
	for _, reason := range []string{"serialization", "spool-quota"} {
		t.Run(reason, func(t *testing.T) {
			c := captureConfig(t)
			cfg := c.Sinks["raw"]
			cfg.Spool.MaxBytes = 16384
			c.Sinks["raw"] = cfg
			p := &fakeProducer{deliveries: make(chan fakeDelivery, 16), autoAck: true}
			m, src := testManager(t, c, p)
			defer closeManager(t, m)
			if reason == "serialization" {
				v := testObservation()
				v.Message = nil
				src.callback(v)
			} else {
				m.sinks[0].offer(testObservation(), make([]byte, 32768), "oversize")
			}
			require.Eventually(t, func() bool { return captureStateOf(m, "raw").State == "invalid" }, time.Second, time.Millisecond)
			require.Contains(t, captureStateOf(m, "raw").LossReasons, reason)
			captureSnapshot(src, "failed")
			require.Eventually(t, func() bool { return captureStateOf(m, "raw").SnapshotEndSequence > 0 }, time.Second, time.Millisecond)
			require.Equal(t, "invalid", captureStateOf(m, "raw").State)
		})
	}
}

type blockedCaptureSource struct {
	fakeSource
	block   chan struct{}
	entered chan struct{}
	fail    bool
}

func (s *blockedCaptureSource) SubscribeEvents(o event.SubscriptionOptions, fn func(event.Observation)) (event.Subscription, error) {
	if s.block != nil {
		close(s.entered)
		<-s.block
	}
	if s.fail {
		return nil, errors.New("subscription unavailable")
	}
	return s.fakeSource.SubscribeEvents(o, fn)
}

func TestCaptureStatusRemainsAvailableDuringResync(t *testing.T) {
	p := &fakeProducer{deliveries: make(chan fakeDelivery, 16), autoAck: true}
	m, err := newManager(captureConfig(t), nil, prometheus.NewRegistry(), func(string, config.Cluster) (producer, error) { return p, nil })
	require.NoError(t, err)
	src := &blockedCaptureSource{}
	require.NoError(t, m.Start(src))
	defer closeManager(t, m)
	src.block = make(chan struct{})
	src.entered = make(chan struct{})
	release := src.block
	released := false
	defer func() {
		if !released {
			close(release)
		}
	}()
	result := make(chan error, 1)
	old := m.CaptureStatus().Generation
	go func() { _, err := m.Resync(context.Background(), old); result <- err }()
	<-src.entered
	report := m.CaptureStatus()
	require.NotEqual(t, old, report.Generation)
	_, err = m.Resync(context.Background(), report.Generation)
	require.ErrorIs(t, err, ErrCaptureConflict)
	response := httptest.NewRecorder()
	m.Handler().ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/v1/capture", nil))
	require.Equal(t, http.StatusOK, response.Code)
	close(src.block)
	released = true
	require.NoError(t, <-result)
	src.block = nil
	src.fail = true
	_, err = m.Resync(context.Background(), report.Generation)
	require.Error(t, err)
	require.Equal(t, "invalid", captureStateOf(m, "raw").State)
}

func TestCaptureControlRejectsUnfencedAndCanceledRequests(t *testing.T) {
	p := &fakeProducer{deliveries: make(chan fakeDelivery, 16), autoAck: true}
	m, _ := testManager(t, captureConfig(t), p)
	defer closeManager(t, m)
	generation := m.CaptureStatus().Generation
	for _, body := range []string{`{}`, `{"generation":"old"}`, `{"generation":"x","unknown":1}`, `{} {}`} {
		response := httptest.NewRecorder()
		m.Handler().ServeHTTP(response, httptest.NewRequest(http.MethodPost, "/v1/resync", strings.NewReader(body)))
		require.GreaterOrEqual(t, response.Code, 400)
		require.Equal(t, generation, m.CaptureStatus().Generation)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := m.Resync(ctx, generation)
	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, generation, m.CaptureStatus().Generation)
}

func TestCaptureUnexpectedSubscriptionEndInvalidatesWithoutNewData(t *testing.T) {
	p := &fakeProducer{deliveries: make(chan fakeDelivery, 16), autoAck: true}
	m, src := testManager(t, captureConfig(t), p)
	defer closeManager(t, m)
	captureSnapshot(src, "before-close")
	require.Eventually(t, func() bool { return captureStateOf(m, "raw").State == "ready" }, time.Second, time.Millisecond)
	m.subscription.Stop()
	require.Eventually(t, func() bool { return captureStateOf(m, "raw").State == "invalid" }, time.Second, time.Millisecond)
	require.Contains(t, captureStateOf(m, "raw").LossReasons, "subscription")
}

func TestCaptureRestartReplaysOriginalGenerationWithoutValidatingNewView(t *testing.T) {
	c := captureConfig(t)
	p := &fakeProducer{deliveries: make(chan fakeDelivery, 32)}
	first, src := testManager(t, c, p)
	captureSnapshot(src, "persisted-before-crash")
	require.Eventually(t, func() bool { return first.sinks[0].store.count.Load() == 3 }, 3*time.Second, time.Millisecond)
	old := first.CaptureStatus().Generation
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	require.Error(t, first.Close(ctx))
	cancel()
	select {
	case <-first.sinks[0].storageClosed:
	case <-time.After(3 * time.Second):
		t.Fatal("pending store did not close")
	}
	nextProducer := &fakeProducer{deliveries: make(chan fakeDelivery, 32), autoAck: true}
	next, nextSource := testManager(t, c, nextProducer)
	defer closeManager(t, next)
	require.NotEqual(t, old, next.CaptureStatus().Generation)
	for range 3 {
		require.Equal(t, old, header(delivery(t, nextProducer).record, "capture-generation"))
	}
	require.NotEqual(t, "ready", captureStateOf(next, "raw").State, "an old durable snapshot cannot validate a new generation")
	waitEmpty(t, next)
	require.Zero(t, captureStateOf(next, "raw").LastPublishedSequence, "old replay must not advance the new capture publication watermark")
	captureSnapshot(nextSource, "after-restart")
	require.Eventually(t, func() bool { return captureStateOf(next, "raw").State == "ready" }, 3*time.Second, time.Millisecond)
	for range 3 {
		require.Equal(t, next.CaptureStatus().Generation, header(delivery(t, nextProducer).record, "capture-generation"))
	}
	waitEmpty(t, next)
	require.Equal(t, uint64(3), captureStateOf(next, "raw").LastPublishedSequence)
}
