package kafka

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"

	"github.com/google/uuid"
	"github.com/osrg/gobgp/v4/pkg/event"
)

const CaptureContractVersion = "1"

const (
	lossIngress uint32 = 1 << iota
	lossQueue
	lossSerialization
	lossQuota
	lossStorage
	lossDelivery
	lossSuperseded
	lossSubscription
	lossProtocol
	lossClosed
)

var lossNames = []string{"ingress-overflow", "sink-overflow", "serialization", "spool-quota", "storage", "delivery-order", "superseded", "subscription", "snapshot-protocol", "closed"}

// CaptureStatus is authoritative even when no control record can enter Kafka.
// Ready describes producer capture/persistence, not consumer delivery progress.
type CaptureStatus struct {
	Sink                  string            `json:"sink"`
	Generation            string            `json:"generation"`
	State                 string            `json:"state"`
	Scope                 string            `json:"scope"`
	Match                 event.MatchConfig `json:"match"`
	SnapshotRequested     bool              `json:"snapshot_requested"`
	StorageHealthy        bool              `json:"storage_healthy"`
	Publishing            bool              `json:"publishing"`
	LossReasons           []string          `json:"loss_reasons"`
	LossIncidents         uint64            `json:"loss_incidents"`
	LastSequence          uint64            `json:"last_sequence"`
	LastPublishedSequence uint64            `json:"last_published_sequence"`
	SnapshotEndSequence   uint64            `json:"snapshot_end_sequence"`
}

type CaptureReport struct {
	Contract   string          `json:"contract"`
	Generation string          `json:"generation"`
	Sinks      []CaptureStatus `json:"sinks"`
}

type captureState struct {
	generation string
	sequence   atomic.Uint64
	published  atomic.Uint64
	lost       atomic.Uint32
	incidents  atomic.Uint64
	end        atomic.Uint64
	count      atomic.Int64
	snapshotID string // writer only; one snapshot per generation
}

func (c *captureState) invalidate(reason uint32) {
	if c != nil {
		c.lost.Or(reason)
		c.incidents.Add(1)
	}
}

type captureBatch struct {
	generation string
	states     map[*sink]*captureState // immutable after publication
	forced     bool
}

func (m *Manager) newCapture(forced bool) *captureBatch {
	b := &captureBatch{generation: uuid.NewString(), states: make(map[*sink]*captureState), forced: forced}
	for _, s := range m.sinks {
		c := &captureState{generation: b.generation}
		if !s.healthy.Load() {
			c.invalidate(lossStorage)
		}
		b.states[s] = c
		s.capture.Store(c)
	}
	m.capture.Store(b)
	return b
}

// CaptureStatus never takes lifecycle or admission locks and never waits for I/O.
func (m *Manager) CaptureStatus() CaptureReport {
	r := CaptureReport{Contract: CaptureContractVersion, Sinks: []CaptureStatus{}}
	if m == nil {
		return r
	}
	b := m.capture.Load()
	if b == nil {
		return r
	}
	r.Generation = b.generation
	for _, s := range m.sinks {
		c := b.states[s]
		end := c.end.Load()
		published := c.published.Load()
		last := c.sequence.Load()
		healthy, publishing := s.healthy.Load(), s.up.Load()
		flags := c.lost.Load()
		state := "awaiting-snapshot"
		if end != 0 {
			state = "ready"
		} else if last != 0 {
			state = "capturing"
		}
		reasons := []string{}
		for i, name := range lossNames {
			if flags&(1<<i) != 0 {
				reasons = append(reasons, name)
			}
		}
		if flags != 0 {
			state = "invalid"
		}
		r.Sinks = append(r.Sinks, CaptureStatus{Sink: s.name, Generation: b.generation, State: state, Scope: "entire-configured-sink", Match: s.config.Match, SnapshotRequested: b.forced || s.config.InitialDump, StorageHealthy: healthy, Publishing: publishing, LossReasons: reasons, LossIncidents: c.incidents.Load(), LastSequence: last, LastPublishedSequence: published, SnapshotEndSequence: end})
	}
	return r
}

var ErrCaptureConflict = errors.New("capture generation changed or management operation in progress")

// Resync replaces the subscription with a fresh native snapshot-plus-deltas
// boundary. A failed or canceled attempt leaves an invalid generation, never a
// successful continuation of the old one. It does not discard durable backlog.
func (m *Manager) Resync(ctx context.Context, expected string) (CaptureReport, error) {
	if m == nil {
		return CaptureReport{}, fmt.Errorf("kafka is disabled")
	}
	if !m.lifecycle.TryLock() {
		return m.CaptureStatus(), ErrCaptureConflict
	}
	defer m.lifecycle.Unlock()
	if err := ctx.Err(); err != nil {
		return m.CaptureStatus(), err
	}
	old := m.capture.Load()
	if m.closed || !m.started || m.source == nil {
		return m.CaptureStatus(), fmt.Errorf("kafka observation is not active")
	}
	if expected == "" || old == nil || expected != old.generation {
		return m.CaptureStatus(), ErrCaptureConflict
	}
	for _, c := range old.states {
		c.invalidate(lossSuperseded)
	}
	b := m.newCapture(true)
	fail := func(err error) (CaptureReport, error) {
		for _, c := range b.states {
			c.invalidate(lossSubscription)
		}
		return m.CaptureStatus(), err
	}
	if m.subscription != nil {
		sub := m.subscription
		m.subscription = nil
		sub.Stop()
		select {
		case <-sub.Done():
		case <-ctx.Done():
			return fail(ctx.Err())
		}
	}
	sub, err := m.subscribe(m.source, b)
	if err != nil {
		return fail(err)
	}
	m.subscription = sub
	m.monitorCapture(b, sub)
	if err := ctx.Err(); err != nil {
		return fail(err)
	}
	return m.CaptureStatus(), nil
}

func (s *sink) invalidateCapture(reason uint32) { s.capture.Load().invalidate(reason) }

// Callers holding old subscription callbacks retain their original states;
// they can neither stamp new generations nor invalidate a replacement capture.
func (m *Manager) captureDrop(b *captureBatch) {
	m.metrics.ingressDropped.Inc()
	for _, c := range b.states {
		c.invalidate(lossIngress)
	}
}

func (m *Manager) monitorCapture(b *captureBatch, sub event.Subscription) {
	if sub == nil {
		return
	}
	go func() {
		<-sub.Done()
		for _, c := range b.states {
			c.invalidate(lossSubscription)
		}
	}()
}
