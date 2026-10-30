package reference

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/osrg/gobgp/v4/api"
	"github.com/osrg/gobgp/v4/pkg/apiutil"
	"github.com/osrg/gobgp/v4/pkg/event"
	"github.com/osrg/gobgp/v4/pkg/packet/bgp"
	"github.com/osrg/gobgp/v4/pkg/sink/kafka"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

func report(generation string, last, end uint64) kafka.CaptureReport {
	return kafka.CaptureReport{Contract: "1", Generation: generation, Sinks: []kafka.CaptureStatus{{Sink: "raw", Generation: generation, State: "ready", SnapshotRequested: true, StorageHealthy: true, Publishing: true, LastSequence: last, LastPublishedSequence: last, SnapshotEndSequence: end}}}
}

func path(t *testing.T, prefix string, id uint32) *api.Path {
	n, err := bgp.NewIPAddrPrefix(netip.MustParsePrefix(prefix))
	require.NoError(t, err)
	p, err := apiutil.NewPath(bgp.RF_IPv4_UC, n, false, []bgp.PathAttributeInterface{bgp.NewPathAttributeOrigin(0)}, time.Now())
	require.NoError(t, err)
	p.NeighborIp = "192.0.2.1"
	p.Identifier = id
	return p
}

func record(t *testing.T, generation string, seq uint64, phase string, p *api.Path) *kgo.Record {
	response := &api.WatchEventResponse{}
	if p != nil {
		response.Event = &api.WatchEventResponse_Table{Table: &api.WatchEventResponse_TableEvent{Paths: []*api.Path{p}}}
	}
	value, err := protojson.Marshal(response)
	require.NoError(t, err)
	r := &kgo.Record{Value: value}
	for k, v := range map[string]string{"sink": "raw", "capture-contract": "1", "capture-generation": generation, "capture-sequence": strconv.FormatUint(seq, 10), "source": "adj-in", "event-type": "route", "serializer": "protojson"} {
		r.Headers = append(r.Headers, kgo.RecordHeader{Key: k, Value: []byte(v)})
	}
	if phase != "" {
		r.Headers = append(r.Headers, kgo.RecordHeader{Key: "snapshot-id", Value: []byte("snapshot")}, kgo.RecordHeader{Key: "snapshot-phase", Value: []byte(phase)})
	}
	if phase == "end" {
		r.Headers = append(r.Headers, kgo.RecordHeader{Key: "snapshot-records", Value: []byte("1")})
	}
	return r
}

func TestReferenceReordersPartitionsAndDeduplicates(t *testing.T) {
	s := New("raw")
	require.NoError(t, s.ObserveStatus(report("g", 4, 3)))
	p := path(t, "203.0.113.0/24", 11)
	for _, r := range []*kgo.Record{record(t, "g", 3, "end", nil), record(t, "g", 2, "data", p), record(t, "g", 1, "begin", nil)} {
		require.NoError(t, s.Ingest(r))
	}
	require.Equal(t, 1, s.RouteCount())
	require.False(t, s.Ready())
	w := proto.Clone(p).(*api.Path)
	w.IsWithdraw = true
	require.NoError(t, s.Ingest(record(t, "g", 4, "", w)))
	require.True(t, s.Ready())
	require.Zero(t, s.RouteCount())
	require.NoError(t, s.Ingest(record(t, "g", 2, "data", p)))
	require.Zero(t, s.RouteCount())
}

func TestReferenceLossIsLatchedAndOldReplayCannotValidateNewCapture(t *testing.T) {
	s := New("raw")
	r := report("old", 3, 3)
	require.NoError(t, s.ObserveStatus(r))
	p := path(t, "203.0.113.0/24", 11)
	for _, v := range []*kgo.Record{record(t, "old", 1, "begin", nil), record(t, "old", 2, "data", p), record(t, "old", 3, "end", nil)} {
		require.NoError(t, s.Ingest(v))
	}
	require.True(t, s.Ready())
	r.Sinks[0].State = "invalid"
	require.NoError(t, s.ObserveStatus(r))
	require.False(t, s.Ready())
	r.Sinks[0].State = "ready"
	require.NoError(t, s.ObserveStatus(r))
	require.False(t, s.Ready(), "a stale status reply cannot erase observed loss")
	require.NoError(t, s.ObserveStatus(report("new", 3, 3)))
	require.NoError(t, s.Ingest(record(t, "old", 3, "end", nil)))
	require.False(t, s.Ready())
	require.Zero(t, s.RouteCount())
	for _, v := range []*kgo.Record{record(t, "new", 1, "begin", nil), record(t, "new", 2, "data", path(t, "198.51.100.0/24", 22)), record(t, "new", 3, "end", nil)} {
		require.NoError(t, s.Ingest(v))
	}
	require.True(t, s.Ready())
	require.Equal(t, 1, s.RouteCount())
	require.NotContains(t, s.Keys()[0], "203.0.113")
}

func TestReferenceMissingLastRecordAndReorderBudgetRequireRecovery(t *testing.T) {
	s := New("raw")
	require.NoError(t, s.ObserveStatus(report("g", 3, 3)))
	require.NoError(t, s.Ingest(record(t, "g", 1, "begin", nil)))
	require.False(t, s.Ready())
	require.True(t, s.NeedsResync(time.Now().Add(time.Minute), time.Second))
	s = New("raw")
	s.MaxPending = 1
	require.NoError(t, s.ObserveStatus(report("g", 3, 3)))
	require.NoError(t, s.Ingest(record(t, "g", 3, "end", nil)))
	require.Error(t, s.Ingest(record(t, "g", 2, "data", path(t, "203.0.113.0/24", 1))))
	require.False(t, s.Ready())
	require.True(t, s.NeedsResync(time.Now(), time.Second))
	// A small retained record must not pin an unrelated large Kafka key/context.
	s = New("raw")
	require.NoError(t, s.ObserveStatus(report("g", 3, 3)))
	buffered := record(t, "g", 3, "end", nil)
	buffered.Key = make([]byte, 1<<20)
	type retentionContextKey struct{}
	buffered.Context = context.WithValue(context.Background(), retentionContextKey{}, make([]byte, 1<<20))
	require.NoError(t, s.Ingest(buffered))
	require.Empty(t, s.pending[3].Key)
	require.Nil(t, s.pending[3].Context)
}

func TestReferenceRefusesViewsWithoutWithdrawalsAndUnsupportedContract(t *testing.T) {
	s := New("raw")
	r := report("g", 3, 3)
	r.Sinks[0].Match.EventTypes = event.Filter[string]{Include: []string{"route"}}
	require.Error(t, s.ObserveStatus(r))
	require.False(t, s.Ready())
	r = report("g", 3, 3)
	r.Contract = "unknown"
	require.Error(t, s.ObserveStatus(r))
	require.False(t, s.Ready())
}

func TestReferenceDigestIncludesAttributeChangesAndAddPathIdentity(t *testing.T) {
	a := path(t, "203.0.113.0/24", 11)
	b := proto.Clone(a).(*api.Path)
	b.Identifier = 22
	first, err := DigestPaths([]*api.Path{a, b})
	require.NoError(t, err)
	b.Pattrs, err = apiutil.MarshalPathAttributes([]bgp.PathAttributeInterface{bgp.NewPathAttributeOrigin(1)})
	require.NoError(t, err)
	second, err := DigestPaths([]*api.Path{a, b})
	require.NoError(t, err)
	require.NotEqual(t, first, second)
	reverse, err := DigestPaths([]*api.Path{b, a})
	require.NoError(t, err)
	require.Equal(t, second, reverse)
}

func TestReferenceRefreshesGenerationBeforeAdvancingNewSnapshotOffsets(t *testing.T) {
	var available atomic.Bool
	httpServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !available.Load() {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		_ = json.NewEncoder(w).Encode(report("new", 3, 3))
	}))
	defer httpServer.Close()
	s := New("raw")
	require.NoError(t, s.ObserveStatus(report("old", 3, 3)))
	records := []*kgo.Record{record(t, "new", 1, "begin", nil), record(t, "new", 2, "data", path(t, "203.0.113.0/24", 11)), record(t, "new", 3, "end", nil)}
	control := ControlClient{URL: httpServer.URL}
	require.ErrorIs(t, control.IngestBatch(context.Background(), s, records), ErrControlUnavailable)
	require.Zero(t, s.Applied(), "keep the fetched batch until the control fence can be read")
	available.Store(true)
	require.NoError(t, control.IngestBatch(context.Background(), s, records))
	require.Equal(t, "new", s.Generation())
	require.True(t, s.Ready())
	require.Equal(t, 1, s.RouteCount())
}

func TestReferenceWaitsForSnapshotPersistenceBeforeGapRecovery(t *testing.T) {
	s := New("raw")
	r := report("g", 5002, 0)
	r.Sinks[0].State = "capturing"
	require.NoError(t, s.ObserveStatus(r))
	s.gapSince = time.Now().Add(-time.Hour)
	require.False(t, s.NeedsResync(time.Now(), time.Second), "do not replace a healthy snapshot still being persisted")
	r.Sinks[0].State = "ready"
	r.Sinks[0].SnapshotEndSequence = 5002
	require.NoError(t, s.ObserveStatus(r))
	require.False(t, s.NeedsResync(time.Now(), time.Second), "delivery gets its own no-progress budget after persistence")
	require.True(t, s.NeedsResync(time.Now().Add(time.Minute), time.Second))
	r.Sinks[0].State = "invalid"
	r.Sinks[0].SnapshotEndSequence = 0
	require.NoError(t, s.ObserveStatus(r))
	require.True(t, s.NeedsResync(time.Now(), time.Hour), "explicit loss bypasses the gap timer")
}

func TestReferenceDurableBacklogDoesNotTriggerRecoveryBeforePublication(t *testing.T) {
	s := New("raw")
	r := report("g", 5002, 5002)
	r.Sinks[0].LastPublishedSequence = 0
	require.NoError(t, s.ObserveStatus(r))
	s.gapSince = time.Now().Add(-time.Hour)
	require.False(t, s.NeedsResync(time.Now(), time.Second), "capture queued behind durable replay is transport lag")
	r.Sinks[0].LastPublishedSequence = 5002
	require.NoError(t, s.ObserveStatus(r))
	require.False(t, s.NeedsResync(time.Now(), time.Second), "new publication starts a fresh delivery budget")
	require.True(t, s.NeedsResync(time.Now().Add(time.Minute), time.Second), "published records missing at the consumer require recovery")
}
