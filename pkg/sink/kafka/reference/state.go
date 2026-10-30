// Package reference implements a bounded, fail-closed consumer for the capture
// contract. It is a reference materialized view, not an exactly-once database.
package reference

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/osrg/gobgp/v4/api"
	"github.com/osrg/gobgp/v4/pkg/apiutil"
	"github.com/osrg/gobgp/v4/pkg/event"
	"github.com/osrg/gobgp/v4/pkg/sink/kafka"
	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

func Header(r *kgo.Record, key string) string {
	for _, h := range r.Headers {
		if h.Key == key {
			return string(h.Value)
		}
	}
	return ""
}

// State is owned by one consumer loop. Sequence numbers are global to a sink
// generation, so records from different partitions must be reordered, not
// mistaken for loss at their first out-of-order arrival.
type State struct {
	Sink             string
	MaxPending       int
	MaxPendingBytes  int
	MaxRoutes        int
	generation       string
	status           kafka.CaptureStatus
	applied          uint64
	pending          map[uint64]*kgo.Record
	pendingBytes     int
	routes           map[string]*api.Path
	snapshotID       string
	snapshotCount    int64
	snapshotComplete bool
	invalid          bool
	gapSince         time.Time
}

func New(sink string) *State {
	return &State{Sink: sink, MaxPending: 16384, MaxPendingBytes: 32 << 20, MaxRoutes: 1000000}
}

func (s *State) ObserveStatus(r kafka.CaptureReport) error {
	if r.Contract != kafka.CaptureContractVersion || r.Generation == "" {
		s.invalid = true
		return fmt.Errorf("unsupported or unavailable capture contract")
	}
	var found *kafka.CaptureStatus
	for i := range r.Sinks {
		if r.Sinks[i].Sink == s.Sink {
			found = &r.Sinks[i]
			break
		}
	}
	if found == nil || found.Generation != r.Generation {
		s.invalid = true
		return fmt.Errorf("sink capture is unavailable")
	}
	if s.generation != r.Generation {
		s.generation = r.Generation
		s.applied = 0
		s.pending = make(map[uint64]*kgo.Record)
		s.pendingBytes = 0
		s.routes = make(map[string]*api.Path)
		s.snapshotID = ""
		s.snapshotCount = 0
		s.snapshotComplete = false
		s.invalid = false
		s.gapSince = time.Now()
	}
	matcher, err := event.NewMatcher(found.Match)
	if err != nil || !matcher.AllowsSource("adj-in") || !matcher.AllowsEventType("route") || !matcher.AllowsEventType("withdraw") {
		s.invalid = true
		return fmt.Errorf("reference state requires Adj-RIB-In route and withdrawal events")
	}
	// Persistence can outlast the consumer gap budget. Start that budget only
	// when the producer has completed this snapshot, rather than replacing a
	// healthy capture while its records are still being written.
	if s.status.State != "ready" && found.State == "ready" || s.status.LastPublishedSequence <= s.applied && found.LastPublishedSequence > s.applied {
		s.gapSince = time.Now()
	}
	s.status = *found
	if found.State == "invalid" {
		s.invalid = true
		s.pending = make(map[uint64]*kgo.Record)
		s.pendingBytes = 0
	}
	if found.LastSequence > s.applied && s.gapSince.IsZero() {
		s.gapSince = time.Now()
	}
	return nil
}

func (s *State) Ingest(r *kgo.Record) error {
	if Header(r, "sink") != s.Sink || Header(r, "capture-generation") != s.generation {
		return nil
	}
	if s.invalid {
		return nil
	}
	fail := func(err error) error { s.invalid = true; return err }
	if Header(r, "capture-contract") != kafka.CaptureContractVersion {
		return fail(fmt.Errorf("unsupported record contract"))
	}
	seq, err := strconv.ParseUint(Header(r, "capture-sequence"), 10, 64)
	if err != nil || seq == 0 {
		return fail(fmt.Errorf("invalid capture sequence"))
	}
	if seq <= s.applied {
		return nil
	}
	if old := s.pending[seq]; old != nil {
		if !bytes.Equal(old.Value, r.Value) {
			return fail(fmt.Errorf("conflicting record at one capture sequence"))
		}
		return nil
	}
	size := len(r.Value)
	for _, h := range r.Headers {
		size += len(h.Key) + len(h.Value)
	}
	if len(s.pending) >= s.MaxPending || size > s.MaxPendingBytes-s.pendingBytes {
		return fail(fmt.Errorf("consumer reorder budget exceeded"))
	}
	// Retain only fields needed by the contract, so a key or context cannot
	// pin an unrelated fetch buffer outside the reorder byte budget.
	copyRecord := kgo.Record{}
	copyRecord.Value = bytes.Clone(r.Value)
	copyRecord.Headers = make([]kgo.RecordHeader, len(r.Headers))
	for i, h := range r.Headers {
		copyRecord.Headers[i] = kgo.RecordHeader{Key: strings.Clone(h.Key), Value: bytes.Clone(h.Value)}
	}
	s.pending[seq] = &copyRecord
	s.pendingBytes += size
	for {
		next := s.applied + 1
		v := s.pending[next]
		if v == nil {
			break
		}
		delete(s.pending, next)
		s.pendingBytes -= len(v.Value)
		for _, h := range v.Headers {
			s.pendingBytes -= len(h.Key) + len(h.Value)
		}
		if err := s.apply(v); err != nil {
			return fail(err)
		}
		s.applied = next
		s.gapSince = time.Now()
	}
	return nil
}

func (s *State) apply(r *kgo.Record) error {
	phase, id := Header(r, "snapshot-phase"), Header(r, "snapshot-id")
	switch phase {
	case "begin":
		if s.applied != 0 || id == "" || s.snapshotID != "" {
			return fmt.Errorf("invalid snapshot begin")
		}
		s.snapshotID = id
	case "failed":
		return fmt.Errorf("producer snapshot failed")
	case "end":
		count, err := strconv.ParseInt(Header(r, "snapshot-records"), 10, 64)
		if err != nil || s.snapshotID == "" || id != s.snapshotID || count != s.snapshotCount {
			return fmt.Errorf("incomplete snapshot")
		}
		s.snapshotComplete = true
	case "data":
		if s.snapshotID == "" || id != s.snapshotID || s.snapshotComplete {
			return fmt.Errorf("snapshot data outside its boundary")
		}
		s.snapshotCount++
	case "":
		if !s.snapshotComplete {
			return fmt.Errorf("delta before a complete snapshot")
		}
	default:
		return fmt.Errorf("unknown snapshot phase")
	}
	if phase == "begin" || phase == "end" {
		return nil
	}
	// The reference view is Adj-RIB-In. Other selected sources are sequenced
	// and counted, but never mixed into this routing identity space.
	if Header(r, "source") != "adj-in" || Header(r, "event-type") == "eor" {
		return nil
	}
	if Header(r, "serializer") != "protojson" {
		return fmt.Errorf("unsupported serializer")
	}
	var response api.WatchEventResponse
	if err := protojson.Unmarshal(r.Value, &response); err != nil {
		return err
	}
	for _, p := range response.GetTable().GetPaths() {
		key, err := RouteKey(p)
		if err != nil {
			return err
		}
		if p.IsWithdraw {
			delete(s.routes, key)
		} else {
			if s.routes[key] == nil && len(s.routes) >= s.MaxRoutes {
				return fmt.Errorf("consumer route budget exceeded")
			}
			s.routes[key] = p
		}
	}
	return nil
}

func RouteKey(p *api.Path) (string, error) {
	n, err := apiutil.GetNativeNlri(p)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%s|%d/%d|%s|%d", p.NeighborIp, p.Family.Afi, p.Family.Safi, n.String(), p.Identifier), nil
}

func (s *State) NeedsResync(now time.Time, gapTimeout time.Duration) bool {
	if !s.status.StorageHealthy || !s.status.Publishing {
		return false
	}
	return s.invalid || !s.status.SnapshotRequested || s.status.State == "ready" && s.status.LastPublishedSequence > s.applied && !s.gapSince.IsZero() && now.Sub(s.gapSince) > gapTimeout
}

func (s *State) Ready() bool {
	return !s.invalid && s.status.State == "ready" && s.snapshotComplete && s.applied >= s.status.LastSequence && s.applied >= s.status.SnapshotEndSequence
}
func (s *State) Generation() string { return s.generation }
func (s *State) Applied() uint64    { return s.applied }
func (s *State) RouteCount() int    { return len(s.routes) }
func (s *State) Keys() []string {
	keys := make([]string, 0, len(s.routes))
	for k := range s.routes {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// DigestPaths includes routing attributes and source identity, excluding age
// and API inspection-only flags. It supports independent RIB comparison.
func DigestPaths(paths []*api.Path) (string, error) {
	values := make(map[string][]byte, len(paths))
	keys := make([]string, 0, len(paths))
	for _, p := range paths {
		key, err := RouteKey(p)
		if err != nil {
			return "", err
		}
		value, err := proto.MarshalOptions{Deterministic: true}.Marshal(&api.Path{Nlri: p.Nlri, Pattrs: p.Pattrs, Family: p.Family, SourceAsn: p.SourceAsn, SourceId: p.SourceId, NeighborIp: p.NeighborIp, Identifier: p.Identifier})
		if err != nil {
			return "", err
		}
		if _, exists := values[key]; !exists {
			keys = append(keys, key)
		}
		values[key] = value
	}
	sort.Strings(keys)
	h := sha256.New()
	for _, key := range keys {
		_, _ = h.Write([]byte(key + "\n"))
		_, _ = h.Write(values[key])
		_, _ = h.Write([]byte{0})
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

func (s *State) Digest() string {
	paths := make([]*api.Path, 0, len(s.routes))
	for _, p := range s.routes {
		paths = append(paths, p)
	}
	digest, err := DigestPaths(paths)
	if err != nil {
		return "invalid"
	}
	return digest
}
