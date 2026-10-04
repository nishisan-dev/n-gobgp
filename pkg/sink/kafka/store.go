package kafka

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sync/atomic"
	"time"

	"github.com/osrg/gobgp/v4/pkg/event"
	"github.com/twmb/franz-go/pkg/kgo"
	bolt "go.etcd.io/bbolt"
)

var (
	lanesBucket    = []byte("pending-by-lane")
	recordsBucket  = []byte("records")
	metadataBucket = []byte("metadata")
	errSpoolFull   = errors.New("durable backlog limit reached")
)

// record is an internal delivery envelope. Value is the unmodified ProtoJSON.
type record struct {
	Destination *destination `json:",omitempty"`
	Checksum    string
	Version     int
	Sequence    uint64 `json:"-"`
	Value       []byte
	Key         []byte
	Headers     []kgo.RecordHeader
	Metadata    event.Metadata
}

// Broker seeds identify the configured original cluster without storing secrets.
type destination struct {
	Cluster, Topic string
	Brokers        []string
}

func (s *store) destinations() ([]destination, error) {
	var out []destination
	seen := map[string]bool{}
	err := s.db.View(func(tx *bolt.Tx) error {
		return tx.Bucket(recordsBucket).ForEach(func(_, value []byte) error {
			var r record
			if err := decodeRecord(value, &r); err != nil {
				return err
			}
			if r.Destination == nil {
				return fmt.Errorf("pending record has no original destination; explicitly use replayDestination=current to migrate legacy backlog")
			}
			key, _ := json.Marshal(r.Destination)
			if !seen[string(key)] {
				out = append(out, *r.Destination)
				seen[string(key)] = true
			}
			return nil
		})
	})
	return out, err
}

// window reads a bounded sequence-ordered delivery batch, including many records
// from one peer. No transaction or mmap-backed value survives the call.
func (s *store) window(limit int) ([]*record, error) {
	out := make([]*record, 0, limit)
	err := s.db.View(func(tx *bolt.Tx) error {
		c := tx.Bucket(recordsBucket).Cursor()
		for key, value := c.First(); key != nil && len(out) < limit; key, value = c.Next() {
			r := new(record)
			if err := decodeRecord(value, r); err != nil {
				return err
			}
			r.Sequence = binary.BigEndian.Uint64(key)
			out = append(out, r)
		}
		return nil
	})
	return out, err
}

type store struct {
	db              *bolt.DB
	limit           int64
	bytes           atomic.Int64
	count           atomic.Int64
	initialSequence uint64
	strategy        string
}

func openStore(path string, limit int64, strategies ...string) (*store, error) {
	db, err := bolt.Open(path, 0o600, &bolt.Options{Timeout: time.Second})
	if err != nil {
		return nil, fmt.Errorf("open durable queue %s: %w; correct permissions or stop the process holding this file; if corrupted, remove this file manually (all pending events will be lost)", path, err)
	}
	strategy := "peer"
	if len(strategies) > 0 {
		strategy = strategies[0]
	}
	s := &store{db: db, limit: limit, strategy: strategy}
	fail := func(err error) (*store, error) {
		_ = db.Close()
		return nil, fmt.Errorf("invalid durable queue %s: %w; if corrupted, remove this file manually (all pending events will be lost)", path, err)
	}
	// Check existing pages before writing anything to a potentially damaged file.
	if err := db.View(func(tx *bolt.Tx) error {
		var first error
		for err := range tx.Check() {
			if first == nil {
				first = err
			}
		}
		return first
	}); err != nil {
		return fail(err)
	}
	if err := db.Update(func(tx *bolt.Tx) error {
		meta, err := tx.CreateBucketIfNotExists(metadataBucket)
		if err != nil {
			return err
		}
		if v := meta.Get([]byte("version")); v != nil && string(v) != "1" {
			return fmt.Errorf("unsupported spool version %q", v)
		}
		if err := meta.Put([]byte("version"), []byte("1")); err != nil {
			return err
		}
		b, err := tx.CreateBucketIfNotExists(recordsBucket)
		if err != nil {
			return err
		}
		s.initialSequence = b.Sequence()
		rebuild := tx.Bucket(lanesBucket) == nil || string(meta.Get([]byte("index-strategy"))) != strategy
		if rebuild && tx.Bucket(lanesBucket) != nil {
			if err := tx.DeleteBucket(lanesBucket); err != nil {
				return err
			}
		}
		lanes, err := tx.CreateBucketIfNotExists(lanesBucket)
		if err != nil {
			return err
		}
		if err := meta.Put([]byte("index-strategy"), []byte(strategy)); err != nil {
			return err
		}
		if err := b.ForEach(func(k, v []byte) error {
			var r record
			if len(k) != 8 || decodeRecord(v, &r) != nil {
				return fmt.Errorf("corrupt record")
			}
			r.Sequence = binary.BigEndian.Uint64(k)
			indexKey := laneIndexKey(recordLane(&r, strategy), r.Sequence)
			if rebuild {
				if err := lanes.Put(indexKey, k); err != nil {
					return err
				}
			} else if !bytes.Equal(lanes.Get(indexKey), k) {
				return fmt.Errorf("corrupt durable lane index")
			}
			s.bytes.Add(int64(len(v)))
			s.count.Add(1)
			return nil
		}); err != nil {
			return err
		}
		indexCount := int64(0)
		if err := lanes.ForEach(func(k, v []byte) error {
			if len(k) != 40 || len(v) != 8 {
				return fmt.Errorf("corrupt durable lane index")
			}
			indexCount++
			return nil
		}); err != nil {
			return err
		}
		if indexCount != s.count.Load() {
			return fmt.Errorf("corrupt durable lane index")
		}
		return nil
	}); err != nil {
		return fail(err)
	}
	return s, nil
}

func sequenceKey(n uint64) []byte {
	key := make([]byte, 8)
	binary.BigEndian.PutUint64(key, n)
	return key
}

func (s *store) append(batch []*record) (int, error) {
	var addedBytes int64
	added := 0
	err := s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(recordsBucket)
		for _, r := range batch {
			data, err := encodeRecord(r)
			if err != nil {
				return err
			}
			if s.bytes.Load()+addedBytes+int64(len(data)) > s.limit {
				continue
			}
			seq, err := b.NextSequence()
			if err != nil {
				return err
			}
			if err := b.Put(sequenceKey(seq), data); err != nil {
				return err
			}
			r.Sequence = seq
			if err := tx.Bucket(lanesBucket).Put(laneIndexKey(recordLane(r, s.strategy), seq), sequenceKey(seq)); err != nil {
				return err
			}
			addedBytes += int64(len(data))
			added++
		}
		return nil
	})
	if err != nil {
		for _, r := range batch {
			r.Sequence = 0
		}
		return 0, err
	}
	s.bytes.Add(addedBytes)
	s.count.Add(int64(added))
	return added, nil
}

// candidates returns the first pending record for each unoccupied lane. A read
// transaction is never held across Kafka I/O, and mmap-backed values are copied.
func (s *store) candidates(limit int, occupied map[string]bool, strategy string) ([]*record, error) {
	if strategy != s.strategy {
		return nil, fmt.Errorf("durable index strategy changed without restart")
	}
	out := make([]*record, 0, limit)
	skipped := map[[32]byte]bool{}
	for lane := range occupied {
		skipped[sha256.Sum256([]byte(lane))] = true
	}
	err := s.db.View(func(tx *bolt.Tx) error {
		records := tx.Bucket(recordsBucket)
		cursor := tx.Bucket(lanesBucket).Cursor()
		for key, value := cursor.First(); key != nil && len(out) < limit; {
			if len(key) != 40 || len(value) != 8 {
				return fmt.Errorf("corrupt durable lane index")
			}
			var digest [32]byte
			copy(digest[:], key[:32])
			if !skipped[digest] {
				r := new(record)
				if err := decodeRecord(records.Get(value), r); err != nil {
					return err
				}
				r.Sequence = binary.BigEndian.Uint64(value)
				if !bytes.Equal(key, laneIndexKey(recordLane(r, strategy), r.Sequence)) {
					return fmt.Errorf("corrupt durable lane index")
				}
				out = append(out, r)
			}
			// Seek past the entire lane rather than decoding every record for a busy
			// peer. Work stays proportional to peer lanes, not backlog depth.
			next := nextLane(digest)
			if next == nil {
				break
			}
			key, value = cursor.Seek(next)
		}
		return nil
	})
	return out, err
}

func laneIndexKey(lane string, sequence uint64) []byte {
	digest := sha256.Sum256([]byte(lane))
	key := make([]byte, 40)
	copy(key, digest[:])
	binary.BigEndian.PutUint64(key[32:], sequence)
	return key
}

func nextLane(digest [32]byte) []byte {
	for i := len(digest) - 1; i >= 0; i-- {
		digest[i]++
		if digest[i] != 0 {
			return digest[:]
		}
	}
	return nil
}

func recordLane(r *record, strategy string) string {
	if strategy == "none" {
		return fmt.Sprintf("record:%d", r.Sequence)
	}
	return string(r.Key)
}

func (s *store) remove(sequence uint64) error {
	return s.removeBatch([]uint64{sequence})
}

func (s *store) removeBatch(sequences []uint64) error {
	var removedBytes int64
	var removedCount int64
	err := s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(recordsBucket)
		for _, sequence := range sequences {
			key := sequenceKey(sequence)
			if v := b.Get(key); v != nil {
				var r record
				if err := decodeRecord(v, &r); err != nil {
					return err
				}
				r.Sequence = sequence
				if err := tx.Bucket(lanesBucket).Delete(laneIndexKey(recordLane(&r, s.strategy), sequence)); err != nil {
					return err
				}
				removedBytes += int64(len(v))
				removedCount++
				if err := b.Delete(key); err != nil {
					return err
				}
			}
		}
		return nil
	})
	if err == nil && removedBytes > 0 {
		s.bytes.Add(-removedBytes)
		s.count.Add(-removedCount)
	}
	return err
}

func (s *store) fileSize() int64 {
	info, err := os.Stat(s.db.Path())
	if err != nil {
		return 0
	}
	return info.Size()
}

func (s *store) close() error {
	if s == nil {
		return nil
	}
	return s.db.Close()
}

func encodeRecord(r *record) ([]byte, error) {
	r.Checksum = ""
	data, err := json.Marshal(r)
	if err != nil {
		return nil, err
	}
	digest := sha256.Sum256(data)
	r.Checksum = hex.EncodeToString(digest[:])
	return json.Marshal(r)
}

func decodeRecord(data []byte, r *record) error {
	if err := json.Unmarshal(data, r); err != nil {
		return err
	}
	if r.Version != 1 || len(r.Value) == 0 {
		return fmt.Errorf("invalid durable record version or payload")
	}
	expected := r.Checksum
	_, err := encodeRecord(r)
	if err != nil {
		return err
	}
	if expected != r.Checksum {
		return fmt.Errorf("durable record checksum mismatch")
	}
	return nil
}
