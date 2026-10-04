package kafka

import (
	"bytes"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	bolt "go.etcd.io/bbolt"

	"github.com/stretchr/testify/require"
)

func TestDurableStoreQuotaAndRecovery(t *testing.T) {
	path := filepath.Join(t.TempDir(), "events.db")
	s, err := openStore(path, 1024)
	require.NoError(t, err)
	first := &record{Version: 1, Value: []byte(`{"table":{}}`), Key: []byte("peer")}
	n, err := s.append([]*record{first})
	require.NoError(t, err)
	require.Equal(t, 1, n)
	require.Equal(t, int64(1), s.count.Load())
	second := &record{Version: 1, Value: []byte(`{"table":{}}`), Key: []byte("peer")}
	n, err = s.append([]*record{second})
	require.NoError(t, err)
	require.Equal(t, 1, n)
	third := &record{Version: 1, Value: []byte(`{"table":{}}`), Key: make([]byte, 2048)}
	n, err = s.append([]*record{third})
	require.NoError(t, err)
	require.Zero(t, n)
	require.Zero(t, third.Sequence)
	pending, err := s.candidates(10, nil, "peer")
	require.NoError(t, err)
	require.Len(t, pending, 1)
	require.Equal(t, first.Sequence, pending[0].Sequence)
	require.NoError(t, s.close())
	s, err = openStore(path, 1024)
	require.NoError(t, err)
	defer s.close()
	require.Equal(t, int64(2), s.count.Load())
	require.Equal(t, second.Sequence, s.initialSequence)
	require.NoError(t, s.remove(first.Sequence))
	pending, err = s.candidates(10, nil, "peer")
	require.NoError(t, err)
	require.Len(t, pending, 1)
	require.Equal(t, second.Sequence, pending[0].Sequence)
	require.NoError(t, s.remove(second.Sequence))
	require.Zero(t, s.count.Load())
	require.Zero(t, s.bytes.Load())
}

func TestDurableStoreRejectsCorruptionAndLock(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "locked.db")
	s, err := openStore(path, 1024)
	require.NoError(t, err)
	_, err = openStore(path, 1024)
	require.ErrorContains(t, err, "process holding this file")
	require.NoError(t, s.close())
	corrupt := filepath.Join(dir, "corrupt.db")
	require.NoError(t, os.WriteFile(corrupt, []byte("corrupt"), 0o600))
	_, err = openStore(corrupt, 1024)
	require.ErrorContains(t, err, "pending events will be lost")
	data, err := os.ReadFile(corrupt)
	require.NoError(t, err)
	require.Equal(t, "corrupt", string(data))
}

func TestDurableStoreSurvivesAbruptExit(t *testing.T) {
	if path := os.Getenv("GOBGP_KAFKA_CRASH_TEST_PATH"); path != "" {
		s, err := openStore(path, 1<<20)
		if err != nil {
			os.Exit(2)
		}
		if _, err = s.append([]*record{{Version: 1, Value: []byte(`{"table":{}}`), Key: []byte("durable")}}); err != nil {
			os.Exit(3)
		}
		os.Exit(0) // Intentionally skip Close and all deferred cleanup.
	}
	path := filepath.Join(t.TempDir(), "crash.db")
	command := exec.Command(os.Args[0], "-test.run=^TestDurableStoreSurvivesAbruptExit$")
	command.Env = append(os.Environ(), "GOBGP_KAFKA_CRASH_TEST_PATH="+path)
	require.NoError(t, command.Run())
	s, err := openStore(path, 1<<20)
	require.NoError(t, err)
	defer s.close()
	pending, err := s.candidates(10, nil, "peer")
	require.NoError(t, err)
	require.Len(t, pending, 1)
	require.Equal(t, []byte("durable"), pending[0].Key)
}

func TestDurableStoreDetectsPayloadCorruption(t *testing.T) {
	path := filepath.Join(t.TempDir(), "checksum.db")
	s, err := openStore(path, 1<<20)
	require.NoError(t, err)
	r := &record{Version: 1, Value: []byte(`{"table":{}}`), Key: []byte("peer")}
	_, err = s.append([]*record{r})
	require.NoError(t, err)
	require.NoError(t, s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(recordsBucket)
		data := append([]byte(nil), b.Get(sequenceKey(r.Sequence))...)
		// A syntactically valid modification must still be rejected by checksum.
		data = bytes.Replace(data, []byte(`cGVlcg==`), []byte(`cGVlcw==`), 1)
		return b.Put(sequenceKey(r.Sequence), data)
	}))
	require.NoError(t, s.close())
	_, err = openStore(path, 1<<20)
	require.ErrorContains(t, err, "corrupt record")
}

func TestDurableIndexSkipsOccupiedLaneAndRebuilds(t *testing.T) {
	path := filepath.Join(t.TempDir(), "lanes.db")
	s, err := openStore(path, 1<<20, "peer")
	require.NoError(t, err)
	batch := make([]*record, 0, 1001)
	for range 1000 {
		batch = append(batch, &record{Version: 1, Value: []byte(`{"table":{}}`), Key: []byte("busy-peer")})
	}
	batch = append(batch, &record{Version: 1, Value: []byte(`{"table":{}}`), Key: []byte("other-peer")})
	n, err := s.append(batch)
	require.NoError(t, err)
	require.Equal(t, 1001, n)
	candidates, err := s.candidates(10, map[string]bool{"busy-peer": true}, "peer")
	require.NoError(t, err)
	require.Len(t, candidates, 1)
	require.Equal(t, []byte("other-peer"), candidates[0].Key)
	require.NoError(t, s.close())
	s, err = openStore(path, 1<<20, "none")
	require.NoError(t, err)
	defer s.close()
	candidates, err = s.candidates(10, nil, "none")
	require.NoError(t, err)
	require.Len(t, candidates, 10)
}
