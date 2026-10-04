package kafka

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/osrg/gobgp/v4/api"
	config "github.com/osrg/gobgp/v4/pkg/config/kafka"
	"github.com/osrg/gobgp/v4/pkg/event"
	"github.com/osrg/gobgp/v4/pkg/server"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"
)

func TestSpoolFailureIsolatesSinkByDefault(t *testing.T) {
	c := testConfig(t)
	require.NoError(t, os.WriteFile(filepath.Join(c.SpoolDir, "raw.db"), []byte("corrupt"), 0o600))
	c.Sinks["good"] = config.Sink{Cluster: "primary", Topic: "good"}
	p := &fakeProducer{deliveries: make(chan fakeDelivery, 20), autoAck: true}
	m, src := testManager(t, c, p)
	defer closeManager(t, m)
	var failed *sink
	for _, s := range m.sinks {
		if s.name == "raw" {
			failed = s
		}
	}
	require.Nil(t, failed.store)
	require.False(t, failed.up.Load())
	require.Equal(t, float64(1), testutil.ToFloat64(m.metrics.storageErrors.WithLabelValues("primary", "raw", "network.bgp.raw")))
	src.callback(testObservation())
	require.Equal(t, "good", delivery(t, p).record.Topic)
	data, err := os.ReadFile(filepath.Join(c.SpoolDir, "raw.db"))
	require.NoError(t, err)
	require.Equal(t, "corrupt", string(data))
}

func TestAllFailedSinksNeedNoSourceOrProducer(t *testing.T) {
	c := testConfig(t)
	c.SpoolDir = filepath.Join(c.SpoolDir, "file")
	require.NoError(t, os.WriteFile(c.SpoolDir, []byte("not a directory"), 0o600))
	m, err := newManager(c, nil, prometheus.NewRegistry(), func(string, config.Cluster) (producer, error) {
		t.Fatal("failed sink started producer")
		return nil, nil
	})
	require.NoError(t, err)
	require.NoError(t, m.Start(nil))
	closeManager(t, m)
}

func seedBacklog(t *testing.T, c config.Config) {
	t.Helper()
	require.NoError(t, c.Normalize())
	s, err := openStore(filepath.Join(c.SpoolDir, "raw.db"), 1<<20)
	require.NoError(t, err)
	_, err = s.append([]*record{{Version: 1, Value: []byte(`{"table":{}}`), Key: []byte("peer"), Destination: &destination{Cluster: "primary", Topic: "network.bgp.raw", Brokers: c.Clusters["primary"].Brokers}}})
	require.NoError(t, err)
	require.NoError(t, s.close())
}

func TestReplayOriginalKeepsClusterAndTopic(t *testing.T) {
	c := testConfig(t)
	seedBacklog(t, c)
	c.Clusters["analytics"] = config.Cluster{Brokers: []string{"analytics:9092"}}
	c.Sinks["raw"] = config.Sink{Cluster: "analytics", Topic: "new-topic"}
	primary := &fakeProducer{deliveries: make(chan fakeDelivery, 20), autoAck: true}
	analytics := &fakeProducer{deliveries: make(chan fakeDelivery, 20), autoAck: true}
	m, err := newManager(c, nil, nil, func(name string, _ config.Cluster) (producer, error) {
		if name == "primary" {
			return primary, nil
		}
		return analytics, nil
	})
	require.NoError(t, err)
	src := new(fakeSource)
	require.NoError(t, m.Start(src))
	defer closeManager(t, m)
	require.Equal(t, "network.bgp.raw", delivery(t, primary).record.Topic)
	src.callback(testObservation())
	require.Equal(t, "new-topic", delivery(t, analytics).record.Topic)
	waitEmpty(t, m)
}

func TestReplayOriginalRefusesMissingOrChangedCluster(t *testing.T) {
	for _, changed := range []bool{false, true} {
		t.Run(map[bool]string{false: "removed", true: "changed"}[changed], func(t *testing.T) {
			c := testConfig(t)
			seedBacklog(t, c)
			c.Clusters["analytics"] = config.Cluster{Brokers: []string{"analytics:9092"}}
			c.Sinks["raw"] = config.Sink{Cluster: "analytics", Topic: "new-topic"}
			if changed {
				c.Clusters["primary"] = config.Cluster{Brokers: []string{"new-primary:9092"}}
			} else {
				delete(c.Clusters, "primary")
			}
			m, err := newManager(c, nil, nil, func(string, config.Cluster) (producer, error) {
				return &fakeProducer{deliveries: make(chan fakeDelivery, 20)}, nil
			})
			require.NoError(t, err)
			require.False(t, m.sinks[0].up.Load())
			require.NoError(t, m.Start(nil))
			closeManager(t, m)
			s, err := openStore(filepath.Join(c.SpoolDir, "raw.db"), 1<<20)
			require.NoError(t, err)
			defer s.close()
			require.Equal(t, int64(1), s.count.Load())
		})
	}
}

func TestInitialDumpOptInAndCompletionCount(t *testing.T) {
	c := testConfig(t)
	cfg := c.Sinks["raw"]
	cfg.InitialDump = true
	cfg.Match.EventTypes.Include = []string{"route"}
	c.Sinks["raw"] = cfg
	c.Sinks["deltas"] = config.Sink{Cluster: "primary", Topic: "deltas"}
	p := &fakeProducer{deliveries: make(chan fakeDelivery, 20), autoAck: true}
	m, src := testManager(t, c, p)
	defer closeManager(t, m)
	require.True(t, src.options.InitialDump)
	control := testObservation()
	control.Message = &api.WatchEventResponse{}
	control.Metadata = event.Metadata{EventType: "snapshot", SnapshotID: "snapshot-1", SnapshotPhase: "begin", ObservedAt: time.Now()}
	src.callback(control)
	data := testObservation()
	data.Metadata.SnapshotID = "snapshot-1"
	data.Metadata.SnapshotPhase = "data"
	src.callback(data)
	control.Metadata.SnapshotPhase = "end"
	src.callback(control)
	for range 3 {
		d := delivery(t, p)
		require.Equal(t, "network.bgp.raw", d.record.Topic)
		if header(d.record, "snapshot-phase") == "end" {
			require.JSONEq(t, `{}`, string(d.record.Value))
			require.Equal(t, "1", header(d.record, "snapshot-records"))
		}
	}
	src.callback(testObservation())
	a, b := delivery(t, p), delivery(t, p)
	require.NotEqual(t, a.record.Topic, b.record.Topic)
	waitEmpty(t, m)
}

func TestLostIngressInvalidatesBootstrap(t *testing.T) {
	c := testConfig(t)
	cfg := c.Sinks["raw"]
	cfg.InitialDump = true
	c.Sinks["raw"] = cfg
	p := &fakeProducer{deliveries: make(chan fakeDelivery, 20), autoAck: true}
	m, src := testManager(t, c, p)
	defer closeManager(t, m)
	src.options.OnDrop()
	v := testObservation()
	v.Metadata.SnapshotID = "snapshot-1"
	v.Metadata.SnapshotPhase = "end"
	src.callback(v)
	require.Equal(t, "failed", header(delivery(t, p).record, "snapshot-phase"))
	waitEmpty(t, m)
	require.Equal(t, float64(1), testutil.ToFloat64(m.metrics.snapshotFailures.WithLabelValues("primary", "raw", "network.bgp.raw")))
}

func TestQuotaLossCannotCompleteBootstrap(t *testing.T) {
	c := testConfig(t)
	cfg := c.Sinks["raw"]
	cfg.InitialDump = true
	cfg.Spool.MaxBytes = 4096
	c.Sinks["raw"] = cfg
	p := &fakeProducer{deliveries: make(chan fakeDelivery, 20), autoAck: true}
	m, src := testManager(t, c, p)
	defer closeManager(t, m)
	data := testObservation()
	data.Metadata.SnapshotID = "snapshot-1"
	data.Metadata.SnapshotPhase = "data"
	// Force an oversized serialized record while keeping the completion marker small.
	m.sinks[0].offer(data, make([]byte, 8192), "oversized")
	data.Metadata.SnapshotPhase = "end"
	src.callback(data)
	require.Equal(t, "failed", header(delivery(t, p).record, "snapshot-phase"))
	waitEmpty(t, m)
}

func TestLegacyDestinationRequiresExplicitMigration(t *testing.T) {
	c := testConfig(t)
	s, err := openStore(filepath.Join(c.SpoolDir, "raw.db"), 1<<20)
	require.NoError(t, err)
	_, err = s.append([]*record{{Version: 1, Value: []byte(`{}`)}})
	require.NoError(t, err)
	require.NoError(t, s.close())
	m, err := New(c, nil, nil)
	require.NoError(t, err)
	require.False(t, m.sinks[0].up.Load())
	require.NoError(t, m.Close(context.Background()))
	cfg := c.Sinks["raw"]
	cfg.ReplayDestination = "current"
	c.Sinks["raw"] = cfg
	p := &fakeProducer{deliveries: make(chan fakeDelivery, 20), autoAck: true}
	m, _ = testManager(t, c, p)
	defer closeManager(t, m)
	require.Equal(t, "network.bgp.raw", delivery(t, p).record.Topic)
	waitEmpty(t, m)
}

func TestAcknowledgedGapSuspendsApplicationRetry(t *testing.T) {
	p := &fakeProducer{deliveries: make(chan fakeDelivery, 20)}
	m, src := testManager(t, testConfig(t), p)
	src.callback(testObservation())
	src.callback(testObservation())
	first, second := delivery(t, p), delivery(t, p)
	first.complete(kgo.ErrRecordTimeout)
	second.complete(nil)
	require.Eventually(t, func() bool { return !m.sinks[0].up.Load() && m.sinks[0].store.count.Load() == 1 }, time.Second, time.Millisecond)
	select {
	case <-p.deliveries:
		t.Fatal("earlier failed record retried behind an acknowledged later record")
	case <-time.After(250 * time.Millisecond):
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	require.ErrorIs(t, m.Close(ctx), context.DeadlineExceeded)
}

func TestFailedSpoolDoesNotPreventBGPConfiguration(t *testing.T) {
	c := testConfig(t)
	require.NoError(t, os.WriteFile(filepath.Join(c.SpoolDir, "raw.db"), []byte("corrupt"), 0o600))
	m, err := New(c, nil, nil)
	require.NoError(t, err)
	bgpServer := server.NewBgpServer()
	go bgpServer.Serve()
	defer bgpServer.Stop()
	require.NoError(t, m.Start(bgpServer))
	defer closeManager(t, m)
	require.NoError(t, bgpServer.StartBgp(context.Background(), &api.StartBgpRequest{Global: &api.Global{Asn: 65000, RouterId: "192.0.2.10", ListenPort: -1}}))
	require.NoError(t, bgpServer.AddPeer(context.Background(), &api.AddPeerRequest{Peer: &api.Peer{Conf: &api.PeerConf{PeerAsn: 65001, NeighborAddress: "192.0.2.1", AdminDown: true}}}))
	peers := 0
	require.NoError(t, bgpServer.ListPeer(context.Background(), &api.ListPeerRequest{}, func(*api.Peer) { peers++ }))
	require.Equal(t, 1, peers)
}

func TestBootstrapRequestsOnlyOptInViewsAndFamilies(t *testing.T) {
	c := testConfig(t)
	cfg := c.Sinks["raw"]
	cfg.InitialDump = true
	cfg.Match.Sources.Include = []string{"adj-in"}
	cfg.Match.Families.Include = []string{"bgp-ls"}
	c.Sinks["raw"] = cfg
	c.Sinks["deltas"] = config.Sink{Cluster: "primary", Topic: "deltas"}
	p := &fakeProducer{deliveries: make(chan fakeDelivery, 20), autoAck: true}
	m, src := testManager(t, c, p)
	defer closeManager(t, m)
	require.Equal(t, []string{"adj-in"}, src.options.InitialSources)
	require.Equal(t, []string{"ls"}, src.options.InitialFamilies)
	require.Equal(t, event.Sources, src.options.Sources)
}

func TestNativeBootstrapProducesEmptySnapshotMarkers(t *testing.T) {
	c := testConfig(t)
	cfg := c.Sinks["raw"]
	cfg.InitialDump = true
	cfg.Match.Sources.Include = []string{"adj-in"}
	cfg.Match.Families.Include = []string{"bgp-ls"}
	c.Sinks["raw"] = cfg
	p := &fakeProducer{deliveries: make(chan fakeDelivery, 20), autoAck: true}
	m, err := newManager(c, nil, nil, func(string, config.Cluster) (producer, error) { return p, nil })
	require.NoError(t, err)
	bgpServer := server.NewBgpServer()
	go bgpServer.Serve()
	defer bgpServer.Stop()
	require.NoError(t, bgpServer.StartBgp(context.Background(), &api.StartBgpRequest{Global: &api.Global{Asn: 65000, RouterId: "192.0.2.10", ListenPort: -1}}))
	require.NoError(t, m.Start(bgpServer))
	defer closeManager(t, m)
	begin, end := delivery(t, p), delivery(t, p)
	require.Equal(t, "begin", header(begin.record, "snapshot-phase"))
	require.Equal(t, "end", header(end.record, "snapshot-phase"))
	require.Equal(t, "0", header(end.record, "snapshot-records"))
	require.Equal(t, header(begin.record, "snapshot-id"), header(end.record, "snapshot-id"))
	require.JSONEq(t, `{}`, string(begin.record.Value))
	require.JSONEq(t, `{}`, string(end.record.Value))
	waitEmpty(t, m)
}
