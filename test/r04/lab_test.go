package r04

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http/httptest"
	"net/netip"
	"os"
	"os/exec"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/osrg/gobgp/v4/api"
	"github.com/osrg/gobgp/v4/pkg/apiutil"
	config "github.com/osrg/gobgp/v4/pkg/config/kafka"
	"github.com/osrg/gobgp/v4/pkg/event"
	"github.com/osrg/gobgp/v4/pkg/packet/bgp"
	"github.com/osrg/gobgp/v4/pkg/server"
	"github.com/osrg/gobgp/v4/pkg/sink/kafka"
	"github.com/osrg/gobgp/v4/pkg/sink/kafka/reference"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

type gatedSource struct {
	event.Source
	hold    atomic.Bool
	release chan struct{}
}

func (s *gatedSource) SubscribeEvents(o event.SubscriptionOptions, fn func(event.Observation)) (event.Subscription, error) {
	return s.Source.SubscribeEvents(o, func(v event.Observation) {
		if s.hold.Load() {
			<-s.release
		}
		fn(v)
	})
}

func TestR04ReflectorHotRecovery(t *testing.T) {
	if os.Getenv("NGOBGP_R04_LAB") != "1" {
		t.Skip("run the isolated cloud/CI Docker laboratory")
	}
	started := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), 240*time.Second)
	defer cancel()
	root := os.Getenv("LAB_DIR")
	if root == "" {
		root = "/lab"
	}
	rr := "10.231.84.2"
	topic := "r04-" + uuid.NewString()
	client, err := kgo.NewClient(kgo.SeedBrokers("10.231.84.3:9092"), kgo.ConsumeTopics(topic), kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
	require.NoError(t, err)
	defer client.Close()
	require.Eventually(t, func() bool { return client.Ping(ctx) == nil }, 30*time.Second, 100*time.Millisecond)
	create := kmsg.NewPtrCreateTopicsRequest()
	create.Topics = []kmsg.CreateTopicsRequestTopic{{Topic: topic, NumPartitions: 3, ReplicationFactor: 1}}
	result, err := create.RequestWith(ctx, client)
	require.NoError(t, err)
	require.Zero(t, result.Topics[0].ErrorCode)
	bgpServer := server.NewBgpServer()
	go bgpServer.Serve()
	defer bgpServer.Stop()
	require.NoError(t, bgpServer.StartBgp(ctx, &api.StartBgpRequest{Global: &api.Global{Asn: 65000, RouterId: "10.231.84.4", ListenPort: 179, ListenAddresses: []string{"0.0.0.0"}}}))
	peer := &api.Peer{Conf: &api.PeerConf{NeighborAddress: rr, PeerAsn: 65000}, Transport: &api.Transport{PassiveMode: true}}
	for _, afi := range []api.Family_Afi{api.Family_AFI_IP, api.Family_AFI_IP6} {
		peer.AfiSafis = append(peer.AfiSafis, &api.AfiSafi{Config: &api.AfiSafiConfig{Family: &api.Family{Afi: afi, Safi: api.Family_SAFI_UNICAST}, Enabled: true}})
	}
	require.NoError(t, bgpServer.AddPeer(ctx, &api.AddPeerRequest{Peer: peer}))
	source := &gatedSource{Source: bgpServer, release: make(chan struct{})}
	source.hold.Store(true)
	released := false
	defer func() {
		if !released {
			close(source.release)
		}
	}()
	manager, err := kafka.New(config.Config{Enabled: true, SpoolDir: t.TempDir(), IngressQueueSize: 1, Clusters: map[string]config.Cluster{"lab": {Brokers: []string{"10.231.84.3:9092"}, Producer: config.Producer{DeliveryTimeout: 2 * time.Second}}}, Sinks: map[string]config.Sink{"raw": {Cluster: "lab", Topic: topic, Key: config.Key{Strategy: "none"}, Queue: config.Queue{Size: 8192}, Spool: config.Spool{MaxBytes: 64 << 20}, Match: event.MatchConfig{Sources: event.Filter[string]{Include: []string{"adj-in"}}}}}}, nil, prometheus.NewRegistry())
	require.NoError(t, err)
	require.NoError(t, manager.Start(source))
	defer func() {
		if !released {
			source.hold.Store(false)
			close(source.release)
			released = true
		}
		closeCtx, stop := context.WithTimeout(context.Background(), 10*time.Second)
		defer stop()
		require.NoError(t, manager.Close(closeCtx))
	}()
	httpServer := httptest.NewServer(manager.Handler())
	defer httpServer.Close()
	control := reference.ControlClient{URL: httpServer.URL}
	state := reference.New("raw")
	checkPeer := func() bool {
		established := false
		_ = bgpServer.ListPeer(ctx, &api.ListPeerRequest{Address: rr}, func(p *api.Peer) { established = p.State.SessionState == api.PeerState_SESSION_STATE_ESTABLISHED })
		return established
	}
	require.Eventually(t, checkPeer, 20*time.Second, 100*time.Millisecond)
	peerEpoch := func() (int64, uint32) {
		var uptime int64
		var flops uint32
		_ = bgpServer.ListPeer(ctx, &api.ListPeerRequest{Address: rr}, func(p *api.Peer) { uptime = p.Timers.State.Uptime.GetSeconds(); flops = p.State.Flops })
		return uptime, flops
	}
	initialUptime, initialFlops := peerEpoch()
	require.Eventually(t, func() bool {
		_, a := os.Stat(filepath.Join(root, "generator-1.ready"))
		_, b := os.Stat(filepath.Join(root, "generator-2.ready"))
		return a == nil && b == nil
	}, 20*time.Second, 100*time.Millisecond)
	paths := func() ([]*api.Path, error) {
		var out []*api.Path
		var conversionErr error
		for _, f := range []bgp.Family{bgp.RF_IPv4_UC, bgp.RF_IPv6_UC} {
			err := bgpServer.ListPath(apiutil.ListPathRequest{TableType: api.TableType_TABLE_TYPE_ADJ_IN, Name: rr, Family: f}, func(_ bgp.NLRI, list []*apiutil.Path) {
				for _, p := range list {
					v, e := apiutil.NewPath(p.Family, p.Nlri, p.Withdrawal, p.Attrs, time.Unix(p.Age, 0))
					if e != nil {
						conversionErr = e
						continue
					}
					v.NeighborIp = apiutil.AddrOrEmpty(p.PeerAddress)
					v.SourceId = apiutil.AddrOrEmpty(p.PeerID)
					v.SourceAsn = p.PeerASN
					v.Identifier = p.RemoteID
					out = append(out, v)
				}
			})
			if err != nil {
				return nil, err
			}
		}
		return out, conversionErr
	}
	expected := func(churn bool) map[string]uint8 {
		out := make(map[string]uint8)
		first, last := 0, 1250
		if churn {
			first, last = 100, 1350
		}
		for peer := 1; peer <= 2; peer++ {
			for i := first; i < last; i++ {
				origin := uint8(0)
				if churn && i < 200 {
					origin = 1
				}
				out[fmt.Sprintf("10.%d.%d.%d/32", peer, i>>8, i&255)] = origin
				out[netip.MustParsePrefix(fmt.Sprintf("fd10:%d::%x/128", peer, i)).String()] = origin
			}
		}
		return out
	}
	settled := func(churn bool) bool {
		list, err := paths()
		if err != nil || len(list) != 5000 {
			return false
		}
		want := expected(churn)
		for _, p := range list {
			n, err := apiutil.GetNativeNlri(p)
			if err != nil {
				return false
			}
			origin, ok := want[n.String()]
			if !ok {
				return false
			}
			attrs, err := apiutil.GetNativePathAttributes(p)
			if err != nil {
				return false
			}
			found := false
			for _, a := range attrs {
				if o, ok := a.(*bgp.PathAttributeOrigin); ok {
					found = o.Value == origin
				}
			}
			if !found {
				return false
			}
			delete(want, n.String())
		}
		return len(want) == 0
	}
	phase := func(name string) { require.NoError(t, os.WriteFile(filepath.Join(root, "phase"), []byte(name), 0o644)) }
	phase("load")
	require.Eventually(t, func() bool { return settled(false) }, 35*time.Second, 100*time.Millisecond)
	require.Eventually(t, func() bool { return manager.CaptureStatus().Sinks[0].State == "invalid" }, 3*time.Second, time.Millisecond)
	first := manager.CaptureStatus()
	require.Equal(t, "invalid", first.Sinks[0].State)
	require.Contains(t, first.Sinks[0].LossReasons, "ingress-overflow")
	require.True(t, checkPeer(), "BGP must stay established during observation overflow")
	source.hold.Store(false)
	close(source.release)
	released = true
	fresh, err := control.Resync(ctx, first.Generation)
	require.NoError(t, err)
	require.NoError(t, state.ObserveStatus(fresh))
	partitions := map[int32]bool{}
	converge := func(timeout time.Duration) {
		deadline := time.Now().Add(timeout)
		for time.Now().Before(deadline) {
			poll, stop := context.WithTimeout(ctx, 200*time.Millisecond)
			fetches := client.PollRecords(poll, 2048)
			stop()
			for _, r := range fetches.Records() {
				partitions[r.Partition] = true
			}
			if err := control.IngestBatch(ctx, state, fetches.Records()); err != nil {
				t.Logf("reference invalidation: %v", err)
			}
			report, err := control.Status(ctx)
			require.NoError(t, err)
			require.NoError(t, state.ObserveStatus(report))
			if state.Ready() {
				list, err := paths()
				require.NoError(t, err)
				digest, err := reference.DigestPaths(list)
				require.NoError(t, err)
				if state.RouteCount() == 5000 && state.Digest() == digest {
					return
				}
			}
		}
		t.Fatalf("consumer did not converge: routes=%d applied=%d status=%+v", state.RouteCount(), state.Applied(), manager.CaptureStatus())
	}
	converge(90 * time.Second)
	initialDigest := state.Digest()
	binary := os.Getenv("NGOBGP_R04_REFERENCE")
	require.NotEmpty(t, binary)
	cliContext, stopCLI := context.WithCancel(ctx)
	cliLog, err := os.Create(filepath.Join(root, "reference.log"))
	require.NoError(t, err)
	defer cliLog.Close()
	command := exec.CommandContext(cliContext, binary, "--brokers=10.231.84.3:9092", "--topic="+topic, "--sink=raw", "--control="+httpServer.URL, "--gap-timeout=30s")
	command.Stdout = cliLog
	command.Stderr = cliLog
	require.NoError(t, command.Start())
	defer func() { stopCLI(); _ = command.Wait() }()
	cliConverged := func() bool {
		data, err := os.ReadFile(filepath.Join(root, "reference.log"))
		if err != nil {
			return false
		}
		for _, line := range bytes.Split(data, []byte{'\n'}) {
			var report struct {
				Generation string `json:"generation"`
				Consistent bool   `json:"consistent"`
				Routes     int    `json:"routes"`
				Digest     string `json:"digest"`
			}
			if json.Unmarshal(line, &report) == nil && report.Generation == state.Generation() && report.Consistent && report.Routes == 5000 && report.Digest == state.Digest() {
				return true
			}
		}
		return false
	}
	require.Eventually(t, cliConverged, 20*time.Second, 100*time.Millisecond)
	beforeOutage := state.Generation()
	t.Logf("initial convergence after %s: %+v", time.Since(started), manager.CaptureStatus())
	require.NoError(t, os.WriteFile(filepath.Join(root, "broker-stop.request"), []byte("stop"), 0o644))
	require.Eventually(t, func() bool { _, err := os.Stat(filepath.Join(root, "broker-stop.done")); return err == nil }, 20*time.Second, 100*time.Millisecond)
	phase("churn")
	require.Eventually(t, func() bool { return settled(true) }, 30*time.Second, 100*time.Millisecond)
	lost, err := control.Status(ctx)
	require.NoError(t, err)
	require.NoError(t, state.ObserveStatus(lost))
	require.False(t, state.Ready(), "consumer must not claim its old state still matches the live RIB")
	require.True(t, checkPeer(), "broker outage must not reset BGP")
	require.NoError(t, os.WriteFile(filepath.Join(root, "broker-start.request"), []byte("start"), 0o644))
	require.Eventually(t, func() bool { _, err := os.Stat(filepath.Join(root, "broker-start.done")); return err == nil }, 20*time.Second, 100*time.Millisecond)
	t.Logf("broker restarted after %s: %+v", time.Since(started), manager.CaptureStatus())
	beforeRecovery := beforeOutage
	require.Eventually(t, func() bool { report, e := control.Status(ctx); return e == nil && report.Generation != beforeRecovery }, 25*time.Second, 100*time.Millisecond)
	report, err := control.Status(ctx)
	require.NoError(t, err)
	require.NoError(t, state.ObserveStatus(report))
	converge(90 * time.Second)
	require.Eventually(t, cliConverged, 20*time.Second, 100*time.Millisecond)
	require.NotEqual(t, initialDigest, state.Digest(), "churn changes both prefixes and attributes")
	require.GreaterOrEqual(t, len(partitions), 2, "exercise consumption across partitions")
	require.True(t, checkPeer())
	finalUptime, finalFlops := peerEpoch()
	require.Equal(t, initialUptime, finalUptime)
	require.Equal(t, initialFlops, finalFlops)
	for i := 1; i <= 2; i++ {
		_, err := os.Stat(filepath.Join(root, fmt.Sprintf("generator-%d.error", i)))
		require.True(t, os.IsNotExist(err), "generator session must remain healthy")
	}
	evidence := map[string]any{"result": "PASS", "routes": 5000, "peers": 2, "families": []string{"ipv4", "ipv6"}, "partitions_observed": len(partitions), "initial_loss": first, "final_capture": manager.CaptureStatus(), "consumer_generation": state.Generation(), "consumer_applied": state.Applied(), "initial_digest": initialDigest, "final_digest": state.Digest(), "bgp_established": true, "reference_cli_verified": true, "peer_uptime_unchanged": true, "peer_flops_unchanged": true}
	data, err := json.MarshalIndent(evidence, "", "  ")
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(root, "result.json"), data, 0o644))
}
