package server

import (
	"context"
	"fmt"
	"net/netip"
	"sync/atomic"
	"testing"
	"time"

	"github.com/osrg/gobgp/v4/api"
	"github.com/osrg/gobgp/v4/internal/pkg/table"
	"github.com/osrg/gobgp/v4/pkg/apiutil"
	"github.com/osrg/gobgp/v4/pkg/config/oc"
	"github.com/osrg/gobgp/v4/pkg/event"
	"github.com/osrg/gobgp/v4/pkg/packet/bgp"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

func testEventPath(t *testing.T, family bgp.Family) *table.Path {
	t.Helper()
	prefix := netip.MustParsePrefix("192.0.2.0/24")
	if family == bgp.RF_IPv6_UC {
		prefix = netip.MustParsePrefix("2001:db8::/48")
	}
	nlri, err := bgp.NewIPAddrPrefix(prefix)
	require.NoError(t, err)
	return table.NewPath(family, &table.PeerInfo{AS: 65001, Address: netip.MustParseAddr("192.0.2.1")}, bgp.PathNLRI{NLRI: nlri}, false, []bgp.PathAttributeInterface{bgp.NewPathAttributeOrigin(0)}, time.Now(), false)
}

func TestBoundedEventSubscriptionDoesNotBlockNotifications(t *testing.T) {
	s := NewBgpServer()
	go s.Serve()
	defer func() { s.runningCancel(); s.shutdownWG.Wait() }()
	entered, release := make(chan struct{}), make(chan struct{})
	var once atomic.Bool
	var dropped atomic.Int64
	sub, err := s.SubscribeEvents(event.SubscriptionOptions{Sources: []string{"adj-in"}, Capacity: 2, OnDrop: func() { dropped.Add(1) }}, func(event.Observation) {
		if once.CompareAndSwap(false, true) {
			close(entered)
			<-release
		}
	})
	require.NoError(t, err)
	path := testEventPath(t, bgp.RF_IPv4_UC)
	s.notifyWatcher(watchEventTypePreUpdate, &watchEventUpdate{PathList: []*table.Path{path}})
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("conversion callback did not start")
	}
	completed := make(chan struct{})
	go func() {
		for range 100 {
			s.notifyWatcher(watchEventTypePreUpdate, &watchEventUpdate{PathList: []*table.Path{path}})
		}
		close(completed)
	}()
	select {
	case <-completed:
	case <-time.After(time.Second):
		close(release)
		t.Fatal("notification blocked on slow consumer")
	}
	require.Equal(t, int64(98), dropped.Load())
	require.Equal(t, 2, sub.QueueSize())
	sub.Stop()
	close(release)
	select {
	case <-sub.Done():
	case <-time.After(time.Second):
		t.Fatal("subscription did not drain")
	}
}

func TestEventObservationClassification(t *testing.T) {
	p4 := testEventPath(t, bgp.RF_IPv4_UC)
	p6 := testEventPath(t, bgp.RF_IPv6_UC)
	ls := table.NewEOR(bgp.RF_LS)
	ls.SetSource(p4.GetSource())
	now := time.Now()
	cases := []struct {
		native watchEvent
		types  []string
		source string
	}{
		{&watchEventUpdate{PathList: []*table.Path{p4, p6.Clone(true)}, Timestamp: now}, []string{"route", "withdraw"}, "adj-in"},
		{&watchEventUpdate{PathList: []*table.Path{p4}, PostPolicy: true, Timestamp: now}, []string{"route"}, "post-policy"},
		{&watchEventBestPath{UpdatePathList: []*table.Path{p6}, WithdrawPathList: []*table.Path{p4.Clone(true)}, Timestamp: now}, []string{"withdraw", "route"}, "best"},
		{&watchEventEor{Family: bgp.RF_LS, PeerInfo: ls.GetSource(), Timestamp: now}, []string{"eor"}, "adj-in"},
		{&watchEventPeer{Type: apiutil.PEER_EVENT_STATE, PeerAS: 65001, PeerAddress: netip.MustParseAddr("192.0.2.1"), State: bgp.BGP_FSM_ESTABLISHED, Timestamp: now}, []string{"peer-state"}, "peer"},
	}
	for _, tc := range cases {
		var got []event.Observation
		observationsFromWatch(tc.native, func(v event.Observation) { got = append(got, v) })
		require.Len(t, got, len(tc.types))
		for i, v := range got {
			require.NoError(t, v.Err)
			require.Equal(t, tc.types[i], v.Metadata.EventType)
			require.Equal(t, tc.source, v.Metadata.Source)
			require.Equal(t, now, v.Metadata.ObservedAt)
			require.Equal(t, uint32(65001), v.Metadata.PeerASN)
			require.NotNil(t, v.Message)
			if v.Message.GetTable() != nil {
				require.Len(t, v.Message.GetTable().Paths, 1)
			}
		}
	}
	observationsFromWatch(&watchEventPeer{Type: apiutil.PEER_EVENT_INIT}, func(event.Observation) { t.Fatal("unexpected snapshot") })
}

func TestExistingWatchEventWithBoundedSubscriber(t *testing.T) {
	s := NewBgpServer()
	go s.Serve()
	defer func() { s.runningCancel(); s.shutdownWG.Wait() }()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	got := make(chan struct{}, 1)
	require.NoError(t, s.WatchEvent(ctx, WatchEventMessageCallbacks{OnPathUpdate: func([]*apiutil.Path, time.Time) { got <- struct{}{} }}, WatchUpdate(false, "", "")))
	sub, err := s.SubscribeEvents(event.SubscriptionOptions{Sources: []string{"adj-in"}, Capacity: 1}, func(event.Observation) {})
	require.NoError(t, err)
	defer sub.Stop()
	s.notifyWatcher(watchEventTypePreUpdate, &watchEventUpdate{PathList: []*table.Path{testEventPath(t, bgp.RF_IPv4_UC)}})
	select {
	case <-got:
	case <-time.After(time.Second):
		t.Fatal("existing WatchEvent did not receive event")
	}
}

func TestEventObservationPreservesBGPLinkState(t *testing.T) {
	node := &api.LsNodeDescriptor{Asn: 65001, IgpRouterId: "192.0.2.1"}
	nlri := &api.NLRI{Nlri: &api.NLRI_LsAddrPrefix{LsAddrPrefix: &api.LsAddrPrefix{ProtocolId: api.LsProtocolID_LS_PROTOCOL_ID_ISIS_L2, Nlri: &api.LsAddrPrefix_LsNLRI{Nlri: &api.LsAddrPrefix_LsNLRI_Node{Node: &api.LsNodeNLRI{LocalNode: node}}}}}}
	native, err := apiutil.UnmarshalNLRI(bgp.RF_LS, nlri)
	require.NoError(t, err)
	name := "router-one"
	attrs := []bgp.PathAttributeInterface{bgp.NewPathAttributeOrigin(0), &bgp.PathAttributeLs{TLVs: []bgp.LsTLVInterface{bgp.NewLsTLVNodeName(&name)}}}
	p := table.NewPath(bgp.RF_LS, &table.PeerInfo{AS: 65001, Address: netip.MustParseAddr("192.0.2.1")}, bgp.PathNLRI{NLRI: native}, false, attrs, time.Now(), false)
	observationsFromWatch(&watchEventUpdate{PathList: []*table.Path{p}}, func(v event.Observation) {
		require.NoError(t, v.Err)
		require.Equal(t, "ls", v.Metadata.Family)
		expectedNLRI, err := apiutil.MarshalNLRI(native)
		require.NoError(t, err)
		expectedAttrs, err := apiutil.MarshalPathAttributes(attrs)
		require.NoError(t, err)
		path := v.Message.GetTable().Paths[0]
		require.True(t, proto.Equal(expectedNLRI, path.Nlri))
		require.Len(t, path.Pattrs, len(expectedAttrs))
		for i, a := range expectedAttrs {
			require.True(t, proto.Equal(a, path.Pattrs[i]))
		}
	})
}

func TestInitialDumpCapturesBGPLinkStateBeforeDeltas(t *testing.T) {
	s := NewBgpServer()
	go s.Serve()
	defer func() { s.runningCancel(); s.shutdownWG.Wait() }()
	require.NoError(t, s.StartBgp(context.Background(), &api.StartBgpRequest{Global: &api.Global{Asn: 65001, RouterId: "192.0.2.10", ListenPort: -1}}))
	addr := netip.MustParseAddr("192.0.2.1")
	p := newPeerandInfo(t, 65001, 65002, addr.String(), s.globalRib)
	p.fsm.state.Store(bgp.BGP_FSM_ESTABLISHED)
	open, err := bgp.NewBGPOpenMessage(65002, 90, addr, nil)
	require.NoError(t, err)
	p.fsm.recvOpen = open
	p.adjRibIn = table.NewAdjRib(logger, []bgp.Family{bgp.RF_LS, bgp.RF_IPv4_UC})
	conf := p.fsm.pConf.ReadCopy()
	conf.AfiSafis = []oc.AfiSafi{{Config: oc.AfiSafiConfig{AfiSafiName: oc.AFI_SAFI_TYPE_LS, Enabled: true}, State: oc.AfiSafiState{Family: bgp.RF_LS}}, {Config: oc.AfiSafiConfig{AfiSafiName: oc.AFI_SAFI_TYPE_IPV4_UNICAST, Enabled: true}, State: oc.AfiSafiState{Family: bgp.RF_IPv4_UC}}}
	p.fsm.pConf.Update(&conf)
	paths := []*table.Path{}
	for i := range 100 {
		nlri := &api.NLRI{Nlri: &api.NLRI_LsAddrPrefix{LsAddrPrefix: &api.LsAddrPrefix{ProtocolId: api.LsProtocolID_LS_PROTOCOL_ID_ISIS_L2, Nlri: &api.LsAddrPrefix_LsNLRI{Nlri: &api.LsAddrPrefix_LsNLRI_Node{Node: &api.LsNodeNLRI{LocalNode: &api.LsNodeDescriptor{Asn: 65002, IgpRouterId: fmt.Sprintf("0000.0000.%04d", i)}}}}}}}
		native, err := apiutil.UnmarshalNLRI(bgp.RF_LS, nlri)
		require.NoError(t, err)
		paths = append(paths, table.NewPath(bgp.RF_LS, p.peerInfo.Load(), bgp.PathNLRI{NLRI: native}, false, []bgp.PathAttributeInterface{bgp.NewPathAttributeOrigin(0)}, time.Now(), false))
	}
	require.NoError(t, s.mgmtOperation(func() error {
		s.neighborMap[addr] = p
		p.adjRibIn.Update(paths)
		v4 := testEventPath(t, bgp.RF_IPv4_UC)
		p.adjRibIn.Update([]*table.Path{v4})
		s.globalRib.Update(v4)
		for _, path := range paths {
			s.globalRib.Update(path)
		}
		return nil
	}, true))
	defer func() {
		require.NoError(t, s.mgmtOperation(func() error { delete(s.neighborMap, addr); return nil }, false))
		cleanInfiniteChannel(p.fsm.outgoingCh)
		require.NoError(t, s.StopBgp(context.Background(), &api.StopBgpRequest{}))
	}()
	entered, release := make(chan struct{}), make(chan struct{})
	observations := make(chan event.Observation, 400)
	var drops atomic.Int64
	sub, err := s.SubscribeEvents(event.SubscriptionOptions{InitialDump: true, InitialSources: []string{"adj-in", "post-policy", "best"}, InitialFamilies: []string{"ls"}, Sources: []string{"adj-in", "post-policy", "best", "peer"}, Capacity: 1, OnDrop: func() { drops.Add(1) }}, func(v event.Observation) {
		if v.Metadata.SnapshotPhase == "begin" {
			close(entered)
			<-release
		}
		observations <- v
	})
	require.NoError(t, err)
	<-entered
	// Delta is admitted while the snapshot callback waits, with no BGP-loop wait.
	require.NoError(t, s.mgmtOperation(func() error {
		withdrawal := paths[0].Clone(true)
		p.adjRibIn.Update([]*table.Path{withdrawal})
		s.globalRib.Update(withdrawal)
		s.notifyWatcher(watchEventTypePreUpdate, &watchEventUpdate{PathList: []*table.Path{withdrawal}, Timestamp: time.Now()})
		return nil
	}, true))
	close(release)
	sub.Stop()
	select {
	case <-sub.Done():
	case <-time.After(3 * time.Second):
		t.Fatal("snapshot failed to drain")
	}
	close(observations)
	counts := map[string]int{}
	completed := false
	for v := range observations {
		require.NoError(t, v.Err)
		switch v.Metadata.SnapshotPhase {
		case "data":
			require.False(t, completed)
			require.Equal(t, "ls", v.Metadata.Family)
			if v.Metadata.EventType == "route" {
				counts[v.Metadata.Source]++
			}
		case "end":
			completed = true
		case "":
			require.True(t, completed)
			require.Equal(t, "withdraw", v.Metadata.EventType)
		}
	}
	require.True(t, completed)
	require.Equal(t, map[string]int{"adj-in": 100, "post-policy": 100, "best": 100}, counts)
	require.Zero(t, drops.Load())
}
