package server

import (
	"context"
	"encoding/binary"
	"fmt"
	"net/netip"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/osrg/gobgp/v4/api"
	"github.com/osrg/gobgp/v4/internal/pkg/table"
	"github.com/osrg/gobgp/v4/pkg/apiutil"
	"github.com/osrg/gobgp/v4/pkg/config/oc"
	"github.com/osrg/gobgp/v4/pkg/event"
	"github.com/osrg/gobgp/v4/pkg/packet/bgp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func refreshTestPeer(t *testing.T, s *BgpServer, address string, asn uint32, mode bgp.BGPAddPathMode, sendMax uint8) *peer {
	t.Helper()
	p := newPeerandInfo(t, 65000, asn, address, s.globalRib)
	p.policy = s.policy
	var err error
	p.fsm.recvOpen, err = bgp.NewBGPOpenMessage(uint16(asn), 90, netip.MustParseAddr(address), nil)
	require.NoError(t, err)
	p.fsm.state.Store(bgp.BGP_FSM_ESTABLISHED)
	p.fsm.familyMap.Store(map[bgp.Family]bgp.BGPAddPathMode{bgp.RF_IPv4_UC: mode, bgp.RF_IPv6_UC: mode})
	c := p.fsm.pConf.ReadCopy()
	c.AfiSafis = []oc.AfiSafi{}
	for _, f := range []bgp.Family{bgp.RF_IPv4_UC, bgp.RF_IPv6_UC} {
		af := oc.AfiSafi{Config: oc.AfiSafiConfig{AfiSafiName: oc.AfiSafiType(f.String())}, State: oc.AfiSafiState{Family: f}}
		af.AddPaths.Config.SendMax = sendMax
		c.AfiSafis = append(c.AfiSafis, af)
	}
	p.fsm.pConf.Update(&c)
	p.adjRibIn = table.NewAdjRib(logger, []bgp.Family{bgp.RF_IPv4_UC, bgp.RF_IPv6_UC})
	p.fsm.capMap = map[bgp.BGPCapabilityCode][]bgp.ParameterCapabilityInterface{bgp.BGP_CAP_ROUTE_REFRESH: {bgp.NewCapRouteRefresh()}, bgp.BGP_CAP_ENHANCED_ROUTE_REFRESH: {bgp.NewCapEnhancedRouteRefresh()}}
	require.NoError(t, s.mgmtOperation(func() error { s.neighborMap[netip.MustParseAddr(address)] = p; return nil }, true))
	t.Cleanup(func() {
		require.NoError(t, s.mgmtOperation(func() error { p.cancelRefresh(); delete(s.neighborMap, netip.MustParseAddr(address)); return nil }, false))
		cleanInfiniteChannel(p.fsm.outgoingCh)
	})
	return p
}

func refreshTestMessage(t *testing.T, pfx string, id uint32, withdraw bool) *bgp.BGPMessage {
	t.Helper()
	prefix := netip.MustParsePrefix(pfx)
	nlri, err := bgp.NewIPAddrPrefix(prefix)
	require.NoError(t, err)
	paths := []bgp.PathNLRI{{NLRI: nlri, ID: id}}
	attrs := []bgp.PathAttributeInterface{bgp.NewPathAttributeOrigin(0), bgp.NewPathAttributeAsPath([]bgp.AsPathParamInterface{bgp.NewAs4PathParam(2, []uint32{65010})})}
	if prefix.Addr().Is4() {
		if withdraw {
			return bgp.NewBGPUpdateMessage(paths, nil, nil)
		}
		nh, err := bgp.NewPathAttributeNextHop(netip.MustParseAddr("192.0.2.1"))
		require.NoError(t, err)
		return bgp.NewBGPUpdateMessage(nil, append(attrs, nh), paths)
	}
	if withdraw {
		mp, err := bgp.NewPathAttributeMpUnreachNLRI(bgp.RF_IPv6_UC, paths)
		require.NoError(t, err)
		return bgp.NewBGPUpdateMessage(nil, []bgp.PathAttributeInterface{mp}, nil)
	}
	mp, err := bgp.NewPathAttributeMpReachNLRI(bgp.RF_IPv6_UC, paths, netip.MustParseAddr("2001:db8::1"))
	require.NoError(t, err)
	return bgp.NewBGPUpdateMessage(nil, append(attrs, mp), nil)
}

func refreshReceive(s *BgpServer, p *peer, m *bgp.BGPMessage) {
	s.handleFSMMessage(p, &fsmMsg{MsgType: fsmMsgBGPMessage, MsgData: m, timestamp: time.Now()})
}

func refreshMarker(s *BgpServer, p *peer, f bgp.Family, subtype uint8) {
	afi, safi := f.Afi(), f.Safi()
	refreshReceive(s, p, bgp.NewBGPRouteRefreshMessage(afi, subtype, safi))
}

func refreshOutgoing(t *testing.T, p *peer) []*table.Path {
	t.Helper()
	select {
	case m := <-p.fsm.outgoingCh.Out():
		return m.(*fsmOutgoingMsg).Paths
	case <-time.After(3 * time.Second):
		t.Fatal("missing refresh output")
		return nil
	}
}

func TestRefreshRespectsAddPathSendMax(t *testing.T) {
	for _, limit := range []uint8{1, 2} {
		t.Run(fmt.Sprint(limit), func(t *testing.T) {
			s := r03Server(t)
			target := refreshTestPeer(t, s, "192.0.2.20", 65020, bgp.BGP_ADD_PATH_SEND, limit)
			a := makePath(t, "203.0.113.0/24", "192.0.2.1", 0)
			a.SetSource(&table.PeerInfo{AS: 65010, Address: netip.MustParseAddr("192.0.2.10"), ID: netip.MustParseAddr("192.0.2.10")})
			b := makePath(t, "203.0.113.0/24", "192.0.2.2", 0)
			b.SetSource(&table.PeerInfo{AS: 65011, Address: netip.MustParseAddr("192.0.2.11"), ID: netip.MustParseAddr("192.0.2.11")})
			s.propagateUpdate(nil, []*table.Path{a})
			require.Len(t, refreshOutgoing(t, target), 1)
			s.propagateUpdate(nil, []*table.Path{b})
			if limit == 2 {
				require.Len(t, refreshOutgoing(t, target), 1)
			}
			for range 2 {
				refreshMarker(s, target, bgp.RF_IPv4_UC, 0)
				paths := refreshOutgoing(t, target)
				require.Len(t, paths, int(limit), "refresh must use the same outbound ceiling as live updates")
				require.Equal(t, limit, target.getRoutesCount(bgp.RF_IPv4_UC, a.GetPrefix()))
				require.Equal(t, limit == 1, target.isPathSendMaxFiltered(b))
				count, blocked := 0, 0
				require.NoError(t, s.ListPath(apiutil.ListPathRequest{TableType: api.TableType_TABLE_TYPE_ADJ_OUT, Name: target.ID(), Family: bgp.RF_IPv4_UC, EnableFiltered: true}, func(_ bgp.NLRI, paths []*apiutil.Path) {
					count += len(paths)
					for _, p := range paths {
						if p.SendMaxFiltered {
							blocked++
						}
					}
				}))
				require.Equal(t, 2, count, "Adj-RIB-Out inspection must preserve distinct local ADD-PATH IDs")
				require.Equal(t, 2-int(limit), blocked)
			}
			s.propagateUpdate(nil, []*table.Path{b.Clone(true)})
			if limit == 2 {
				require.True(t, refreshOutgoing(t, target)[0].IsWithdraw)
			}
			s.propagateUpdate(nil, []*table.Path{a.Clone(true)})
			require.True(t, refreshOutgoing(t, target)[0].IsWithdraw)
			require.Zero(t, target.advertisedRoutes([]bgp.Family{bgp.RF_IPv4_UC}))
		})
	}
}

func TestEnhancedRefreshRemovesOnlyOmittedPaths(t *testing.T) {
	s := r03Server(t)
	p := refreshTestPeer(t, s, "192.0.2.10", 65010, bgp.BGP_ADD_PATH_RECEIVE, 0)
	refreshReceive(s, p, refreshTestMessage(t, "203.0.113.0/24", 1, false))
	refreshReceive(s, p, refreshTestMessage(t, "203.0.113.0/24", 2, false))
	refreshReceive(s, p, refreshTestMessage(t, "2001:db8:10::/48", 1, false))
	refreshMarker(s, p, bgp.RF_IPv4_UC, 1)
	refreshReceive(s, p, refreshTestMessage(t, "203.0.113.0/24", 1, false))
	refreshReceive(s, p, bgp.NewEndOfRib(bgp.RF_IPv4_UC))
	require.Equal(t, 2, p.adjRibIn.Count([]bgp.Family{bgp.RF_IPv4_UC}), "EOR is not EoRR")
	refreshMarker(s, p, bgp.RF_IPv4_UC, 2)
	paths := p.adjRibIn.PathList([]bgp.Family{bgp.RF_IPv4_UC}, false)
	require.Len(t, paths, 1, "EoRR must remove the omitted path ID")
	require.Equal(t, uint32(1), paths[0].RemoteID())
	require.Equal(t, 1, p.adjRibIn.Count([]bgp.Family{bgp.RF_IPv6_UC}), "another family must survive")
	refreshMarker(s, p, bgp.RF_IPv6_UC, 2)
	require.Equal(t, 1, p.adjRibIn.Count([]bgp.Family{bgp.RF_IPv6_UC}), "unpaired EoRR must be ignored")
}

func TestGREORRemovesStalePathsPerFamily(t *testing.T) {
	s := r03Server(t)
	p := refreshTestPeer(t, s, "192.0.2.10", 65010, bgp.BGP_ADD_PATH_NONE, 0)
	for _, prefix := range []string{"203.0.113.0/24", "2001:db8:10::/48"} {
		refreshReceive(s, p, refreshTestMessage(t, prefix, 0, false))
	}
	rejected := refreshTestMessage(t, "203.0.114.0/24", 0, false)
	rejected.Body.(*bgp.BGPUpdate).PathAttributes[1] = bgp.NewPathAttributeAsPath([]bgp.AsPathParamInterface{bgp.NewAs4PathParam(2, []uint32{65000})})
	refreshReceive(s, p, rejected)
	counts := refreshEventCounts(t, s)
	counts(3, 2, 2)
	order := make(chan string, 16)
	sub, err := s.SubscribeEvents(event.SubscriptionOptions{Sources: []string{"adj-in"}, Capacity: 16}, func(v event.Observation) { order <- v.Metadata.EventType })
	require.NoError(t, err)
	t.Cleanup(func() { sub.Stop(); <-sub.Done() })
	c := p.fsm.pConf.ReadCopy()
	c.GracefulRestart.State.PeerRestarting = true
	c.GracefulRestart.State.Enabled = true
	for i := range c.AfiSafis {
		c.AfiSafis[i].MpGracefulRestart.State.Enabled = true
		c.AfiSafis[i].MpGracefulRestart.State.Received = true
	}
	p.fsm.pConf.Update(&c)
	s.propagateUpdate(p, p.StaleAll([]bgp.Family{bgp.RF_IPv4_UC, bgp.RF_IPv6_UC}))
	refreshReceive(s, p, bgp.NewEndOfRib(bgp.RF_IPv4_UC))
	require.Zero(t, p.adjRibIn.Count([]bgp.Family{bgp.RF_IPv4_UC}), "IPv4 EOR must clean IPv4 without waiting for IPv6")
	require.Equal(t, 1, p.adjRibIn.Count([]bgp.Family{bgp.RF_IPv6_UC}))
	require.True(t, p.fsm.pConf.ReadOnly().GracefulRestart.State.PeerRestarting)
	counts(1, 1, 1)
	var observed []string
	for range 3 {
		select {
		case kind := <-order:
			observed = append(observed, kind)
		case <-time.After(3 * time.Second):
			t.Fatal("missing EOR completion observation")
		}
	}
	require.Equal(t, []string{"withdraw", "withdraw", "eor"}, observed, "EOR must follow cleanup, including rejected snapshot input")
	refreshReceive(s, p, bgp.NewEndOfRib(bgp.RF_IPv6_UC))
	require.Zero(t, p.adjRibIn.Count([]bgp.Family{bgp.RF_IPv6_UC}))
	require.False(t, p.fsm.pConf.ReadOnly().GracefulRestart.State.PeerRestarting)
	counts(0, 0, 0)
}

func TestSendMessageloopPreservesRefreshBoundaries(t *testing.T) {
	m := NewMockConnection()
	p, h := makePeerAndHandler(m)
	t.Cleanup(func() { cleanPeerAndHandler(p, h) })
	path := func(prefix string) *table.Path { return makePath(t, prefix, "192.0.2.1", 0) }
	for _, item := range []*fsmOutgoingMsg{
		{Paths: []*table.Path{path("203.0.110.0/24")}},
		{Paths: []*table.Path{path("203.0.111.0/24")}, RefreshFamily: bgp.RF_IPv4_UC},
		{Paths: []*table.Path{path("203.0.112.0/24")}},
		{RefreshFamily: bgp.RF_IPv6_UC},
		{Paths: []*table.Path{path("203.0.113.0/24")}},
	} {
		h.outgoing.In() <- item
	}
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	wg.Add(1)
	go h.sendMessageloop(ctx, m, make(chan fsmStateReason, 3), &wg)
	t.Cleanup(func() { cancel(); wg.Wait() })
	require.Eventually(t, func() bool { return len(parseBGPUpdates(t, m.GetSentMessages())) >= 8 }, 3*time.Second, time.Millisecond)
	cancel()
	wg.Wait()
	messages := parseBGPUpdates(t, m.GetSentMessages())
	require.Len(t, messages, 8)
	var observed []string
	for _, msg := range messages {
		switch body := msg.Body.(type) {
		case *bgp.BGPRouteRefresh:
			observed = append(observed, fmt.Sprintf("%d:%d", body.AFI, body.Demarcation))
		case *bgp.BGPUpdate:
			require.Len(t, body.NLRI, 1)
			observed = append(observed, body.NLRI[0].NLRI.String())
		}
	}
	require.Equal(t, []string{"203.0.110.0/24", "1:1", "203.0.111.0/24", "1:2", "203.0.112.0/24", "2:1", "2:2", "203.0.113.0/24"}, observed)
}

func TestEnhancedRefreshMarkerLength(t *testing.T) {
	for _, subtype := range []uint8{1, 2} {
		t.Run(fmt.Sprint(subtype), func(t *testing.T) {
			m := NewMockConnection()
			p, h := makePeerAndHandler(m)
			t.Cleanup(func() { cleanPeerAndHandler(p, h) })
			p.fsm.capMap = map[bgp.BGPCapabilityCode][]bgp.ParameterCapabilityInterface{bgp.BGP_CAP_ENHANCED_ROUTE_REFRESH: {bgp.NewCapEnhancedRouteRefresh()}}
			raw, err := bgp.NewBGPRouteRefreshMessage(1, subtype, 1).Serialize()
			require.NoError(t, err)
			raw = append(raw, 0)
			binary.BigEndian.PutUint16(raw[16:18], uint16(len(raw)))
			done := make(chan struct{})
			go func() { defer close(done); _, _ = m.remote.Write(raw) }()
			result, err := h.recvMessageWithError(m, make(chan fsmStateReason, 1))
			require.Error(t, err)
			require.Equal(t, bgp.ERROR_HANDLING_SESSION_RESET, result.handling)
			var messageError *bgp.MessageError
			require.ErrorAs(t, err, &messageError)
			require.Equal(t, uint8(bgp.BGP_ERROR_ROUTE_REFRESH_MESSAGE_ERROR), messageError.TypeCode)
			require.Equal(t, uint8(bgp.BGP_ERROR_SUB_INVALID_MESSAGE_LENGTH), messageError.SubTypeCode)
			require.Equal(t, raw, messageError.Data)
			<-done
		})
	}
}

func TestEnhancedRefreshRequiresCapabilityAndInitialEOR(t *testing.T) {
	s := r03Server(t)
	p := refreshTestPeer(t, s, "192.0.2.10", 65010, bgp.BGP_ADD_PATH_NONE, 0)
	refreshReceive(s, p, refreshTestMessage(t, "203.0.113.0/24", 0, false))
	delete(p.fsm.capMap, bgp.BGP_CAP_ENHANCED_ROUTE_REFRESH)
	refreshMarker(s, p, bgp.RF_IPv4_UC, 1)
	refreshMarker(s, p, bgp.RF_IPv4_UC, 2)
	require.Equal(t, 1, p.adjRibIn.Count([]bgp.Family{bgp.RF_IPv4_UC}))
	require.Empty(t, p.refreshPending)
	p.fsm.capMap[bgp.BGP_CAP_ENHANCED_ROUTE_REFRESH] = []bgp.ParameterCapabilityInterface{bgp.NewCapEnhancedRouteRefresh()}
	p.fsm.capMap[bgp.BGP_CAP_GRACEFUL_RESTART] = []bgp.ParameterCapabilityInterface{bgp.NewCapGracefulRestart(false, false, 60, nil)}
	refreshMarker(s, p, bgp.RF_IPv4_UC, 1)
	refreshMarker(s, p, bgp.RF_IPv4_UC, 2)
	require.Empty(t, p.refreshPending, "BoRR before initial EOR must be ignored")
	refreshReceive(s, p, bgp.NewEndOfRib(bgp.RF_IPv4_UC))
	refreshMarker(s, p, bgp.RF_IPv4_UC, 1)
	refreshMarker(s, p, bgp.RF_IPv4_UC, 2)
	require.Zero(t, p.adjRibIn.Count([]bgp.Family{bgp.RF_IPv4_UC}))
}

// Reconstruct each event source independently, as a collector would.
func refreshEventCounts(t *testing.T, s *BgpServer) func(int, int, int) {
	t.Helper()
	var mu sync.Mutex
	var failed atomic.Bool
	captured := map[string]map[string]bool{"adj-in": {}, "post-policy": {}, "best": {}}
	sub, err := s.SubscribeEvents(event.SubscriptionOptions{Sources: []string{"adj-in", "post-policy", "best"}, InitialDump: true, Capacity: 128, OnDrop: func() { failed.Store(true) }}, func(v event.Observation) {
		if v.Err != nil {
			failed.Store(true)
			return
		}
		if v.Metadata.EventType == "eor" {
			return
		}
		mu.Lock()
		defer mu.Unlock()
		for _, p := range v.Message.GetTable().GetPaths() {
			nlri, err := apiutil.GetNativeNlri(p)
			if err != nil {
				failed.Store(true)
				continue
			}
			key := fmt.Sprintf("%s:%s:%d", v.Metadata.PeerAddress, nlri.String(), p.Identifier)
			if v.Metadata.Source == "best" {
				key = nlri.String()
			}
			if p.IsWithdraw {
				delete(captured[v.Metadata.Source], key)
			} else {
				captured[v.Metadata.Source][key] = true
			}
		}
	})
	require.NoError(t, err)
	t.Cleanup(func() { sub.Stop(); <-sub.Done(); require.False(t, failed.Load()) })
	return func(adj, post, best int) {
		t.Helper()
		require.EventuallyWithT(t, func(c *assert.CollectT) {
			mu.Lock()
			defer mu.Unlock()
			assert.Equal(c, adj, len(captured["adj-in"]), "adj-in: %v", captured["adj-in"])
			assert.Equal(c, post, len(captured["post-policy"]), "post-policy: %v", captured["post-policy"])
			assert.Equal(c, best, len(captured["best"]), "best: %v", captured["best"])
		}, 3*time.Second, time.Millisecond)
	}
}

func TestEnhancedRefreshConvergesEventsAndRemovesRejectedInput(t *testing.T) {
	s := r03Server(t)
	p := refreshTestPeer(t, s, "192.0.2.10", 65010, bgp.BGP_ADD_PATH_RECEIVE, 0)
	other := refreshTestPeer(t, s, "192.0.2.11", 65011, bgp.BGP_ADD_PATH_RECEIVE, 0)
	refreshReceive(s, p, refreshTestMessage(t, "203.0.113.0/24", 1, false))
	refreshReceive(s, p, refreshTestMessage(t, "203.0.113.0/24", 2, false))
	refreshReceive(s, p, refreshTestMessage(t, "2001:db8:10::/48", 1, false))
	refreshReceive(s, other, refreshTestMessage(t, "198.51.100.0/24", 1, false))
	rejected := refreshTestMessage(t, "203.0.114.0/24", 1, false)
	rejected.Body.(*bgp.BGPUpdate).PathAttributes[1] = bgp.NewPathAttributeAsPath([]bgp.AsPathParamInterface{bgp.NewAs4PathParam(2, []uint32{65010, 65000})})
	refreshReceive(s, p, rejected)
	counts := refreshEventCounts(t, s)
	counts(5, 4, 3)
	refreshMarker(s, p, bgp.RF_IPv4_UC, 1)
	refreshReceive(s, p, refreshTestMessage(t, "203.0.113.0/24", 1, false))
	// A management replay is not a refresh advertisement from the peer.
	require.NoError(t, s.mgmtOperation(func() error { return s.softResetIn(p.ID(), bgp.RF_IPv4_UC) }, true))
	refreshMarker(s, p, bgp.RF_IPv4_UC, 2)
	counts(3, 3, 3)
	require.Equal(t, 1, p.adjRibIn.Count([]bgp.Family{bgp.RF_IPv4_UC}))
	require.Equal(t, 1, other.adjRibIn.Count([]bgp.Family{bgp.RF_IPv4_UC}))
	refreshMarker(s, p, bgp.RF_IPv4_UC, 1)
	refreshMarker(s, p, bgp.RF_IPv4_UC, 2)
	counts(2, 2, 2)
}

func TestRefreshReconcilesChangedAddPathLimit(t *testing.T) {
	s := r03Server(t)
	target := refreshTestPeer(t, s, "192.0.2.20", 65020, bgp.BGP_ADD_PATH_SEND, 1)
	a := makePath(t, "203.0.113.0/24", "192.0.2.1", 0)
	a.SetSource(&table.PeerInfo{AS: 65010, Address: netip.MustParseAddr("192.0.2.10"), ID: netip.MustParseAddr("192.0.2.10")})
	b := makePath(t, "203.0.113.0/24", "192.0.2.2", 0)
	b.SetSource(&table.PeerInfo{AS: 65011, Address: netip.MustParseAddr("192.0.2.11"), ID: netip.MustParseAddr("192.0.2.11")})
	s.propagateUpdate(nil, []*table.Path{a})
	refreshOutgoing(t, target)
	s.propagateUpdate(nil, []*table.Path{b})
	limit := func(n uint8) {
		c := target.fsm.pConf.ReadCopy()
		c.AfiSafis[0].AddPaths.Config.SendMax = n
		target.fsm.pConf.Update(&c)
		s.softResetOutPeer(target, bgp.RF_IPv4_UC, false)
	}
	limit(2)
	require.Len(t, refreshOutgoing(t, target), 2)
	require.Equal(t, uint8(2), target.getRoutesCount(bgp.RF_IPv4_UC, a.GetPrefix()))
	limit(1)
	paths := refreshOutgoing(t, target)
	require.Len(t, paths, 2)
	require.True(t, paths[0].IsWithdraw, "a formerly advertised path must be withdrawn when the ceiling decreases")
	require.Equal(t, uint8(1), target.getRoutesCount(bgp.RF_IPv4_UC, a.GetPrefix()))
	selected, blocked := a, b
	if target.hasPathAlreadyBeenSent(b) {
		selected, blocked = b, a
	}
	s.propagateUpdate(nil, []*table.Path{selected.Clone(true)})
	paths = refreshOutgoing(t, target)
	require.Len(t, paths, 2, "withdrawal must promote the still eligible fallback")
	require.True(t, paths[0].IsWithdraw)
	require.Equal(t, blocked.LocalID(), paths[1].LocalID())
	require.False(t, target.isPathSendMaxFiltered(blocked))
	info, err := s.getAdjRibInfo(target.ID(), bgp.RF_IPv4_UC, false)
	require.NoError(t, err)
	require.Equal(t, 1, info.NumPath)
	s.propagateUpdate(nil, []*table.Path{blocked.Clone(true)})
	require.True(t, refreshOutgoing(t, target)[0].IsWithdraw)
	require.Zero(t, target.getRoutesCount(bgp.RF_IPv4_UC, a.GetPrefix()))
}

func TestRefreshResponseWaitsForInitialEOR(t *testing.T) {
	s := r03Server(t)
	p := refreshTestPeer(t, s, "192.0.2.10", 65010, bgp.BGP_ADD_PATH_NONE, 0)
	c := p.fsm.pConf.ReadCopy()
	c.GracefulRestart.State.Enabled = true
	p.fsm.pConf.Update(&c)
	s.softResetOutPeer(p, bgp.RF_IPv4_UC, false)
	select {
	case item := <-p.fsm.outgoingCh.Out():
		msg := item.(*fsmOutgoingMsg)
		require.Zero(t, msg.RefreshFamily, "BoRR must wait until initial EOR has been queued")
		require.Len(t, msg.Paths, 1)
		require.True(t, msg.Paths[0].IsEOR())
	case <-time.After(3 * time.Second):
		t.Fatal("initial EOR missing")
	}
	s.softResetOutPeer(p, bgp.RF_IPv4_UC, false)
	select {
	case item := <-p.fsm.outgoingCh.Out():
		require.Equal(t, bgp.RF_IPv4_UC, item.(*fsmOutgoingMsg).RefreshFamily)
	case <-time.After(3 * time.Second):
		t.Fatal("refresh after initial EOR missing")
	}
}

func TestEstablishedPeerAcceptsLiveUpdateAfterInitialReplay(t *testing.T) {
	s := r03Server(t)
	target := refreshTestPeer(t, s, "192.0.2.20", 65020, bgp.BGP_ADD_PATH_SEND, 1)
	target.fsm.state.Store(bgp.BGP_FSM_OPENCONFIRM)
	conf := target.fsm.pConf.ReadCopy()
	conf.State.SessionState = oc.SESSION_STATE_OPENCONFIRM
	target.fsm.pConf.Update(&conf)
	// The FSM stores its new state after this callback returns. A different
	// peer can deliver a route in that interval, after the initial empty replay.
	s.handleFSMMessage(target, &fsmMsg{MsgType: fsmMsgStateChange, MsgData: bgp.BGP_FSM_ESTABLISHED, timestamp: time.Now()})
	path := makePath(t, "203.0.113.0/24", "192.0.2.1", 0)
	s.propagateUpdate(nil, []*table.Path{path})
	paths := refreshOutgoing(t, target)
	require.Len(t, paths, 1, "a route arriving after initial replay must not disappear in the state publication gap")
	require.Equal(t, path.GetPrefix(), paths[0].GetPrefix())
	require.Equal(t, uint8(1), target.getRoutesCount(bgp.RF_IPv4_UC, path.GetPrefix()))
}
