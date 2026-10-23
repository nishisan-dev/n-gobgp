package server

import (
	"context"
	"fmt"
	"net/netip"
	"testing"
	"time"

	"github.com/osrg/gobgp/v4/api"
	"github.com/osrg/gobgp/v4/internal/pkg/table"
	"github.com/osrg/gobgp/v4/pkg/packet/bgp"
	"github.com/stretchr/testify/require"
)

func startupAddPeer(t *testing.T, s *BgpServer, address string, deferral uint32) *peer {
	t.Helper()
	r := &api.Peer{Conf: &api.PeerConf{NeighborAddress: address, PeerAsn: 65000}, Transport: &api.Transport{PassiveMode: true}, GracefulRestart: &api.GracefulRestart{Enabled: true, LocalRestarting: true, RestartTime: 60, DeferralTime: deferral}}
	for _, afi := range []api.Family_Afi{api.Family_AFI_IP, api.Family_AFI_IP6} {
		r.AfiSafis = append(r.AfiSafis, &api.AfiSafi{Config: &api.AfiSafiConfig{Family: &api.Family{Afi: afi, Safi: api.Family_SAFI_UNICAST}, Enabled: true}, MpGracefulRestart: &api.MpGracefulRestart{Config: &api.MpGracefulRestartConfig{Enabled: true}}})
	}
	require.NoError(t, s.AddPeer(context.Background(), &api.AddPeerRequest{Peer: r}))
	var p *peer
	require.NoError(t, s.mgmtOperation(func() error { p = s.neighborMap[netip.MustParseAddr(address)]; return nil }, true))
	return p
}

func startupConnect(t *testing.T, p *peer, gr, restarting, tuples bool, extra ...bgp.ParameterCapabilityInterface) *MockConnection {
	t.Helper()
	require.Eventually(t, func() bool { return p.State() == bgp.BGP_FSM_ACTIVE }, 5*time.Second, 10*time.Millisecond)
	m := NewMockConnection()
	m.SetRemoteAddr(p.ID())
	t.Cleanup(func() { m.Close() })
	p.fsm.connCh <- m
	caps := []bgp.ParameterCapabilityInterface{bgp.NewCapFourOctetASNumber(65000), bgp.NewCapMultiProtocol(bgp.RF_IPv4_UC), bgp.NewCapMultiProtocol(bgp.RF_IPv6_UC)}
	caps = append(caps, extra...)
	if gr {
		var fs []*bgp.CapGracefulRestartTuple
		if tuples {
			fs = []*bgp.CapGracefulRestartTuple{bgp.NewCapGracefulRestartTuple(bgp.RF_IPv4_UC, true), bgp.NewCapGracefulRestartTuple(bgp.RF_IPv6_UC, true)}
		}
		caps = append(caps, bgp.NewCapGracefulRestart(restarting, false, 60, fs))
	}
	opening, err := bgp.NewBGPOpenMessage(65000, 90, netip.MustParseAddr(p.ID()), []bgp.OptionParameterInterface{bgp.NewOptionParameterCapability(caps)})
	require.NoError(t, err)
	m.PushBgpMessage(opening)
	m.PushBgpMessage(bgp.NewBGPKeepAliveMessage())
	require.Eventually(t, func() bool { return p.State() == bgp.BGP_FSM_ESTABLISHED }, 5*time.Second, 10*time.Millisecond)
	return m
}

func startupSentEOR(p *peer, family bgp.Family) bool {
	_, ok := p.sentEOR.Load(family)
	return ok
}

func TestGRStartupSelectionAndEORPerFamily(t *testing.T) {
	s := r03Server(t)
	a := startupAddPeer(t, s, "192.0.2.10", 30)
	b := startupAddPeer(t, s, "192.0.2.11", 30)
	ma := startupConnect(t, a, true, false, true)
	mb := startupConnect(t, b, true, false, true)
	ma.PushBgpMessage(refreshTestMessage(t, "203.0.113.0/24", 0, false))
	ma.PushBgpMessage(refreshTestMessage(t, "2001:db8:10::/48", 0, false))
	ma.PushBgpMessage(bgp.NewEndOfRib(bgp.RF_IPv4_UC))
	require.Eventually(t, func() bool { return a.fsm.pConf.ReadOnly().AfiSafis[0].MpGracefulRestart.State.EndOfRibReceived }, time.Second, time.Millisecond)
	require.Empty(t, s.globalRib.GetBestPathList(table.GLOBAL_RIB_NAME, 0, []bgp.Family{bgp.RF_IPv4_UC, bgp.RF_IPv6_UC}), "selection must wait for every relevant peer")
	mb.PushBgpMessage(bgp.NewEndOfRib(bgp.RF_IPv4_UC))
	require.Eventually(t, func() bool { return startupSentEOR(a, bgp.RF_IPv4_UC) && startupSentEOR(b, bgp.RF_IPv4_UC) }, 2*time.Second, 10*time.Millisecond)
	require.False(t, startupSentEOR(a, bgp.RF_IPv6_UC), "IPv4 convergence must not publish IPv6 EOR")
	require.Len(t, s.globalRib.GetBestPathList(table.GLOBAL_RIB_NAME, 0, []bgp.Family{bgp.RF_IPv4_UC}), 1)
	require.Empty(t, s.globalRib.GetBestPathList(table.GLOBAL_RIB_NAME, 0, []bgp.Family{bgp.RF_IPv6_UC}))
	ma.PushBgpMessage(bgp.NewEndOfRib(bgp.RF_IPv6_UC))
	mb.PushBgpMessage(bgp.NewEndOfRib(bgp.RF_IPv6_UC))
	require.Eventually(t, func() bool {
		return startupSentEOR(a, bgp.RF_IPv6_UC) && !b.fsm.pConf.ReadOnly().GracefulRestart.State.LocalRestarting
	}, 2*time.Second, 10*time.Millisecond)
}

func TestGRStartupRestartBitDoesNotFabricateEOR(t *testing.T) {
	s := r03Server(t)
	a := startupAddPeer(t, s, "192.0.2.10", 30)
	b := startupAddPeer(t, s, "192.0.2.11", 30)
	startupConnect(t, b, true, true, true)
	for _, af := range b.fsm.pConf.ReadOnly().AfiSafis {
		require.False(t, af.MpGracefulRestart.State.EndOfRibReceived, "R bit excludes the peer from the wait, but is not an actual EOR")
	}
	ma := startupConnect(t, a, true, false, true)
	ma.PushBgpMessage(bgp.NewEndOfRib(bgp.RF_IPv4_UC))
	ma.PushBgpMessage(bgp.NewEndOfRib(bgp.RF_IPv6_UC))
	require.Eventually(t, func() bool { return startupSentEOR(b, bgp.RF_IPv4_UC) && startupSentEOR(b, bgp.RF_IPv6_UC) }, 2*time.Second, 10*time.Millisecond)
}

func TestGRStartupHelperOnlyPeerStillNeedsEOR(t *testing.T) {
	s := r03Server(t)
	a := startupAddPeer(t, s, "192.0.2.10", 30)
	b := startupAddPeer(t, s, "192.0.2.11", 30)
	ma := startupConnect(t, a, true, false, true)
	mb := startupConnect(t, b, true, false, false)
	ma.PushBgpMessage(bgp.NewEndOfRib(bgp.RF_IPv4_UC))
	ma.PushBgpMessage(bgp.NewEndOfRib(bgp.RF_IPv6_UC))
	require.Eventually(t, func() bool { return a.fsm.pConf.ReadOnly().AfiSafis[1].MpGracefulRestart.State.EndOfRibReceived }, time.Second, time.Millisecond)
	require.Never(t, func() bool { return startupSentEOR(a, bgp.RF_IPv4_UC) }, 100*time.Millisecond, time.Millisecond)
	mb.PushBgpMessage(bgp.NewEndOfRib(bgp.RF_IPv4_UC))
	mb.PushBgpMessage(bgp.NewEndOfRib(bgp.RF_IPv6_UC))
	require.Eventually(t, func() bool { return startupSentEOR(a, bgp.RF_IPv4_UC) }, 2*time.Second, 10*time.Millisecond)
}

func TestGRStartupDeferralDeadlineSharedWithDisconnectedPeer(t *testing.T) {
	s := r03Server(t)
	a := startupAddPeer(t, s, "192.0.2.10", 1)
	b := startupAddPeer(t, s, "192.0.2.11", 30)
	startupConnect(t, a, true, false, true)
	require.Eventually(t, func() bool { return !a.fsm.pConf.ReadOnly().GracefulRestart.State.LocalRestarting }, 3*time.Second, 10*time.Millisecond)
	require.False(t, b.fsm.pConf.ReadOnly().GracefulRestart.State.LocalRestarting, "one family deadline must bound the whole restart cohort")
}

func TestGRStartupSecondaryRouteEmptyEOR(t *testing.T) {
	s := r03Server(t)
	p := refreshTestPeer(t, s, "192.0.2.10", 65010, bgp.BGP_ADD_PATH_NONE, 0)
	c := p.fsm.pConf.ReadCopy()
	c.RouteServer.Config.RouteServerClient = true
	c.RouteServer.Config.SecondaryRoute = true
	c.GracefulRestart.State.Enabled = true
	p.fsm.pConf.Update(&c)
	var paths []*table.Path
	s.getBestFromLocalCallback(p, p.negotiatedRFList(), true, true, func(p, _ []*table.Path) { paths = p })
	require.Len(t, paths, 2, "secondary-route peers need EOR even when their RIB is empty")
	for _, p := range paths {
		require.True(t, p.IsEOR())
	}
}

func TestGRStartupCoalescesWithdrawalsAndPublishesFinalBest(t *testing.T) {
	s := r03Server(t)
	a := startupAddPeer(t, s, "192.0.2.10", 30)
	b := startupAddPeer(t, s, "192.0.2.11", 30)
	ma := startupConnect(t, a, true, false, true)
	mb := startupConnect(t, b, true, false, true)
	w, err := s.watch(WatchBestPath(false))
	require.NoError(t, err)
	t.Cleanup(w.Stop)
	ma.PushBgpMessage(refreshTestMessage(t, "203.0.113.0/24", 0, false))
	ma.PushBgpMessage(refreshTestMessage(t, "203.0.113.0/24", 0, true))
	for i := range 10 {
		ma.PushBgpMessage(refreshTestMessage(t, fmt.Sprintf("198.51.100.%d/32", i), 0, true))
	}
	ma.PushBgpMessage(bgp.NewEndOfRib(bgp.RF_IPv4_UC))
	require.Eventually(t, func() bool { return a.fsm.pConf.ReadOnly().AfiSafis[0].MpGracefulRestart.State.EndOfRibReceived }, time.Second, time.Millisecond)
	pending := -1
	require.NoError(t, s.mgmtOperation(func() error { pending = len(s.localRestartFamilies[bgp.RF_IPv4_UC].paths); return nil }, true))
	require.Zero(t, pending, "withdraw-only input and canceled announcements must not grow the startup buffer")
	ma.PushBgpMessage(refreshTestMessage(t, "203.0.113.0/24", 0, false))
	preferred := refreshTestMessage(t, "203.0.113.0/24", 0, false)
	preferred.Body.(*bgp.BGPUpdate).PathAttributes = append(preferred.Body.(*bgp.BGPUpdate).PathAttributes, bgp.NewPathAttributeLocalPref(200))
	mb.PushBgpMessage(preferred)
	// Different peer FSMs run concurrently. Observe both candidates before
	// releasing the last EOR, so this asserts one startup batch rather than
	// treating a legitimately later live update as an intermediate selection.
	require.Eventually(t, func() bool {
		count := 0
		require.NoError(t, s.mgmtOperation(func() error { count = len(s.localRestartFamilies[bgp.RF_IPv4_UC].paths); return nil }, true))
		return count == 2
	}, time.Second, time.Millisecond)
	mb.PushBgpMessage(bgp.NewEndOfRib(bgp.RF_IPv4_UC))
	require.Eventually(t, func() bool { return startupSentEOR(a, bgp.RF_IPv4_UC) }, 2*time.Second, time.Millisecond)
	select {
	case ev := <-w.Event():
		paths := ev.(*watchEventBestPath).PathList
		require.Len(t, paths, 1)
		require.Equal(t, b.ID(), paths[0].GetSource().Address.String())
	case <-time.After(time.Second):
		t.Fatal("missing final best path")
	}
	select {
	case <-w.Event():
		t.Fatal("startup must not expose an intermediate best path")
	case <-time.After(30 * time.Millisecond):
	}
}

func TestGRStartupReconnectPreservesCompletedFamily(t *testing.T) {
	s := r03Server(t)
	a := startupAddPeer(t, s, "192.0.2.10", 30)
	b := startupAddPeer(t, s, "192.0.2.11", 30)
	ma := startupConnect(t, a, true, false, true)
	mb := startupConnect(t, b, true, false, true)
	ma.PushBgpMessage(bgp.NewEndOfRib(bgp.RF_IPv4_UC))
	mb.PushBgpMessage(bgp.NewEndOfRib(bgp.RF_IPv4_UC))
	require.Eventually(t, func() bool { return startupSentEOR(a, bgp.RF_IPv4_UC) }, 2*time.Second, time.Millisecond)
	ma.Close()
	startupConnect(t, a, true, true, true)
	require.Eventually(t, func() bool { return startupSentEOR(a, bgp.RF_IPv4_UC) }, 2*time.Second, time.Millisecond)
	require.False(t, startupSentEOR(a, bgp.RF_IPv6_UC))
	require.False(t, a.fsm.pConf.ReadOnly().AfiSafis[1].MpGracefulRestart.State.EndOfRibReceived)
	mb.PushBgpMessage(bgp.NewEndOfRib(bgp.RF_IPv6_UC))
	require.Eventually(t, func() bool { return startupSentEOR(a, bgp.RF_IPv6_UC) }, 2*time.Second, time.Millisecond)
}

func TestGRStartupKeepsDistinctAddPathIdentifiers(t *testing.T) {
	s := r03Server(t)
	p := refreshTestPeer(t, s, "192.0.2.10", 65010, bgp.BGP_ADD_PATH_RECEIVE, 0)
	c := p.fsm.pConf.ReadCopy()
	c.GracefulRestart.Config.Enabled = true
	c.GracefulRestart.Config.DeferralTime = 30
	c.GracefulRestart.State.LocalRestarting = true
	c.GracefulRestart.State.Enabled = true
	for i := range c.AfiSafis {
		c.AfiSafis[i].Config.Enabled = true
		c.AfiSafis[i].MpGracefulRestart.Config.Enabled = true
	}
	p.fsm.pConf.Update(&c)
	require.NoError(t, s.mgmtOperation(func() error { s.registerLocalRestart(p); return nil }, true))
	refreshReceive(s, p, refreshTestMessage(t, "203.0.113.0/24", 11, false))
	refreshReceive(s, p, refreshTestMessage(t, "203.0.113.0/24", 22, false))
	refreshReceive(s, p, refreshTestMessage(t, "203.0.113.0/24", 11, true))
	refreshReceive(s, p, bgp.NewEndOfRib(bgp.RF_IPv4_UC))
	require.Eventually(t, func() bool { return startupSentEOR(p, bgp.RF_IPv4_UC) }, 2*time.Second, time.Millisecond)
	paths := s.globalRib.GetPathList(table.GLOBAL_RIB_NAME, 0, []bgp.Family{bgp.RF_IPv4_UC})
	require.Len(t, paths, 1)
	require.Equal(t, uint32(22), paths[0].RemoteID())
}

func TestGRStartupVRFUsesGlobalFamilyOnce(t *testing.T) {
	s := r03Server(t)
	p := refreshTestPeer(t, s, "192.0.2.10", 65010, bgp.BGP_ADD_PATH_NONE, 0)
	c := p.fsm.pConf.ReadCopy()
	c.Config.Vrf = "blue"
	p.fsm.pConf.Update(&c)
	s.localRestartMu.Lock()
	s.localRestartFamilies = map[bgp.Family]*localRestartFamily{bgp.RF_IPv4_VPN: {paths: make(map[localRestartPathKey]localRestartPath)}}
	s.localRestartActive.Store(true)
	s.localRestartMu.Unlock()
	require.True(t, s.localRestartBlocked(p, bgp.RF_IPv4_UC))
	require.True(t, s.localRestartBlocked(p, bgp.RF_IPv4_VPN))
	require.False(t, s.needToAdvertise(p, bgp.RF_IPv4_VPN))
	require.False(t, s.localRestartBlocked(p, bgp.RF_IPv6_VPN))
}

func TestGRStartupRTCMembershipStillGatesInitialExport(t *testing.T) {
	s := r03Server(t)
	p := refreshTestPeer(t, s, "192.0.2.10", 65010, bgp.BGP_ADD_PATH_NONE, 0)
	c := p.fsm.pConf.ReadCopy()
	c.GracefulRestart.State.Enabled = true
	c.GracefulRestart.State.LocalRestarting = true
	p.fsm.pConf.Update(&c)
	p.setRtcEORWait(true)
	var paths []*table.Path
	s.getBestFromLocalCallback(p, p.negotiatedRFList(), true, true, func(p, _ []*table.Path) { paths = p })
	require.Empty(t, paths, "membership synchronization must precede even an empty initial EOR")
	p.setRtcEORWait(false)
	s.getBestFromLocalCallback(p, p.negotiatedRFList(), true, true, func(p, _ []*table.Path) { paths = p })
	require.Len(t, paths, 2)
	// Received RTC EOR can precede the dispatcher replay of staged membership.
	// Even with the peer wait cleared, the initial view must stay gated.
	p.fsm.familyMap.Store(map[bgp.Family]bgp.BGPAddPathMode{bgp.RF_IPv4_UC: bgp.BGP_ADD_PATH_NONE, bgp.RF_IPv6_UC: bgp.BGP_ADD_PATH_NONE, bgp.RF_RTC_UC: bgp.BGP_ADD_PATH_NONE})
	s.localRestartMu.Lock()
	s.localRestartFamilies = map[bgp.Family]*localRestartFamily{bgp.RF_RTC_UC: {paths: make(map[localRestartPathKey]localRestartPath)}}
	s.localRestartActive.Store(true)
	s.localRestartMu.Unlock()
	s.getBestFromLocalCallback(p, []bgp.Family{bgp.RF_IPv4_UC}, true, true, func(p, _ []*table.Path) { paths = p })
	require.Empty(t, paths)
	require.False(t, s.needToAdvertise(p, bgp.RF_IPv4_UC))
	s.cancelLocalRestart()
	require.True(t, s.needToAdvertise(p, bgp.RF_IPv4_UC))
}

func TestGRStartupPeerWithoutNegotiatedGRDoesNotBlock(t *testing.T) {
	s := r03Server(t)
	a := startupAddPeer(t, s, "192.0.2.10", 30)
	b := startupAddPeer(t, s, "192.0.2.11", 30)
	ma := startupConnect(t, a, true, false, true)
	startupConnect(t, b, false, false, false)
	ma.PushBgpMessage(bgp.NewEndOfRib(bgp.RF_IPv4_UC))
	ma.PushBgpMessage(bgp.NewEndOfRib(bgp.RF_IPv6_UC))
	require.Eventually(t, func() bool { return startupSentEOR(a, bgp.RF_IPv4_UC) && startupSentEOR(a, bgp.RF_IPv6_UC) }, 2*time.Second, 10*time.Millisecond)
}
