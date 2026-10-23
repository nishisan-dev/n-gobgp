package server

import (
	"context"
	"testing"
	"time"

	"github.com/osrg/gobgp/v4/api"
	"github.com/osrg/gobgp/v4/pkg/packet/bgp"
	"github.com/stretchr/testify/require"
)

func TestRefreshTimeoutQueuedCallbackCannotExpireLaterCycle(t *testing.T) {
	s := refreshTimeoutServer(t, time.Hour)
	p := refreshTestPeer(t, s, "192.0.2.10", 65010, bgp.BGP_ADD_PATH_NONE, 0)
	refreshReceive(s, p, refreshTestMessage(t, "203.0.113.0/24", 0, false))
	refreshMarker(s, p, bgp.RF_IPv4_UC, 1)
	var old *refreshDeadline
	require.NoError(t, s.mgmtOperation(func() error { old = p.refreshDeadlines[bgp.RF_IPv4_UC]; return nil }, true))
	refreshMarker(s, p, bgp.RF_IPv4_UC, 2)
	refreshReceive(s, p, refreshTestMessage(t, "203.0.113.0/24", 0, false))
	refreshMarker(s, p, bgp.RF_IPv4_UC, 1)
	require.NoError(t, s.mgmtOperation(func() error { s.expireRefresh(p, bgp.RF_IPv4_UC, old); return nil }, true))
	require.Equal(t, 1, refreshTimeoutCount(s, p, bgp.RF_IPv4_UC), "an already queued timer belongs only to its own cycle")
	// The other family's completion must not cancel the active IPv4 deadline.
	refreshMarker(s, p, bgp.RF_IPv6_UC, 1)
	refreshMarker(s, p, bgp.RF_IPv6_UC, 2)
	require.NoError(t, s.mgmtOperation(func() error { s.expireRefresh(p, bgp.RF_IPv4_UC, p.refreshDeadlines[bgp.RF_IPv4_UC]); return nil }, true))
	require.Zero(t, refreshTimeoutCount(s, p, bgp.RF_IPv4_UC))
}

func TestRefreshTimeoutCompletedCycleDoesNotRemoveFreshRoutes(t *testing.T) {
	s := refreshTimeoutServer(t, 100*time.Millisecond)
	p := refreshTestPeer(t, s, "192.0.2.10", 65010, bgp.BGP_ADD_PATH_NONE, 0)
	refreshReceive(s, p, refreshTestMessage(t, "203.0.113.0/24", 0, false))
	refreshMarker(s, p, bgp.RF_IPv4_UC, 1)
	refreshMarker(s, p, bgp.RF_IPv4_UC, 2)
	refreshReceive(s, p, refreshTestMessage(t, "203.0.113.0/24", 0, false))
	require.Never(t, func() bool { return refreshTimeoutCount(s, p, bgp.RF_IPv4_UC) == 0 }, 300*time.Millisecond, time.Millisecond)
	var deadlines, pending int
	require.NoError(t, s.mgmtOperation(func() error { deadlines, pending = len(p.refreshDeadlines), len(p.refreshPending); return nil }, true))
	require.Zero(t, deadlines)
	require.Zero(t, pending)
}

func TestRefreshTimeoutDisconnectDoesNotOverrideGRRetention(t *testing.T) {
	s := refreshTimeoutServer(t, 150*time.Millisecond)
	p := startupAddPeer(t, s, "192.0.2.10", 30)
	m := startupConnect(t, p, true, false, true, bgp.NewCapRouteRefresh(), bgp.NewCapEnhancedRouteRefresh())
	m.PushBgpMessage(bgp.NewEndOfRib(bgp.RF_IPv4_UC))
	m.PushBgpMessage(bgp.NewEndOfRib(bgp.RF_IPv6_UC))
	require.Eventually(t, func() bool { return !p.fsm.pConf.ReadOnly().GracefulRestart.State.LocalRestarting }, time.Second, time.Millisecond)
	m.PushBgpMessage(refreshTestMessage(t, "203.0.113.0/24", 0, false))
	require.Eventually(t, func() bool { return refreshTimeoutCount(s, p, bgp.RF_IPv4_UC) == 1 }, time.Second, time.Millisecond)
	m.PushBgpMessage(bgp.NewBGPRouteRefreshMessage(1, 1, 1))
	require.Eventually(t, func() bool {
		active := false
		_ = s.mgmtOperation(func() error { active = len(p.refreshDeadlines) == 1; return nil }, true)
		return active
	}, time.Second, time.Millisecond)
	m.Close()
	require.Eventually(t, func() bool {
		return p.fsm.pConf.ReadOnly().GracefulRestart.State.PeerRestarting && p.State() != bgp.BGP_FSM_ESTABLISHED
	}, time.Second, time.Millisecond)
	require.Never(t, func() bool { return refreshTimeoutCount(s, p, bgp.RF_IPv4_UC) == 0 }, 400*time.Millisecond, time.Millisecond,
		"refresh expiration from the old session must not shorten GR retention")
	var deadlines int
	require.NoError(t, s.mgmtOperation(func() error { deadlines = len(p.refreshDeadlines); return nil }, true))
	require.Zero(t, deadlines)
}

func TestRefreshTimeoutDeletionAndShutdownCancelTimers(t *testing.T) {
	for _, remove := range []bool{true, false} {
		t.Run(map[bool]string{true: "delete", false: "stop"}[remove], func(t *testing.T) {
			s := refreshTimeoutServer(t, time.Hour)
			p := startupAddPeer(t, s, "192.0.2.10", 30)
			m := startupConnect(t, p, true, false, true, bgp.NewCapRouteRefresh(), bgp.NewCapEnhancedRouteRefresh())
			m.PushBgpMessage(bgp.NewEndOfRib(bgp.RF_IPv4_UC))
			m.PushBgpMessage(bgp.NewEndOfRib(bgp.RF_IPv6_UC))
			require.Eventually(t, func() bool { return !p.fsm.pConf.ReadOnly().GracefulRestart.State.LocalRestarting }, time.Second, time.Millisecond)
			m.PushBgpMessage(bgp.NewBGPRouteRefreshMessage(1, 1, 1))
			var old *refreshDeadline
			require.Eventually(t, func() bool {
				_ = s.mgmtOperation(func() error { old = p.refreshDeadlines[bgp.RF_IPv4_UC]; return nil }, true)
				return old != nil
			}, time.Second, time.Millisecond)
			if remove {
				require.NoError(t, s.DeletePeer(context.Background(), &api.DeletePeerRequest{Address: p.ID()}))
				// Replacement peers may reuse the address; an old queued callback
				// must not touch the new peer's input or deadline.
				next := startupAddPeer(t, s, p.ID(), 30)
				connection := startupConnect(t, next, true, false, true, bgp.NewCapRouteRefresh(), bgp.NewCapEnhancedRouteRefresh())
				connection.PushBgpMessage(bgp.NewEndOfRib(bgp.RF_IPv4_UC))
				connection.PushBgpMessage(bgp.NewEndOfRib(bgp.RF_IPv6_UC))
				require.Eventually(t, func() bool { return !next.fsm.pConf.ReadOnly().GracefulRestart.State.LocalRestarting }, time.Second, time.Millisecond)
				connection.PushBgpMessage(refreshTestMessage(t, "203.0.113.0/24", 0, false))
				require.Eventually(t, func() bool { return refreshTimeoutCount(s, next, bgp.RF_IPv4_UC) == 1 }, time.Second, time.Millisecond)
				connection.PushBgpMessage(bgp.NewBGPRouteRefreshMessage(1, 1, 1))
				require.Eventually(t, func() bool {
					active := false
					_ = s.mgmtOperation(func() error { active = next.refreshDeadlines[bgp.RF_IPv4_UC] != nil; return nil }, true)
					return active
				}, time.Second, time.Millisecond)
				var deadlines int
				var matched bool
				require.NoError(t, s.mgmtOperation(func() error {
					s.expireRefresh(p, bgp.RF_IPv4_UC, old)
					deadlines = len(p.refreshDeadlines)
					matched = next == s.neighborMap[next.fsm.pConf.ReadOnly().State.NeighborAddress]
					return nil
				}, true))
				require.Zero(t, deadlines)
				require.True(t, matched)
				require.Equal(t, 1, refreshTimeoutCount(s, next, bgp.RF_IPv4_UC))
			} else {
				ctx, cancel := context.WithTimeout(context.Background(), time.Second)
				defer cancel()
				require.NoError(t, s.StopBgp(ctx, &api.StopBgpRequest{}))
				require.Empty(t, p.refreshDeadlines)
			}
		})
	}
}
