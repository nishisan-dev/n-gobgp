package server

import (
	"context"
	"fmt"
	"net/netip"
	"testing"
	"time"

	"github.com/osrg/gobgp/v4/internal/pkg/table"
	"github.com/osrg/gobgp/v4/pkg/config/oc"
	"github.com/osrg/gobgp/v4/pkg/packet/bgp"
	"github.com/stretchr/testify/require"
)

func TestGRNewConnectionReplacesEstablishedSession(t *testing.T) {
	for _, enabled := range []bool{true, false} {
		t.Run(fmt.Sprint(enabled), func(t *testing.T) {
			old := NewMockConnection()
			p, h := makePeerAndHandler(old)
			t.Cleanup(func() { cleanPeerAndHandler(p, h) })
			c := p.fsm.pConf.ReadCopy()
			c.GracefulRestart.State.Enabled = enabled
			c.GracefulRestart.State.PeerRestartTime = 60
			p.fsm.pConf.Update(&c)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			done := make(chan *fsmStateReason, 1)
			go func() { _, reason := h.established(ctx); done <- reason }()
			incoming := NewMockConnection()
			t.Cleanup(func() { incoming.Close() })
			p.fsm.connCh <- incoming
			if enabled {
				select {
				case reason := <-done:
					require.Equal(t, fsmGracefulRestart, reason.Type)
				case <-time.After(time.Second):
					cancel()
					<-done
					t.Fatal("GR reconnect must replace the previous socket without waiting for hold time")
				}
			} else {
				select {
				case <-done:
					t.Fatal("non-GR collision must preserve the established session")
				case <-time.After(20 * time.Millisecond):
				}
				cancel()
				<-done
			}
		})
	}
}

func llgrTestPeer(t *testing.T, s *BgpServer) *peer {
	t.Helper()
	p := refreshTestPeer(t, s, "192.0.2.10", 65010, bgp.BGP_ADD_PATH_NONE, 0)
	c := p.fsm.pConf.ReadCopy()
	c.State.SessionState = oc.SESSION_STATE_ACTIVE
	c.GracefulRestart.State.Enabled = true
	c.GracefulRestart.State.PeerRestarting = true
	c.GracefulRestart.State.LongLivedEnabled = true
	for i := range c.AfiSafis {
		c.AfiSafis[i].MpGracefulRestart.State.Enabled = true
		c.AfiSafis[i].MpGracefulRestart.State.Received = true
		c.AfiSafis[i].MpGracefulRestart.State.Running = true
		c.AfiSafis[i].LongLivedGracefulRestart.State.Enabled = true
		c.AfiSafis[i].LongLivedGracefulRestart.State.Received = true
		c.AfiSafis[i].LongLivedGracefulRestart.State.PeerRestartTime = 1 + uint32(i)*4
	}
	p.fsm.pConf.Update(&c)
	t.Cleanup(p.stopPeerRestarting)
	return p
}

func llgrExpireGR(s *BgpServer, p *peer, reason fsmStateReasonType) {
	s.handleFSMMessage(p, &fsmMsg{MsgType: fsmMsgStateChange, MsgData: bgp.BGP_FSM_IDLE, StateReason: newfsmStateReason(reason, nil, nil)})
}

func TestLLGRDoesNotStartOnFailedHandshake(t *testing.T) {
	s := r03Server(t)
	p := llgrTestPeer(t, s)
	llgrExpireGR(s, p, fsmReadFailed)
	time.Sleep(20 * time.Millisecond)
	require.False(t, p.fsm.pConf.ReadOnly().AfiSafis[0].LongLivedGracefulRestart.State.Running)
}

func TestLLGRExpiryPreservesRefreshedPaths(t *testing.T) {
	s := r03Server(t)
	p := llgrTestPeer(t, s)
	for _, prefix := range []string{"203.0.113.0/24", "203.0.114.0/24"} {
		refreshReceive(s, p, refreshTestMessage(t, prefix, 0, false))
	}
	s.propagateUpdate(p, p.StaleAll([]bgp.Family{bgp.RF_IPv4_UC}))
	llgrExpireGR(s, p, fsmRestartTimerExpired)
	refreshReceive(s, p, refreshTestMessage(t, "203.0.113.0/24", 0, false))
	require.Eventually(t, func() bool {
		return p.fsm.pConf.ReadOnly().AfiSafis[0].LongLivedGracefulRestart.State.PeerRestartTimerExpired
	}, 3*time.Second, 10*time.Millisecond)
	var paths []*table.Path
	require.NoError(t, s.mgmtOperation(func() error {
		paths = p.adjRibIn.PathList([]bgp.Family{bgp.RF_IPv4_UC}, false)
		return nil
	}, false))
	require.Len(t, paths, 1, "LLST expiry must remove only routes still stale during synchronization")
	require.Equal(t, "203.0.113.0/24", paths[0].GetPrefix())
}

func TestLLGREORStopsOnlyCompletedFamilyTimer(t *testing.T) {
	s := r03Server(t)
	p := llgrTestPeer(t, s)
	refreshReceive(s, p, refreshTestMessage(t, "203.0.113.0/24", 0, false))
	refreshReceive(s, p, refreshTestMessage(t, "2001:db8:10::/48", 0, false))
	s.propagateUpdate(p, p.StaleAll([]bgp.Family{bgp.RF_IPv4_UC, bgp.RF_IPv6_UC}))
	llgrExpireGR(s, p, fsmRestartTimerExpired)
	refreshReceive(s, p, refreshTestMessage(t, "203.0.113.0/24", 0, false))
	refreshReceive(s, p, bgp.NewEndOfRib(bgp.RF_IPv4_UC))
	require.Never(t, func() bool {
		count := 0
		require.NoError(t, s.mgmtOperation(func() error { count = p.adjRibIn.Count([]bgp.Family{bgp.RF_IPv4_UC}); return nil }, false))
		return count != 1
	}, 1300*time.Millisecond, 10*time.Millisecond)
	c := p.fsm.pConf.ReadOnly()
	require.False(t, c.AfiSafis[0].LongLivedGracefulRestart.State.Running)
	require.True(t, c.AfiSafis[1].LongLivedGracefulRestart.State.Running)
	require.True(t, c.GracefulRestart.State.PeerRestarting)
}

func TestGRReconnectionReconcilesRetainedFamilies(t *testing.T) {
	for _, llgr := range []bool{false, true} {
		for _, mode := range []string{"preserved", "clear-forwarding", "missing-family", "missing-capability"} {
			t.Run(fmt.Sprintf("llgr=%v/%s", llgr, mode), func(t *testing.T) {
				s := r03Server(t)
				p := llgrTestPeer(t, s)
				refreshReceive(s, p, refreshTestMessage(t, "203.0.113.0/24", 0, false))
				refreshReceive(s, p, refreshTestMessage(t, "2001:db8:10::/48", 0, false))
				s.propagateUpdate(p, p.StaleAll(p.configuredRFlist()))
				c := p.fsm.pConf.ReadCopy()
				if llgr {
					for i := range c.AfiSafis {
						c.AfiSafis[i].MpGracefulRestart.State.Running = false
						c.AfiSafis[i].LongLivedGracefulRestart.State.Running = true
					}
				}
				p.fsm.pConf.Update(&c)
				grTuples := []*bgp.CapGracefulRestartTuple{bgp.NewCapGracefulRestartTuple(bgp.RF_IPv4_UC, !llgr && mode == "preserved"), bgp.NewCapGracefulRestartTuple(bgp.RF_IPv6_UC, !llgr)}
				llgrTuples := []*bgp.CapLongLivedGracefulRestartTuple{bgp.NewCapLongLivedGracefulRestartTuple(bgp.RF_IPv4_UC, mode == "preserved", 60), bgp.NewCapLongLivedGracefulRestartTuple(bgp.RF_IPv6_UC, true, 60)}
				if mode == "missing-family" {
					grTuples, llgrTuples = grTuples[1:], llgrTuples[1:]
				}
				p.fsm.capMap = map[bgp.BGPCapabilityCode][]bgp.ParameterCapabilityInterface{
					bgp.BGP_CAP_GRACEFUL_RESTART: {bgp.NewCapGracefulRestart(true, false, 60, grTuples)},
				}
				if llgr {
					p.fsm.capMap[bgp.BGP_CAP_LONG_LIVED_GRACEFUL_RESTART] = []bgp.ParameterCapabilityInterface{bgp.NewCapLongLivedGracefulRestart(llgrTuples)}
				}
				if mode == "missing-capability" {
					p.fsm.capMap = nil
				}
				s.handleFSMMessage(p, &fsmMsg{MsgType: fsmMsgStateChange, MsgData: bgp.BGP_FSM_ESTABLISHED, StateReason: newfsmStateReason(fsmOpenMsgReceived, nil, nil)})
				expected4, expected6 := 0, 1
				switch mode {
				case "preserved":
					expected4 = 1
				case "missing-capability":
					expected6 = 0
				}
				require.Equal(t, expected4, p.adjRibIn.Count([]bgp.Family{bgp.RF_IPv4_UC}))
				require.Equal(t, expected6, p.adjRibIn.Count([]bgp.Family{bgp.RF_IPv6_UC}))
			})
		}
	}
}

func TestLLGRRequiresNegotiatedGR(t *testing.T) {
	for _, grEnabled := range []bool{true, false} {
		t.Run(fmt.Sprint(grEnabled), func(t *testing.T) {
			s := r03Server(t)
			p := llgrTestPeer(t, s)
			m := NewMockConnection()
			m.SetRemoteAddr("192.0.2.10")
			t.Cleanup(func() { m.Close() })
			p.fsm.conn = m
			c := p.fsm.pConf.ReadCopy()
			c.GracefulRestart.Config.Enabled = grEnabled
			c.GracefulRestart.Config.LongLivedEnabled = true
			p.fsm.pConf.Update(&c)
			caps := []bgp.ParameterCapabilityInterface{bgp.NewCapLongLivedGracefulRestart([]*bgp.CapLongLivedGracefulRestartTuple{bgp.NewCapLongLivedGracefulRestartTuple(bgp.RF_IPv4_UC, true, 60)})}
			if !grEnabled {
				caps = append(caps, bgp.NewCapGracefulRestart(false, false, 60, []*bgp.CapGracefulRestartTuple{bgp.NewCapGracefulRestartTuple(bgp.RF_IPv4_UC, true)}))
			}
			var err error
			p.fsm.recvOpen, err = bgp.NewBGPOpenMessage(65010, 90, netip.MustParseAddr("192.0.2.10"), []bgp.OptionParameterInterface{bgp.NewOptionParameterCapability(caps)})
			require.NoError(t, err)
			p.fsm.stateChange(bgp.BGP_FSM_ESTABLISHED, newfsmStateReason(fsmOpenMsgReceived, nil, nil))
			require.False(t, p.fsm.pConf.ReadOnly().GracefulRestart.State.LongLivedEnabled)
		})
	}
}

func TestGRNegotiationDoesNotReusePreviousSession(t *testing.T) {
	s := r03Server(t)
	p := llgrTestPeer(t, s)
	m := NewMockConnection()
	m.SetRemoteAddr("192.0.2.10")
	t.Cleanup(func() { m.Close() })
	p.fsm.conn = m
	c := p.fsm.pConf.ReadCopy()
	c.GracefulRestart.Config.Enabled = true
	c.GracefulRestart.Config.LongLivedEnabled = true
	c.GracefulRestart.State.NotificationEnabled = true
	p.fsm.pConf.Update(&c)
	p.fsm.stateChange(bgp.BGP_FSM_ESTABLISHED, newfsmStateReason(fsmOpenMsgReceived, nil, nil))
	c = p.fsm.pConf.ReadCopy()
	require.False(t, c.GracefulRestart.State.Enabled)
	require.False(t, c.GracefulRestart.State.LongLivedEnabled)
	require.False(t, c.GracefulRestart.State.NotificationEnabled)
	for _, af := range c.AfiSafis {
		require.False(t, af.MpGracefulRestart.State.Received)
		require.False(t, af.LongLivedGracefulRestart.State.Received)
	}
}

func TestLLGRZeroTimeAndNoLLGRWithdrawals(t *testing.T) {
	s := r03Server(t)
	p := llgrTestPeer(t, s)
	c := p.fsm.pConf.ReadCopy()
	c.AfiSafis[0].LongLivedGracefulRestart.State.PeerRestartTime = 0
	c.AfiSafis[1].LongLivedGracefulRestart.State.PeerRestartTime = 60
	p.fsm.pConf.Update(&c)
	refreshReceive(s, p, refreshTestMessage(t, "203.0.113.0/24", 0, false))
	refreshReceive(s, p, refreshTestMessage(t, "2001:db8:10::/48", 0, false))
	for i, prefix := range []string{"2001:db8:11::/48", "2001:db8:12::/48"} {
		m := refreshTestMessage(t, prefix, 0, false)
		body := m.Body.(*bgp.BGPUpdate)
		body.PathAttributes = append(body.PathAttributes, bgp.NewPathAttributeCommunities([]uint32{uint32(bgp.COMMUNITY_NO_LLGR)}))
		if i == 1 {
			body.PathAttributes[1] = bgp.NewPathAttributeAsPath([]bgp.AsPathParamInterface{bgp.NewAs4PathParam(2, []uint32{65000})})
		}
		refreshReceive(s, p, m)
	}
	counts := refreshEventCounts(t, s)
	counts(4, 3, 3)
	s.propagateUpdate(p, p.StaleAll(p.configuredRFlist()))
	llgrExpireGR(s, p, fsmRestartTimerExpired)
	counts(1, 1, 1)
	require.Zero(t, p.adjRibIn.Count([]bgp.Family{bgp.RF_IPv4_UC}))
	refreshReceive(s, p, bgp.NewEndOfRib(bgp.RF_IPv4_UC))
	refreshReceive(s, p, bgp.NewEndOfRib(bgp.RF_IPv6_UC))
	counts(0, 0, 0)
	require.False(t, p.fsm.pConf.ReadOnly().GracefulRestart.State.PeerRestarting)
}

func TestLLGRQueuedExpiryDoesNotAffectNextRecovery(t *testing.T) {
	s := r03Server(t)
	p := llgrTestPeer(t, s)
	family := bgp.RF_IPv4_UC
	old, _ := p.startLLGRTimer(family)
	require.NotNil(t, old)
	p.finishRestartFamilies([]bgp.Family{family})
	c := p.fsm.pConf.ReadCopy()
	c.AfiSafis[0].MpGracefulRestart.State.Running = true
	p.fsm.pConf.Update(&c)
	current, _ := p.startLLGRTimer(family)
	require.NotNil(t, current)
	require.False(t, p.expireLLGRTimer(family, old), "queued callback from before EOR must not expire a new recovery")
	require.True(t, p.fsm.pConf.ReadOnly().AfiSafis[0].LongLivedGracefulRestart.State.Running)
	require.True(t, p.expireLLGRTimer(family, current))
}

func TestLLGRExpiryDoesNotAffectReplacementPeer(t *testing.T) {
	s := r03Server(t)
	p := llgrTestPeer(t, s)
	refreshReceive(s, p, refreshTestMessage(t, "203.0.113.0/24", 0, false))
	s.propagateUpdate(p, p.StaleAll(p.configuredRFlist()))
	llgrExpireGR(s, p, fsmRestartTimerExpired)
	replacement := refreshTestPeer(t, s, "192.0.2.10", 65010, bgp.BGP_ADD_PATH_NONE, 0)
	refreshReceive(s, replacement, refreshTestMessage(t, "203.0.113.0/24", 0, false))
	counts := refreshEventCounts(t, s)
	counts(1, 1, 1)
	time.Sleep(1300 * time.Millisecond)
	counts(1, 1, 1)
	require.False(t, p.fsm.pConf.ReadOnly().AfiSafis[0].LongLivedGracefulRestart.State.PeerRestartTimerExpired)
}

func TestLLGRAdministrativeStopCancelsRecovery(t *testing.T) {
	for _, reason := range []fsmStateReasonType{fsmAdminDown, fsmAdminReset, fsmHardReset} {
		t.Run(newfsmStateReason(reason, nil, nil).String(), func(t *testing.T) {
			s := r03Server(t)
			p := llgrTestPeer(t, s)
			refreshReceive(s, p, refreshTestMessage(t, "203.0.113.0/24", 0, false))
			s.propagateUpdate(p, p.StaleAll(p.configuredRFlist()))
			llgrExpireGR(s, p, fsmRestartTimerExpired)
			llgrExpireGR(s, p, reason)
			require.Zero(t, p.adjRibIn.Count([]bgp.Family{bgp.RF_IPv4_UC}), "explicit stop during recovery must remove retained routes")
			require.False(t, p.fsm.pConf.ReadOnly().GracefulRestart.State.PeerRestarting)
		})
	}
}
