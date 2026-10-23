package server

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/osrg/gobgp/v4/api"
	"github.com/osrg/gobgp/v4/pkg/packet/bgp"
	"github.com/stretchr/testify/require"
)

func refreshTimeoutServer(t *testing.T, limit time.Duration) *BgpServer {
	t.Helper()
	s := NewBgpServer(RouteRefreshStaleTime(limit))
	go s.Serve()
	require.NoError(t, s.StartBgp(context.Background(), &api.StartBgpRequest{Global: &api.Global{Asn: 65000, RouterId: "192.0.2.254", ListenPort: -1}}))
	t.Cleanup(s.Stop)
	return s
}

// Adj-RIB-In belongs to the FSM; the dispatcher excludes concurrent updates.
func refreshTimeoutCount(s *BgpServer, p *peer, f bgp.Family) int {
	n := -1
	_ = s.mgmtOperation(func() error { n = p.adjRibIn.Count([]bgp.Family{f}); return nil }, true)
	return n
}

func TestRefreshTimeoutRemovesOnlyOmittedPathsAndConvergesEvents(t *testing.T) {
	s := refreshTimeoutServer(t, 200*time.Millisecond)
	p := refreshTestPeer(t, s, "192.0.2.10", 65010, bgp.BGP_ADD_PATH_RECEIVE, 0)
	other := refreshTestPeer(t, s, "192.0.2.11", 65011, bgp.BGP_ADD_PATH_RECEIVE, 0)
	refreshReceive(s, p, refreshTestMessage(t, "203.0.113.0/24", 11, false))
	refreshReceive(s, p, refreshTestMessage(t, "203.0.113.0/24", 22, false))
	refreshReceive(s, p, refreshTestMessage(t, "2001:db8:10::/48", 33, false))
	refreshReceive(s, other, refreshTestMessage(t, "198.51.100.0/24", 11, false))
	rejected := refreshTestMessage(t, "203.0.114.0/24", 11, false)
	rejected.Body.(*bgp.BGPUpdate).PathAttributes[1] = bgp.NewPathAttributeAsPath([]bgp.AsPathParamInterface{bgp.NewAs4PathParam(2, []uint32{65010, 65000})})
	refreshReceive(s, p, rejected)
	counts := refreshEventCounts(t, s)
	counts(5, 4, 3)
	refreshMarker(s, p, bgp.RF_IPv4_UC, 1)
	refreshReceive(s, p, refreshTestMessage(t, "203.0.113.0/24", 11, false))
	// Neither a policy replay nor GR EOR substitutes for an actual reannouncement.
	require.NoError(t, s.mgmtOperation(func() error { return s.softResetIn(p.ID(), bgp.RF_IPv4_UC) }, true))
	refreshReceive(s, p, bgp.NewEndOfRib(bgp.RF_IPv4_UC))
	counts(3, 3, 3)
	require.Equal(t, 1, refreshTimeoutCount(s, p, bgp.RF_IPv4_UC))
	require.Equal(t, 1, refreshTimeoutCount(s, p, bgp.RF_IPv6_UC))
	require.Equal(t, 1, refreshTimeoutCount(s, other, bgp.RF_IPv4_UC))
	require.Equal(t, bgp.BGP_FSM_ESTABLISHED, p.State(), "expiration must not reset the session")
	// A late, unpaired EoRR must not remove routes learned after expiration.
	refreshReceive(s, p, refreshTestMessage(t, "203.0.113.0/24", 22, false))
	refreshMarker(s, p, bgp.RF_IPv4_UC, 2)
	counts(4, 4, 3)
}

func TestRefreshTimeoutRepeatedBoRRDoesNotExtendRetention(t *testing.T) {
	s := refreshTimeoutServer(t, 150*time.Millisecond)
	p := refreshTestPeer(t, s, "192.0.2.10", 65010, bgp.BGP_ADD_PATH_NONE, 0)
	refreshReceive(s, p, refreshTestMessage(t, "203.0.113.0/24", 0, false))
	refreshMarker(s, p, bgp.RF_IPv4_UC, 1)
	stop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		ticker := time.NewTicker(20 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-stop:
				return
			case <-ticker.C:
				refreshMarker(s, p, bgp.RF_IPv4_UC, 1)
			}
		}
	}()
	t.Cleanup(func() { close(stop); wg.Wait() })
	require.Eventually(t, func() bool { return refreshTimeoutCount(s, p, bgp.RF_IPv4_UC) == 0 }, 2*time.Second, time.Millisecond,
		"a stream of BoRR markers must not keep omitted routes forever")
}

func TestRefreshTimeoutDisabledStillAllowsExplicitCompletion(t *testing.T) {
	s := refreshTimeoutServer(t, 0)
	p := refreshTestPeer(t, s, "192.0.2.10", 65010, bgp.BGP_ADD_PATH_NONE, 0)
	refreshReceive(s, p, refreshTestMessage(t, "203.0.113.0/24", 0, false))
	refreshMarker(s, p, bgp.RF_IPv4_UC, 1)
	require.Never(t, func() bool { return refreshTimeoutCount(s, p, bgp.RF_IPv4_UC) == 0 }, 250*time.Millisecond, time.Millisecond)
	refreshMarker(s, p, bgp.RF_IPv4_UC, 2)
	require.Zero(t, refreshTimeoutCount(s, p, bgp.RF_IPv4_UC))
}
