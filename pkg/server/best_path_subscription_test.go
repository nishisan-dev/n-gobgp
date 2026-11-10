package server

import (
	"context"
	"net/netip"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/osrg/gobgp/v4/api"
	"github.com/osrg/gobgp/v4/internal/pkg/table"
	"github.com/osrg/gobgp/v4/pkg/apiutil"
	"github.com/osrg/gobgp/v4/pkg/packet/bgp"
)

func bestPathTestRoute(t *testing.T, prefix string) *apiutil.Path {
	t.Helper()
	p := netip.MustParsePrefix(prefix)
	nlri, err := bgp.NewIPAddrPrefix(p)
	require.NoError(t, err)
	family := bgp.RF_IPv4_UC
	var nh bgp.PathAttributeInterface
	if p.Addr().Is4() {
		nh, err = bgp.NewPathAttributeNextHop(netip.MustParseAddr("192.0.2.1"))
	} else {
		family = bgp.RF_IPv6_UC
		nh, err = bgp.NewPathAttributeMpReachNLRI(family, []bgp.PathNLRI{{NLRI: nlri}}, netip.MustParseAddr("2001:db8::1"))
	}
	require.NoError(t, err)
	return &apiutil.Path{Family: family, Nlri: nlri, Attrs: []bgp.PathAttributeInterface{bgp.NewPathAttributeOrigin(0), nh, bgp.NewPathAttributeAsPath(nil)}}
}

func startBestPathTestServer(t *testing.T) *BgpServer {
	t.Helper()
	s := NewBgpServer()
	go s.Serve()
	t.Cleanup(s.Stop)
	require.NoError(t, s.StartBgp(context.Background(), &api.StartBgpRequest{Global: &api.Global{Asn: 65001, RouterId: "192.0.2.254", ListenPort: -1}}))
	return s
}

func nextBestPathEvent(t *testing.T, ch <-chan BestPathEvent) BestPathEvent {
	t.Helper()
	select {
	case ev := <-ch:
		return ev
	case <-time.After(5 * time.Second):
		t.Fatal("no best path event")
	}
	return BestPathEvent{}
}

func eventPrefixes(ev BestPathEvent) []string {
	var l []string
	for _, p := range ev.Paths {
		state := "+"
		if p.IsWithdraw {
			state = "-"
		}
		l = append(l, state+p.GetNlri().String())
	}
	return l
}

func TestSubscribeBestPathsDeliversCurrentTableThenChanges(t *testing.T) {
	s := startBestPathTestServer(t)
	_, err := s.AddPath(apiutil.AddPathRequest{Paths: []*apiutil.Path{bestPathTestRoute(t, "198.51.100.0/24"), bestPathTestRoute(t, "2001:db8:1::/48")}})
	require.NoError(t, err)

	events := make(chan BestPathEvent, 16)
	sub, err := s.SubscribeBestPaths([]bgp.Family{bgp.RF_IPv4_UC}, func(ev BestPathEvent) { events <- ev })
	require.NoError(t, err)
	t.Cleanup(sub.Stop)

	initial := nextBestPathEvent(t, events)
	require.True(t, initial.Initial)
	require.Equal(t, []string{"+198.51.100.0/24"}, eventPrefixes(initial))

	_, err = s.AddPath(apiutil.AddPathRequest{Paths: []*apiutil.Path{bestPathTestRoute(t, "2001:db8:2::/48")}})
	require.NoError(t, err)
	_, err = s.AddPath(apiutil.AddPathRequest{Paths: []*apiutil.Path{bestPathTestRoute(t, "203.0.113.0/24")}})
	require.NoError(t, err)
	added := nextBestPathEvent(t, events)
	require.False(t, added.Initial)
	require.Equal(t, []string{"+203.0.113.0/24"}, eventPrefixes(added), "other families are filtered out")

	require.NoError(t, s.DeletePath(apiutil.DeletePathRequest{Paths: []*apiutil.Path{bestPathTestRoute(t, "198.51.100.0/24")}}))
	require.Equal(t, []string{"-198.51.100.0/24"}, eventPrefixes(nextBestPathEvent(t, events)))
}

func TestSubscribeBestPathsDoesNotDropWhileCallbackIsBlocked(t *testing.T) {
	s := startBestPathTestServer(t)
	release := make(chan struct{})
	received := make(chan BestPathEvent, 1000)
	sub, err := s.SubscribeBestPaths([]bgp.Family{bgp.RF_IPv4_UC}, func(ev BestPathEvent) {
		<-release
		received <- ev
	})
	require.NoError(t, err)
	path := testEventPath(t, bgp.RF_IPv4_UC)
	for range 500 {
		s.notifyWatcher(watchEventTypeBestPath, &watchEventBestPath{PathList: []*table.Path{path}})
	}
	close(release)
	count := 0
	for count < 500 {
		ev := nextBestPathEvent(t, received)
		if !ev.Initial {
			count++
		}
	}
	sub.Stop()
	select {
	case <-sub.Done():
	case <-time.After(5 * time.Second):
		t.Fatal("subscription did not finish after Stop")
	}
}
