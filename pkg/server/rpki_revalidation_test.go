package server

import (
	"context"
	"encoding/binary"
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
	"github.com/osrg/gobgp/v4/pkg/packet/rtr"
	"github.com/stretchr/testify/require"
)

func r03Server(t *testing.T) *BgpServer {
	t.Helper()
	s := NewBgpServer()
	go s.Serve()
	require.NoError(t, s.StartBgp(context.Background(), &api.StartBgpRequest{Global: &api.Global{Asn: 65000, RouterId: "192.0.2.254", ListenPort: -1}}))
	t.Cleanup(s.Stop)
	return s
}

func r03Peer(t *testing.T, s *BgpServer, address string, asn uint32) (*peer, *MockConnection) {
	t.Helper()
	conf := &oc.Neighbor{Config: oc.NeighborConfig{NeighborAddress: netip.MustParseAddr(address), PeerAs: asn}, Transport: oc.Transport{Config: oc.TransportConfig{PassiveMode: true}}, AfiSafis: []oc.AfiSafi{{Config: oc.AfiSafiConfig{AfiSafiName: oc.AFI_SAFI_TYPE_IPV4_UNICAST, Enabled: true}}}}
	conf.AfiSafis = append(conf.AfiSafis, oc.AfiSafi{Config: oc.AfiSafiConfig{AfiSafiName: oc.AFI_SAFI_TYPE_IPV6_UNICAST, Enabled: true}})
	conf.RouteReflector.Config.RouteReflectorClient = true
	conf.RouteReflector.Config.RouteReflectorClusterId = netip.MustParseAddr("192.0.2.254")
	w := newPeerStateWaiter(s, api.PeerState_SESSION_STATE_ACTIVE)
	require.NoError(t, s.AddPeer(context.Background(), &api.AddPeerRequest{Peer: oc.NewPeerFromConfigStruct(conf)}))
	w.Wait(t, 5*time.Second)
	p := s.neighborMap[netip.MustParseAddr(address)]
	m := NewMockConnection()
	m.SetRemoteAddr(address)
	t.Cleanup(func() { m.Close() })
	p.fsm.connCh <- m
	open, err := bgp.NewBGPOpenMessage(uint16(asn), 90, netip.MustParseAddr(address), []bgp.OptionParameterInterface{bgp.NewOptionParameterCapability([]bgp.ParameterCapabilityInterface{bgp.NewCapMultiProtocol(bgp.RF_IPv4_UC), bgp.NewCapMultiProtocol(bgp.RF_IPv6_UC)})})
	require.NoError(t, err)
	w = newPeerStateWaiter(s, api.PeerState_SESSION_STATE_ESTABLISHED)
	m.PushBgpMessage(open)
	m.PushBgpMessage(bgp.NewBGPKeepAliveMessage())
	w.Wait(t, 5*time.Second)
	return p, m
}

// Read the actual sent UPDATEs rather than deriving Adj-RIB-Out by applying
// today's policy again, which would hide a stale advertisement.
func r03Advertised(m *MockConnection) map[string]bool {
	paths := map[string]bool{}
	for _, chunk := range m.GetSentMessages() {
		for len(chunk) >= bgp.BGP_HEADER_LENGTH {
			n := int(binary.BigEndian.Uint16(chunk[16:18]))
			if n < bgp.BGP_HEADER_LENGTH || n > len(chunk) {
				break
			}
			msg, err := bgp.ParseBGPMessage(chunk[:n], &bgp.MarshallingOption{Use2ByteAS: true})
			chunk = chunk[n:]
			if err != nil {
				continue
			}
			if u, ok := msg.Body.(*bgp.BGPUpdate); ok {
				for _, p := range u.WithdrawnRoutes {
					delete(paths, p.NLRI.String())
				}
				for _, p := range u.NLRI {
					paths[p.NLRI.String()] = true
				}
				for _, attr := range u.PathAttributes {
					switch a := attr.(type) {
					case *bgp.PathAttributeMpReachNLRI:
						for _, p := range a.Value {
							paths[p.NLRI.String()] = true
						}
					case *bgp.PathAttributeMpUnreachNLRI:
						for _, p := range a.Value {
							delete(paths, p.NLRI.String())
						}
					}
				}
			}
		}
	}
	return paths
}

func TestRPKIUpdateRevalidatesInstalledRoute(t *testing.T) {
	for _, pfx := range []string{"203.0.113.0/24", "2001:db8:10::/48"} {
		t.Run(pfx, func(t *testing.T) {
			for _, direction := range []api.PolicyDirection{api.PolicyDirection_POLICY_DIRECTION_IMPORT, api.PolicyDirection_POLICY_DIRECTION_EXPORT} {
				t.Run(direction.String(), func(t *testing.T) {
					for _, recovery := range []string{"withdraw", "disable", "delete", "expire"} {
						t.Run(recovery, func(t *testing.T) { testRPKIRevalidation(t, pfx, direction, recovery) })
					}
				})
			}
		})
	}
}

func testRPKIRevalidation(t *testing.T, prefix string, direction api.PolicyDirection, recovery string) {
	s := r03Server(t)
	src, input := r03Peer(t, s, "192.0.2.10", 65000)
	observer, output := r03Peer(t, s, "192.0.2.20", 65000)
	policy := &api.Policy{Name: "reject-invalid", Statements: []*api.Statement{{Name: "invalid", Conditions: &api.Conditions{RpkiResult: api.ValidationState_VALIDATION_STATE_INVALID}, Actions: &api.Actions{RouteAction: api.RouteAction_ROUTE_ACTION_REJECT}}}}
	require.NoError(t, s.AddPolicy(context.Background(), &api.AddPolicyRequest{Policy: policy}))
	address := src.ID()
	if direction == api.PolicyDirection_POLICY_DIRECTION_EXPORT {
		address = observer.ID()
	}
	require.NoError(t, s.AddPolicyAssignment(context.Background(), &api.AddPolicyAssignmentRequest{Assignment: &api.PolicyAssignment{Name: address, Direction: direction, Policies: []*api.Policy{policy}, DefaultAction: api.RouteAction_ROUTE_ACTION_ACCEPT}}))
	const cache = "192.0.2.100:323"
	require.NoError(t, s.mgmtOperation(func() error {
		s.roaManager.clientMap[cache] = &roaClient{host: cache, cancelfnc: func() {}}
		return nil
	}, true))
	pfx := netip.MustParsePrefix(prefix)
	family := bgp.RF_IPv4_UC
	controlPrefix := "198.51.100.0/24"
	if pfx.Addr().Is6() {
		family = bgp.RF_IPv6_UC
		controlPrefix = "2001:db8:20::/48"
	}
	nlri, err := bgp.NewIPAddrPrefix(pfx)
	require.NoError(t, err)
	control, err := bgp.NewIPAddrPrefix(netip.MustParsePrefix(controlPrefix))
	require.NoError(t, err)
	attrs := []bgp.PathAttributeInterface{bgp.NewPathAttributeOrigin(0), bgp.NewPathAttributeAsPath([]bgp.AsPathParamInterface{bgp.NewAsPathParam(bgp.BGP_ASPATH_ATTR_TYPE_SEQ, []uint16{65010})})}
	nlris := []bgp.PathNLRI{{NLRI: nlri}, {NLRI: control}}
	if family == bgp.RF_IPv4_UC {
		nh, err := bgp.NewPathAttributeNextHop(netip.MustParseAddr("192.0.2.10"))
		require.NoError(t, err)
		attrs = append(attrs, nh)
	} else {
		mp, err := bgp.NewPathAttributeMpReachNLRI(family, nlris, netip.MustParseAddr("2001:db8::10"))
		require.NoError(t, err)
		attrs = append(attrs, mp)
		nlris = nil
	}
	var mu sync.Mutex
	captured := map[string]map[string]bool{"post-policy": {}, "best": {}}
	var dropped atomic.Bool
	sub, err := s.SubscribeEvents(event.SubscriptionOptions{Sources: []string{"post-policy", "best"}, Capacity: 128, OnDrop: func() { dropped.Store(true) }}, func(v event.Observation) {
		mu.Lock()
		defer mu.Unlock()
		if v.Message.GetTable() == nil {
			return
		}
		for _, p := range v.Message.GetTable().Paths {
			nlri, err := apiutil.GetNativeNlri(p)
			if err != nil {
				dropped.Store(true)
				continue
			}
			if p.IsWithdraw {
				delete(captured[v.Metadata.Source], nlri.String())
			} else {
				captured[v.Metadata.Source][nlri.String()] = true
			}
		}
	})
	require.NoError(t, err)
	t.Cleanup(func() { sub.Stop(); <-sub.Done() })
	captureCount := func(count int) bool {
		mu.Lock()
		defer mu.Unlock()
		return len(captured["post-policy"]) == count && len(captured["best"]) == count
	}
	input.PushBgpMessage(bgp.NewBGPUpdateMessage(nil, attrs, nlris))
	require.Eventually(t, func() bool { return captureCount(2) }, 3*time.Second, time.Millisecond)
	require.Eventually(t, func() bool { return len(r03Advertised(output)) == 2 }, 3*time.Second, time.Millisecond)
	receive := func(msg rtr.RTRMessage) {
		data, err := msg.Serialize()
		require.NoError(t, err)
		s.roaManager.eventCh <- &roaEvent{EventType: roaRTR, Src: cache, Data: data, timestamp: time.Now()}
		require.NoError(t, s.mgmtOperation(func() error { return nil }, true))
	}
	vrp := func(asn uint32, flag uint8) rtr.RTRMessage {
		return rtr.NewRTRIPPrefix(pfx.Addr(), uint8(pfx.Bits()), uint8(pfx.Bits()), asn, flag)
	}
	receive(rtr.NewRTRCacheResponse(1))
	receive(vrp(65010, 1))
	receive(rtr.NewRTREndOfData(1, 1))
	receive(rtr.NewRTRCacheResponse(1))
	receive(vrp(65010, 0))
	receive(vrp(65099, 1))
	require.Len(t, r03Advertised(output), 2, "a partial RTR transaction must not change announcements")
	_, validations, err := s.getRib("", family, nil)
	require.NoError(t, err)
	found := false
	for path, v := range validations {
		if path.GetPrefix() == prefix {
			found = true
			require.Equal(t, oc.RPKI_VALIDATION_RESULT_TYPE_VALID, v.Status, "VRPs switch only at EndOfData")
		}
	}
	require.True(t, found)

	receive(rtr.NewRTREndOfData(1, 2))
	require.Eventually(t, func() bool { p := r03Advertised(output); return !p[nlri.String()] && p[control.String()] }, 3*time.Second, time.Millisecond, "RTR update must withdraw the invalid route without a manual reset")
	// The raw input must retain rejected routes so a later VRP update can recover them.
	var rawCount, ribCount int
	require.NoError(t, s.mgmtOperation(func() error {
		rawCount = len(src.adjRibIn.PathList([]bgp.Family{family}, false))
		ribCount = len(s.globalRib.GetPathList(table.GLOBAL_RIB_NAME, 0, []bgp.Family{family}))
		return nil
	}, true))
	require.Equal(t, 2, rawCount)
	expected := 2
	if direction == api.PolicyDirection_POLICY_DIRECTION_IMPORT {
		expected = 1
	}
	require.Equal(t, expected, ribCount)
	require.Eventually(t, func() bool { return captureCount(expected) }, 3*time.Second, time.Millisecond, "collection events must converge with the selected RIB")
	switch recovery {
	case "withdraw":
		receive(rtr.NewRTRCacheResponse(1))
		receive(vrp(65099, 0))
		receive(rtr.NewRTREndOfData(1, 3))
	case "disable":
		require.NoError(t, s.DisableRpki(context.Background(), &api.DisableRpkiRequest{Address: "192.0.2.100"}))
	case "delete":
		require.NoError(t, s.DeleteRpki(context.Background(), &api.DeleteRpkiRequest{Address: cache}))
	case "expire":
		require.NoError(t, s.mgmtOperation(func() error {
			client := s.roaManager.clientMap[cache]
			client.oldSessionID = client.sessionID
			return nil
		}, true))
		s.roaManager.eventCh <- &roaEvent{EventType: roaLifetimeout, Src: cache, timestamp: time.Now()}
	}
	require.Eventually(t, func() bool { return len(r03Advertised(output)) == 2 }, 3*time.Second, time.Millisecond, "recovery via %s", recovery)
	require.Equal(t, bgp.BGP_FSM_ESTABLISHED, src.State())
	require.Equal(t, bgp.BGP_FSM_ESTABLISHED, observer.State())
	require.Eventually(t, func() bool { return captureCount(2) }, 3*time.Second, time.Millisecond)
	require.False(t, dropped.Load())
	require.Nil(t, sentNotification(input))
	require.Nil(t, sentNotification(output))
}
