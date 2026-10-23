package server

import (
	"context"
	"net"
	"net/netip"
	"sync"
	"testing"
	"time"

	"github.com/osrg/gobgp/v4/api"
	"github.com/osrg/gobgp/v4/internal/pkg/table"
	"github.com/osrg/gobgp/v4/pkg/apiutil"
	"github.com/osrg/gobgp/v4/pkg/packet/bgp"
	"github.com/stretchr/testify/require"
)

func receiveASZeroTestUpdate(t *testing.T, attrs []bgp.PathAttributeInterface, legacy bool) *fsmMsg {
	t.Helper()
	nlri, err := bgp.NewIPAddrPrefix(netip.MustParsePrefix("192.0.2.0/24"))
	require.NoError(t, err)
	nh, err := bgp.NewPathAttributeNextHop(netip.MustParseAddr("192.0.2.1"))
	require.NoError(t, err)
	attrs = append([]bgp.PathAttributeInterface{bgp.NewPathAttributeOrigin(0), nh}, attrs...)
	raw, err := bgp.NewBGPUpdateMessage(nil, attrs, []bgp.PathNLRI{{NLRI: nlri}}).Serialize()
	require.NoError(t, err)
	conn, remote := net.Pipe()
	p, h := makePeerAndHandler(conn)
	h.fsm.isTreatAsWithdraw = true
	h.fsm.twoByteAsTrans = legacy
	h.fsm.isEBGP = true
	h.fsm.familyMap.Store(map[bgp.Family]bgp.BGPAddPathMode{bgp.RF_IPv4_UC: bgp.BGP_ADD_PATH_NONE})
	received := make(chan *fsmMsg, 1)
	h.callback = func(m *fsmMsg) { received <- m }
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	wg.Add(1)
	t.Cleanup(func() { cancel(); conn.Close(); remote.Close(); wg.Wait(); cleanPeerAndHandler(p, h) })
	go h.recvMessageloop(ctx, conn, make(chan struct{}, 2), make(chan fsmStateReason, 3), &wg)
	go func() { _, _ = remote.Write(raw) }()
	select {
	case m := <-received:
		return m
	case n := <-h.fsm.notification:
		t.Fatalf("unexpected session reset: %v", n.Body)
	case <-time.After(3 * time.Second):
		t.Fatal("UPDATE was not delivered")
	}
	return nil
}

func TestReceiveASZeroHandling(t *testing.T) {
	agg0, err := bgp.NewPathAttributeAggregator(uint16(0), netip.MustParseAddr("192.0.2.1"))
	require.NoError(t, err)
	aggTrans, err := bgp.NewPathAttributeAggregator(uint16(bgp.AS_TRANS), netip.MustParseAddr("192.0.2.1"))
	require.NoError(t, err)
	as4agg0, err := bgp.NewPathAttributeAs4Aggregator(0, netip.MustParseAddr("192.0.2.1"))
	require.NoError(t, err)
	as4agg, err := bgp.NewPathAttributeAs4Aggregator(4200000010, netip.MustParseAddr("192.0.2.1"))
	require.NoError(t, err)
	legacyPath := bgp.NewPathAttributeAsPath([]bgp.AsPathParamInterface{bgp.NewAsPathParam(bgp.BGP_ASPATH_ATTR_TYPE_SEQ, []uint16{65001})})
	modernPath := bgp.NewPathAttributeAsPath([]bgp.AsPathParamInterface{bgp.NewAs4PathParam(bgp.BGP_ASPATH_ATTR_TYPE_SEQ, []uint32{65001})})
	badAS4 := bgp.NewPathAttributeAs4Path([]*bgp.As4PathParam{bgp.NewAs4PathParam(bgp.BGP_ASPATH_ATTR_TYPE_SEQ, []uint32{0})})
	for _, tc := range []struct {
		name     string
		legacy   bool
		attrs    []bgp.PathAttributeInterface
		handling bgp.ErrorHandling
	}{
		{"two-octet-path", true, []bgp.PathAttributeInterface{bgp.NewPathAttributeAsPath([]bgp.AsPathParamInterface{bgp.NewAsPathParam(bgp.BGP_ASPATH_ATTR_TYPE_SEQ, []uint16{65001, 0})})}, bgp.ERROR_HANDLING_TREAT_AS_WITHDRAW},
		{"four-octet-path", false, []bgp.PathAttributeInterface{bgp.NewPathAttributeAsPath([]bgp.AsPathParamInterface{bgp.NewAs4PathParam(bgp.BGP_ASPATH_ATTR_TYPE_SEQ, []uint32{65001, 0})})}, bgp.ERROR_HANDLING_TREAT_AS_WITHDRAW},
		{"legacy-as4-path", true, []bgp.PathAttributeInterface{legacyPath, badAS4}, bgp.ERROR_HANDLING_ATTRIBUTE_DISCARD},
		{"legacy-aggregator", true, []bgp.PathAttributeInterface{legacyPath, agg0}, bgp.ERROR_HANDLING_ATTRIBUTE_DISCARD},
		{"legacy-as4-aggregator", true, []bgp.PathAttributeInterface{legacyPath, aggTrans, as4agg0}, bgp.ERROR_HANDLING_ATTRIBUTE_DISCARD},
		{"discard-base-and-auxiliary-aggregator", true, []bgp.PathAttributeInterface{legacyPath, agg0, as4agg}, bgp.ERROR_HANDLING_ATTRIBUTE_DISCARD},
		{"new-speaker-ignores-as4-attributes", false, []bgp.PathAttributeInterface{modernPath, badAS4, as4agg0}, bgp.ERROR_HANDLING_NONE},
		{"valid-legacy", true, []bgp.PathAttributeInterface{legacyPath}, bgp.ERROR_HANDLING_NONE},
		{"valid-modern", false, []bgp.PathAttributeInterface{modernPath}, bgp.ERROR_HANDLING_NONE},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m := receiveASZeroTestUpdate(t, tc.attrs, tc.legacy)
			require.Equal(t, tc.handling, m.handling)
			paths := table.ProcessMessage(m.MsgData.(*bgp.BGPMessage), &table.PeerInfo{AS: 65001}, time.Now(), m.handling == bgp.ERROR_HANDLING_TREAT_AS_WITHDRAW)
			require.Len(t, paths, 1)
			require.Equal(t, tc.handling == bgp.ERROR_HANDLING_TREAT_AS_WITHDRAW, paths[0].IsWithdraw)
			if !paths[0].IsWithdraw {
				for _, a := range paths[0].GetPathAttrs() {
					ok, err := bgp.ValidateAttribute(a, map[bgp.Family]bgp.BGPAddPathMode{bgp.RF_IPv4_UC: bgp.BGP_ADD_PATH_NONE}, true, false, false)
					require.True(t, ok)
					require.NoError(t, err)
				}
			}
		})
	}
}

func TestAddPathRejectsASZero(t *testing.T) {
	s := NewBgpServer()
	go s.Serve()
	t.Cleanup(s.Stop)
	require.NoError(t, s.StartBgp(context.Background(), &api.StartBgpRequest{Global: &api.Global{Asn: 65001, RouterId: "192.0.2.254", ListenPort: -1}}))
	nlri, err := bgp.NewIPAddrPrefix(netip.MustParsePrefix("192.0.2.0/24"))
	require.NoError(t, err)
	nh, err := bgp.NewPathAttributeNextHop(netip.MustParseAddr("192.0.2.1"))
	require.NoError(t, err)
	agg, err := bgp.NewPathAttributeAggregator(uint32(0), netip.MustParseAddr("192.0.2.1"))
	require.NoError(t, err)
	as4agg, err := bgp.NewPathAttributeAs4Aggregator(0, netip.MustParseAddr("192.0.2.1"))
	require.NoError(t, err)
	for _, bad := range []bgp.PathAttributeInterface{
		bgp.NewPathAttributeAsPath([]bgp.AsPathParamInterface{bgp.NewAs4PathParam(bgp.BGP_ASPATH_ATTR_TYPE_SEQ, []uint32{0})}),
		bgp.NewPathAttributeAs4Path([]*bgp.As4PathParam{bgp.NewAs4PathParam(bgp.BGP_ASPATH_ATTR_TYPE_SEQ, []uint32{0})}), agg, as4agg,
	} {
		path := &apiutil.Path{Family: bgp.RF_IPv4_UC, Nlri: nlri, Attrs: []bgp.PathAttributeInterface{bgp.NewPathAttributeOrigin(0), nh, bad}}
		_, err := s.AddPath(apiutil.AddPathRequest{Paths: []*apiutil.Path{path}})
		require.ErrorContains(t, err, "AS 0")
		// Removing an old invalid path must still be possible after upgrading.
		withdrawal, err := apiutil2Path(path, false, true)
		require.NoError(t, err)
		require.True(t, withdrawal.IsWithdraw)
	}
	require.Empty(t, s.globalRib.GetPathList(table.GLOBAL_RIB_NAME, 0, []bgp.Family{bgp.RF_IPv4_UC}))
	_, err = s.AddPath(apiutil.AddPathRequest{Paths: []*apiutil.Path{{Family: bgp.RF_IPv4_UC, Nlri: nlri, Attrs: []bgp.PathAttributeInterface{bgp.NewPathAttributeOrigin(0), nh, bgp.NewPathAttributeAsPath(nil)}}}})
	require.NoError(t, err)
	require.Len(t, s.globalRib.GetPathList(table.GLOBAL_RIB_NAME, 0, []bgp.Family{bgp.RF_IPv4_UC}), 1)
}

// Exercise the real receive FSM and RIB, not only the attribute validator:
// replacing an installed route with AS 0 withdraws it without affecting a
// second valid route or resetting the session.
func TestASZeroUpdateWithdrawsInstalledRoute(t *testing.T) {
	var s *BgpServer
	t.Cleanup(func() {
		if s != nil {
			s.bfdServer.Stop()
		}
	})
	s, peer := startServerWithPassivePeer(t, 65001, "10.0.0.1")
	require.NoError(t, s.mgmtOperation(func() error {
		conf := peer.fsm.pConf.ReadCopy()
		conf.ErrorHandling.Config.TreatAsWithdraw = true
		peer.fsm.pConf.Update(&conf)
		return nil
	}, true))
	connection := establishSession(t, s, peer, 65001, "10.0.0.1")
	badPrefix, err := bgp.NewIPAddrPrefix(netip.MustParsePrefix("192.0.2.0/24"))
	require.NoError(t, err)
	control, err := bgp.NewIPAddrPrefix(netip.MustParsePrefix("198.51.100.0/24"))
	require.NoError(t, err)
	nh, err := bgp.NewPathAttributeNextHop(netip.MustParseAddr("192.0.2.1"))
	require.NoError(t, err)
	update := func(asns []uint16, nlris ...bgp.PathNLRI) *bgp.BGPMessage {
		return bgp.NewBGPUpdateMessage(nil, []bgp.PathAttributeInterface{
			bgp.NewPathAttributeOrigin(0), nh,
			bgp.NewPathAttributeAsPath([]bgp.AsPathParamInterface{bgp.NewAsPathParam(bgp.BGP_ASPATH_ATTR_TYPE_SEQ, asns)}),
		}, nlris)
	}
	paths := func() []*table.Path {
		return s.globalRib.GetPathList(table.GLOBAL_RIB_NAME, 0, []bgp.Family{bgp.RF_IPv4_UC})
	}
	connection.PushBgpMessage(update([]uint16{65002}, bgp.PathNLRI{NLRI: badPrefix}, bgp.PathNLRI{NLRI: control}))
	require.Eventually(t, func() bool { return len(paths()) == 2 }, time.Second, time.Millisecond)
	connection.PushBgpMessage(update([]uint16{65002, 0}, bgp.PathNLRI{NLRI: badPrefix}))
	require.Eventually(t, func() bool {
		current := paths()
		return len(current) == 1 && current[0].GetNlri().String() == control.String()
	}, time.Second, time.Millisecond)
	require.Equal(t, bgp.BGP_FSM_ESTABLISHED, peer.State())
	require.Nil(t, sentNotification(connection))
	connection.PushBgpMessage(update([]uint16{65002}, bgp.PathNLRI{NLRI: badPrefix}))
	require.Eventually(t, func() bool { return len(paths()) == 2 }, time.Second, time.Millisecond)
}
