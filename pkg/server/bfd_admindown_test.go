package server

import (
	"context"
	"log/slog"
	"net/netip"
	"sync/atomic"
	"testing"
	"time"

	"github.com/osrg/gobgp/v4/api"
	"github.com/osrg/gobgp/v4/pkg/packet/bfd"
	"github.com/osrg/gobgp/v4/pkg/packet/bgp"
	"github.com/stretchr/testify/require"
)

// No worker runs on this peer: packet handling and timer expiry can be driven
// deterministically, without racing the BFD loop or sleeping for a timeout.
func adminDownTestPeer(t *testing.T, ps peerState) *bfdPeer {
	t.Helper()
	p := &bfdPeer{
		peerState: ps, logger: slog.Default(), peerAddress: netip.MustParseAddr("192.0.2.1"),
		myDiscriminator: 1, yourDiscriminator: 2, multiplier: 3,
		rxInterval: time.Second, txInterval: time.Second, desiredMinTx: time.Second,
		remoteMinRxInterval: time.Second, resetCtx: context.Background(),
		eventTx: time.NewTimer(time.Hour), eventExpiry: time.NewTicker(time.Hour),
	}
	p.state.Store(int32(api.BfdSessionState_BFD_SESSION_STATE_UP))
	t.Cleanup(func() { p.eventTx.Stop(); p.eventExpiry.Stop() })
	return p
}

type adminDownBGPClient struct {
	server *BgpServer
	calls  atomic.Int64
}

func (c *adminDownBGPClient) bfdDown(ctx context.Context, address netip.Addr) error {
	c.calls.Add(1)
	return c.server.bfdDown(ctx, address)
}

func TestBFDAdminDownPreservesEstablishedSession(t *testing.T) {
	const address = "10.0.0.1"
	var s *BgpServer
	// The shared helper stops BGP, but its BFD service has a separate
	// lifecycle. Register this first so it runs after the helper's cleanup.
	t.Cleanup(func() {
		if s != nil {
			s.bfdServer.Stop()
		}
	})
	s, peer := startServerWithPassivePeer(t, 65001, address)
	connection := establishSession(t, s, peer, 65001, address)
	client := &adminDownBGPClient{server: s}
	p := adminDownTestPeer(t, client)
	p.peerAddress = netip.MustParseAddr(address)
	packet := &bfd.BFDHeader{State: bfd.StateAdminDown, MyDiscriminator: 2, YourDiscriminator: 1, DetectTimeMultiplier: 3}
	p.rxPacket(packet)
	require.Zero(t, client.calls.Load())
	require.Nil(t, sentNotification(connection))
	require.Equal(t, bgp.BGP_FSM_ESTABLISHED, peer.State())
	// Recover BFD, then expire it: the same BGP session must still react to
	// a real failure with Cease/BFD Down.
	packet.State = bfd.StateDown
	p.rxPacket(packet)
	packet.State = bfd.StateInit
	p.rxPacket(packet)
	w := newPeerStateWaiter(s, api.PeerState_SESSION_STATE_IDLE)
	p.expiry()
	w.Wait(t, 5*time.Second)
	require.Equal(t, int64(1), client.calls.Load())
	require.Eventually(t, func() bool {
		n := sentNotification(connection)
		return n != nil && n.ErrorCode == bgp.BGP_ERROR_CEASE && n.ErrorSubcode == bgp.BGP_ERROR_SUB_BFD_DOWN
	}, time.Second, time.Millisecond)
}

func TestBFDAdminDownDoesNotNotifyBGP(t *testing.T) {
	for _, initial := range []api.BfdSessionState{api.BfdSessionState_BFD_SESSION_STATE_UP, api.BfdSessionState_BFD_SESSION_STATE_INIT, api.BfdSessionState_BFD_SESSION_STATE_DOWN} {
		t.Run(initial.String(), func(t *testing.T) {
			ps := &mockPeerState{}
			p := adminDownTestPeer(t, ps)
			p.state.Store(int32(initial))
			p.pollSequence = true
			p.rxPacket(&bfd.BFDHeader{State: bfd.StateAdminDown, MyDiscriminator: 2, YourDiscriminator: 1, DetectTimeMultiplier: 3})
			require.Equal(t, api.BfdSessionState_BFD_SESSION_STATE_DOWN, p.sessionState())
			require.Zero(t, atomic.LoadInt64(&ps.bfdDownCount), "administrative maintenance is not a path failure")
			if initial != api.BfdSessionState_BFD_SESSION_STATE_DOWN {
				require.Zero(t, p.yourDiscriminator)
				require.False(t, p.pollSequence)
			}
			p.expiry() // an already queued expiry must not reset BGP afterwards
			require.Zero(t, atomic.LoadInt64(&ps.bfdDownCount))
		})
	}
}

func TestBFDAdminDownDoesNotDisableFailureDetection(t *testing.T) {
	for _, failure := range []string{"remote-down", "timeout"} {
		t.Run(failure, func(t *testing.T) {
			ps := &mockPeerState{}
			p := adminDownTestPeer(t, ps)
			packet := &bfd.BFDHeader{State: bfd.StateAdminDown, MyDiscriminator: 2, YourDiscriminator: 1, DetectTimeMultiplier: 3}
			p.rxPacket(packet)
			require.Zero(t, atomic.LoadInt64(&ps.bfdDownCount))
			packet.State = bfd.StateDown
			p.rxPacket(packet)
			require.Equal(t, api.BfdSessionState_BFD_SESSION_STATE_INIT, p.sessionState())
			packet.State = bfd.StateInit
			p.rxPacket(packet)
			require.Equal(t, api.BfdSessionState_BFD_SESSION_STATE_UP, p.sessionState())
			if failure == "timeout" {
				p.expiry()
			} else {
				packet.State = bfd.StateDown
				p.rxPacket(packet)
			}
			require.Equal(t, api.BfdSessionState_BFD_SESSION_STATE_DOWN, p.sessionState())
			require.Equal(t, int64(1), atomic.LoadInt64(&ps.bfdDownCount))
		})
	}
}
