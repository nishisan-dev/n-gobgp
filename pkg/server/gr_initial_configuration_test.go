package server

import (
	"context"
	"encoding/binary"
	"io"
	"net"
	"net/netip"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/osrg/gobgp/v4/api"
	"github.com/osrg/gobgp/v4/internal/pkg/table"
	"github.com/osrg/gobgp/v4/pkg/apiutil"
	"github.com/osrg/gobgp/v4/pkg/packet/bgp"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func initialConfigurationAPI(t *testing.T) (*BgpServer, api.GoBgpServiceClient) {
	t.Helper()
	s := NewBgpServer()
	go s.Serve()
	t.Cleanup(s.Stop)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	g := grpc.NewServer()
	newAPIserver(s, s.shared, g, "")
	go func() { _ = g.Serve(listener) }()
	t.Cleanup(g.Stop)
	conn, err := grpc.NewClient(listener.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { conn.Close() })
	return s, api.NewGoBgpServiceClient(conn)
}

type startupWirePeer struct {
	conn net.Conn
	mu   sync.Mutex
	eors map[bgp.Family]int
	done chan struct{}
}

func initialConfigurationConnect(t *testing.T, p *peer, port int) *startupWirePeer {
	t.Helper()
	require.Eventually(t, func() bool { return p.State() == bgp.BGP_FSM_ACTIVE }, 3*time.Second, 10*time.Millisecond)
	dialer := net.Dialer{LocalAddr: &net.TCPAddr{IP: net.ParseIP(p.ID())}, Timeout: time.Second}
	conn, err := dialer.Dial("tcp", net.JoinHostPort("127.0.0.1", strconv.Itoa(port)))
	require.NoError(t, err)
	w := &startupWirePeer{conn: conn, eors: make(map[bgp.Family]int), done: make(chan struct{})}
	t.Cleanup(func() { conn.Close(); <-w.done })
	go func() {
		defer close(w.done)
		for {
			header := make([]byte, bgp.BGP_HEADER_LENGTH)
			if _, err := io.ReadFull(conn, header); err != nil {
				return
			}
			length := int(binary.BigEndian.Uint16(header[16:18]))
			if length < bgp.BGP_HEADER_LENGTH {
				return
			}
			body := make([]byte, length-bgp.BGP_HEADER_LENGTH)
			if _, err := io.ReadFull(conn, body); err != nil {
				return
			}
			if header[18] != bgp.BGP_MSG_UPDATE {
				continue
			}
			m, err := bgp.ParseBGPMessage(append(header, body...))
			if err != nil {
				continue
			}
			if eor, family := m.Body.(*bgp.BGPUpdate).IsEndOfRib(); eor {
				w.mu.Lock()
				w.eors[family]++
				w.mu.Unlock()
			}
		}
	}()
	caps := []bgp.ParameterCapabilityInterface{bgp.NewCapFourOctetASNumber(65000), bgp.NewCapMultiProtocol(bgp.RF_IPv4_UC), bgp.NewCapMultiProtocol(bgp.RF_IPv6_UC), bgp.NewCapGracefulRestart(false, false, 60, []*bgp.CapGracefulRestartTuple{bgp.NewCapGracefulRestartTuple(bgp.RF_IPv4_UC, true), bgp.NewCapGracefulRestartTuple(bgp.RF_IPv6_UC, true)})}
	opening, err := bgp.NewBGPOpenMessage(65000, 90, netip.MustParseAddr(p.ID()), []bgp.OptionParameterInterface{bgp.NewOptionParameterCapability(caps)})
	require.NoError(t, err)
	w.send(t, opening)
	w.send(t, bgp.NewBGPKeepAliveMessage())
	require.Eventually(t, func() bool { return p.State() == bgp.BGP_FSM_ESTABLISHED }, 3*time.Second, 10*time.Millisecond)
	return w
}

func (w *startupWirePeer) send(t *testing.T, m *bgp.BGPMessage) {
	t.Helper()
	b, err := m.Serialize()
	require.NoError(t, err)
	require.NoError(t, w.conn.SetWriteDeadline(time.Now().Add(time.Second)))
	_, err = w.conn.Write(b)
	require.NoError(t, err)
}

func (w *startupWirePeer) eor(f bgp.Family) int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.eors[f]
}

func TestGRInitialConfigurationBarrierOverGRPCAndBGP(t *testing.T) {
	s, client := initialConfigurationAPI(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	reservation, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	port := reservation.Addr().(*net.TCPAddr).Port
	require.NoError(t, reservation.Close())
	started, err := client.StartBgp(ctx, &api.StartBgpRequest{Global: &api.Global{Asn: 65000, RouterId: "192.0.2.254", ListenPort: int32(port), ListenAddresses: []string{"127.0.0.1"}}, DeferInitialConfiguration: true})
	require.NoError(t, err)
	require.True(t, started.InitialConfiguration.Pending)
	require.NotEmpty(t, started.InitialConfiguration.Generation)
	a := startupAddPeer(t, s, "127.0.0.2", 30)
	require.Never(t, func() bool { return a.State() != bgp.BGP_FSM_IDLE }, 1200*time.Millisecond, 10*time.Millisecond)
	b := startupAddPeer(t, s, "127.0.0.3", 30)
	// Local routes can be restored and removed by UUID before sessions start.
	var local []*apiutil.Path
	for _, prefix := range []string{"198.51.100.0/24", "198.51.101.0/24"} {
		nlri, err := bgp.NewIPAddrPrefix(netip.MustParsePrefix(prefix))
		require.NoError(t, err)
		nh, err := bgp.NewPathAttributeNextHop(netip.MustParseAddr("127.0.0.1"))
		require.NoError(t, err)
		local = append(local, &apiutil.Path{Family: bgp.RF_IPv4_UC, Nlri: nlri, Attrs: []bgp.PathAttributeInterface{bgp.NewPathAttributeOrigin(0), bgp.NewPathAttributeAsPath(nil), nh}})
	}
	added, err := s.AddPath(apiutil.AddPathRequest{Paths: local})
	require.NoError(t, err)
	require.Len(t, added, 2)
	require.NoError(t, s.DeletePath(apiutil.DeletePathRequest{UUIDs: []uuid.UUID{added[0].UUID}}))
	// The configuration barrier must also reject inbound connections.
	conn, err := (&net.Dialer{LocalAddr: &net.TCPAddr{IP: net.ParseIP(a.ID())}, Timeout: time.Second}).Dial("tcp", net.JoinHostPort("127.0.0.1", strconv.Itoa(port)))
	require.NoError(t, err)
	require.NoError(t, conn.SetReadDeadline(time.Now().Add(time.Second)))
	buf := make([]byte, 1)
	n, err := conn.Read(buf)
	conn.Close()
	require.Zero(t, n)
	require.Error(t, err)
	if timeout, ok := err.(net.Error); ok {
		require.False(t, timeout.Timeout(), "pending activation must close incoming sockets")
	}
	state, err := client.GetBgp(ctx, &api.GetBgpRequest{})
	require.NoError(t, err)
	require.Equal(t, started.InitialConfiguration.Generation, state.InitialConfiguration.Generation)
	require.True(t, state.InitialConfiguration.Pending)
	_, err = client.CompleteInitialConfiguration(ctx, &api.CompleteInitialConfigurationRequest{Generation: "stale-generation"})
	require.Error(t, err)
	require.Equal(t, bgp.BGP_FSM_IDLE, a.State())
	completed, err := client.CompleteInitialConfiguration(ctx, &api.CompleteInitialConfigurationRequest{Generation: started.InitialConfiguration.Generation})
	require.NoError(t, err)
	require.False(t, completed.InitialConfiguration.Pending)
	_, err = client.CompleteInitialConfiguration(ctx, &api.CompleteInitialConfigurationRequest{Generation: started.InitialConfiguration.Generation})
	require.NoError(t, err, "completion of the same generation is idempotent")
	wa := initialConfigurationConnect(t, a, port)
	wb := initialConfigurationConnect(t, b, port)
	wa.send(t, refreshTestMessage(t, "203.0.113.0/24", 0, false))
	wa.send(t, refreshTestMessage(t, "2001:db8:10::/48", 0, false))
	wa.send(t, bgp.NewEndOfRib(bgp.RF_IPv4_UC))
	require.Never(t, func() bool { return wa.eor(bgp.RF_IPv4_UC) > 0 }, 100*time.Millisecond, time.Millisecond)
	wb.send(t, bgp.NewEndOfRib(bgp.RF_IPv4_UC))
	require.Eventually(t, func() bool { return wa.eor(bgp.RF_IPv4_UC) == 1 && wb.eor(bgp.RF_IPv4_UC) == 1 }, 2*time.Second, 10*time.Millisecond)
	require.Zero(t, wa.eor(bgp.RF_IPv6_UC))
	paths := s.globalRib.GetBestPathList(table.GLOBAL_RIB_NAME, 0, []bgp.Family{bgp.RF_IPv4_UC})
	require.Len(t, paths, 2, "only the retained local route and the received BGP route should converge")
	for _, path := range paths {
		require.NotEqual(t, "198.51.100.0/24", path.GetPrefix())
	}
	require.Empty(t, s.globalRib.GetBestPathList(table.GLOBAL_RIB_NAME, 0, []bgp.Family{bgp.RF_IPv6_UC}))
	wa.send(t, bgp.NewEndOfRib(bgp.RF_IPv6_UC))
	wb.send(t, bgp.NewEndOfRib(bgp.RF_IPv6_UC))
	require.Eventually(t, func() bool { return wa.eor(bgp.RF_IPv6_UC) == 1 && wb.eor(bgp.RF_IPv6_UC) == 1 }, 2*time.Second, 10*time.Millisecond)
}

func TestGRInitialConfigurationRejectsEarlierServerGeneration(t *testing.T) {
	_, first := initialConfigurationAPI(t)
	r := &api.StartBgpRequest{Global: &api.Global{Asn: 65000, RouterId: "192.0.2.254", ListenPort: -1}, DeferInitialConfiguration: true}
	old, err := first.StartBgp(context.Background(), r)
	require.NoError(t, err)
	_, second := initialConfigurationAPI(t)
	current, err := second.StartBgp(context.Background(), r)
	require.NoError(t, err)
	require.NotEqual(t, old.InitialConfiguration.Generation, current.InitialConfiguration.Generation)
	_, err = second.CompleteInitialConfiguration(context.Background(), &api.CompleteInitialConfigurationRequest{Generation: old.InitialConfiguration.Generation})
	require.Error(t, err)
	_, err = second.StartBgp(context.Background(), r)
	require.Error(t, err, "a duplicate StartBgp must not replace the active barrier")
	state, err := second.GetBgp(context.Background(), &api.GetBgpRequest{})
	require.NoError(t, err)
	require.Equal(t, current.InitialConfiguration.Generation, state.InitialConfiguration.Generation)
	require.True(t, state.InitialConfiguration.Pending)
}

func TestGRInitialConfigurationCancellationAndShutdown(t *testing.T) {
	for _, pending := range []bool{true, false} {
		t.Run(strconv.FormatBool(pending), func(t *testing.T) {
			s, client := initialConfigurationAPI(t)
			started, err := client.StartBgp(context.Background(), &api.StartBgpRequest{Global: &api.Global{Asn: 65000, RouterId: "192.0.2.254", ListenPort: -1}, DeferInitialConfiguration: pending})
			require.NoError(t, err)
			require.Equal(t, pending, started.InitialConfiguration.Pending)
			p := startupAddPeer(t, s, "192.0.2.10", 30)
			if pending {
				ctx, cancel := context.WithCancel(context.Background())
				cancel()
				_, err = s.CompleteInitialConfiguration(ctx, &api.CompleteInitialConfigurationRequest{Generation: started.InitialConfiguration.Generation})
				require.ErrorIs(t, err, context.Canceled)
				state, err := client.GetBgp(context.Background(), &api.GetBgpRequest{})
				require.NoError(t, err)
				require.True(t, state.InitialConfiguration.Pending)
				ctx, cancel = context.WithTimeout(context.Background(), 3*time.Second)
				defer cancel()
				_, err = client.DisablePeer(ctx, &api.DisablePeerRequest{Address: p.ID()})
				require.NoError(t, err)
				require.Eventually(t, func() bool { return p.AdminState() == adminStateDown }, time.Second, time.Millisecond)
				_, err = client.EnablePeer(ctx, &api.EnablePeerRequest{Address: p.ID()})
				require.NoError(t, err)
				require.Eventually(t, func() bool { return p.AdminState() == adminStateUp }, time.Second, time.Millisecond)
				require.Equal(t, bgp.BGP_FSM_IDLE, p.State())
			} else {
				require.Eventually(t, func() bool { return p.State() == bgp.BGP_FSM_ACTIVE }, 3*time.Second, 10*time.Millisecond)
			}
			_, err = client.DeletePeer(context.Background(), &api.DeletePeerRequest{Address: p.ID()})
			require.NoError(t, err)
			// Stop also cancels a second worker still held behind the barrier.
			startupAddPeer(t, s, "192.0.2.11", 30)
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			_, err = client.StopBgp(ctx, &api.StopBgpRequest{})
			require.NoError(t, err, "stopping a pending generation must not leave FSM workers blocked")
		})
	}
}
