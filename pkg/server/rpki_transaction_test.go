package server

import (
	"log/slog"
	"net/netip"
	"testing"

	"github.com/osrg/gobgp/v4/internal/pkg/table"
	"github.com/osrg/gobgp/v4/pkg/packet/bgp"
	"github.com/osrg/gobgp/v4/pkg/packet/rtr"
	"github.com/stretchr/testify/require"
)

func TestRPKITransactionNotification(t *testing.T) {
	rt := table.NewROATable(slog.Default())
	m := newROAManager(rt, slog.Default())
	client := &roaClient{host: "192.0.2.1:323", sessionID: 1}
	count := 0
	m.onChange = func() { count++ }
	receive := func(msg rtr.RTRMessage) {
		buf, err := msg.Serialize()
		require.NoError(t, err)
		m.handleRTRMsg(client, &client.state, buf)
	}
	vrp := func(flags uint8) rtr.RTRMessage {
		return rtr.NewRTRIPPrefix(netip.MustParseAddr("203.0.113.0"), 24, 24, 65010, flags)
	}
	records := func() uint32 { r, _ := rt.Info(bgp.RF_IPv4_UC); return r[client.host] }
	receive(rtr.NewRTRCacheResponse(1))
	receive(vrp(1))
	receive(vrp(1)) // duplicate announcement
	require.Zero(t, records())
	require.Zero(t, count)
	receive(rtr.NewRTREndOfData(1, 1))
	require.Equal(t, uint32(1), records())
	require.Equal(t, 1, count)
	receive(rtr.NewRTRCacheResponse(1))
	receive(vrp(1))
	receive(rtr.NewRTREndOfData(1, 2))
	require.Equal(t, 1, count, "an unchanged set needs no route re-evaluation")
	receive(rtr.NewRTRCacheResponse(1))
	receive(vrp(0))
	receive(vrp(1))
	receive(vrp(0)) // preserve the order within one serial transaction
	require.Equal(t, uint32(1), records())
	require.Equal(t, 1, count)
	receive(rtr.NewRTREndOfData(1, 3))
	require.Zero(t, records())
	require.Equal(t, 2, count)
}
