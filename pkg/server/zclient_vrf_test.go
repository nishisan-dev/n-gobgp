package server

import (
	"context"
	"fmt"
	"net/netip"
	"syscall"
	"testing"
	"time"

	"github.com/osrg/gobgp/v4/api"
	"github.com/osrg/gobgp/v4/internal/pkg/table"
	"github.com/osrg/gobgp/v4/pkg/apiutil"
	"github.com/osrg/gobgp/v4/pkg/packet/bgp"
	"github.com/osrg/gobgp/v4/pkg/zebra"
	"github.com/stretchr/testify/require"
)

func r03AddPath(t *testing.T, s *BgpServer, vrf, prefix, nexthop string) {
	t.Helper()
	pfx := netip.MustParsePrefix(prefix)
	nlri, err := bgp.NewIPAddrPrefix(pfx)
	require.NoError(t, err)
	family := bgp.RF_IPv4_UC
	attrs := []bgp.PathAttributeInterface{bgp.NewPathAttributeOrigin(0), bgp.NewPathAttributeAsPath(nil)}
	if pfx.Addr().Is4() {
		nh, err := bgp.NewPathAttributeNextHop(netip.MustParseAddr(nexthop))
		require.NoError(t, err)
		attrs = append(attrs, nh)
	} else {
		family = bgp.RF_IPv6_UC
		mp, err := bgp.NewPathAttributeMpReachNLRI(family, []bgp.PathNLRI{{NLRI: nlri}}, netip.MustParseAddr(nexthop))
		require.NoError(t, err)
		attrs = append(attrs, mp)
	}
	_, err = s.AddPath(apiutil.AddPathRequest{VRFID: vrf, Paths: []*apiutil.Path{{Family: family, Nlri: nlri, Attrs: attrs}}})
	require.NoError(t, err)
}

func TestNHTLookupIsScopedToVRF(t *testing.T) {
	for _, tc := range []struct {
		name, prefix, nexthop, newPrefix string
		family                           uint8
		vpn                              bgp.Family
	}{
		{"ipv4", "203.0.113.0/24", "192.0.2.1", "203.0.114.0/24", syscall.AF_INET, bgp.RF_IPv4_VPN},
		{"ipv6", "2001:db8:10::/48", "2001:db8::1", "2001:db8:11::/48", syscall.AF_INET6, bgp.RF_IPv6_VPN},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := r03Server(t)
			addVrf(t, s, "blue", "65000:1", []string{"65000:1"}, []string{"65000:1"}, 1)
			addVrf(t, s, "red", "65000:2", []string{"65000:2"}, []string{"65000:2"}, 2)
			z := &zebraClient{server: s}
			require.NoError(t, s.mgmtOperation(func() error { s.zclient = z; return nil }, true))
			for _, v := range []string{"blue", "red", ""} {
				r03AddPath(t, s, v, tc.prefix, tc.nexthop)
			}
			body := &zebra.NexthopUpdateBody{Prefix: zebra.Prefix{Family: tc.family, Prefix: netip.MustParseAddr(tc.nexthop), PrefixLen: uint8(netip.MustParseAddr(tc.nexthop).BitLen())}, Metric: 10, Nexthops: []zebra.Nexthop{{Gate: netip.MustParseAddr(tc.nexthop)}}}
			for _, id := range []uint32{0, 1, 2} {
				paths := z.getPathListWithNexthopUpdate(id, body)
				require.Len(t, paths, 1)
				require.Equal(t, id, paths[0].NexthopVrfID())
			}
			require.Empty(t, z.getPathListWithNexthopUpdate(99, body))
			blue := z.getPathListWithNexthopUpdate(1, body)[0]
			require.Equal(t, tc.vpn, blue.GetFamily())
			clone := blue.Clone(false)
			require.NoError(t, clone.SetMed(50, true))
			require.Equal(t, uint32(1), clone.ToLocal().NexthopVrfID(), "lookup scope survives changed attributes and VPN conversion")
			z.handleNexthopUpdate(1, body)
			// A registration in blue does not suppress the same IP in red.
			redPaths := z.getPathListWithNexthopUpdate(2, body)
			z.cacheLock.Lock()
			blueReg := newNexthopRegisterBody([]*table.Path{blue}, z.cache(1))
			redReg := newNexthopRegisterBody(redPaths, z.cache(2))
			z.cacheLock.Unlock()
			require.Nil(t, blueReg)
			require.NotNil(t, redReg)
			body.Metric = 20
			z.handleNexthopUpdate(2, body)
			body.Metric = 30
			z.handleNexthopUpdate(0, body)
			body.Nexthops = nil
			z.handleNexthopUpdate(1, body)
			check := func(id uint32, invalid bool, metric uint32, count int) {
				paths := z.getPathListWithNexthopUpdate(id, body)
				require.Len(t, paths, count)
				for _, p := range paths {
					require.Equal(t, invalid, p.IsNexthopInvalid)
					if !invalid {
						med, err := p.GetMed()
						require.NoError(t, err)
						require.Equal(t, metric, med)
					}
				}
			}
			check(1, true, 0, 1)
			check(2, false, 20, 1)
			check(0, false, 30, 1)
			// New routes inherit only the cache for their own lookup VRF.
			for _, v := range []string{"blue", "red", ""} {
				r03AddPath(t, s, v, tc.newPrefix, tc.nexthop)
			}
			check(1, true, 0, 2)
			check(2, false, 20, 2)
			check(0, false, 30, 2)
			body.Nexthops = []zebra.Nexthop{{Gate: netip.MustParseAddr(tc.nexthop)}}
			body.Metric = 5
			z.handleNexthopUpdate(1, body)
			check(1, false, 5, 2)
			check(2, false, 20, 2)
			check(0, false, 30, 2)
			// Deleting a VRF must not poison a later VRF that reuses its numeric ID.
			require.NoError(t, s.DeleteVrf(context.Background(), &api.DeleteVrfRequest{Name: "blue"}))
			addVrf(t, s, "blue-new", "65000:3", []string{"65000:3"}, []string{"65000:3"}, 1)
			r03AddPath(t, s, "blue-new", tc.prefix, tc.nexthop)
			paths := z.getPathListWithNexthopUpdate(1, body)
			require.Len(t, paths, 1)
			_, err := paths[0].GetMed()
			require.Error(t, err, fmt.Sprintf("reused VRF must not inherit metric %d", body.Metric))
		})
	}
}

func TestNHTMultipathKeepsResolutionScope(t *testing.T) {
	rd, err := bgp.ParseRouteDistinguisher("65000:1")
	require.NoError(t, err)
	blue := makePath(t, "203.0.113.0/24", "192.0.2.1", 0).ToGlobal(&table.Vrf{Id: 1, Rd: rd, MplsLabel: 100})
	global := table.NewPath(blue.GetFamily(), nil, bgp.PathNLRI{NLRI: blue.GetNlri()}, false, blue.GetPathAttrs(), time.Now(), false)
	z := &zebraClient{client: &zebra.Client{Version: 6, Software: zebra.NewSoftware(6, "frr8.1")}}
	body, withdraw := newIPRouteBody([]*table.Path{blue, global}, 0, z)
	require.NotNil(t, body)
	require.False(t, withdraw)
	require.Len(t, body.Nexthops, 2)
	require.Equal(t, uint32(1), body.Nexthops[0].VrfID)
	require.Equal(t, uint8(1), body.Nexthops[0].LabelNum)
	require.Equal(t, []uint32{100}, body.Nexthops[0].MplsLabels)
	require.Zero(t, body.Nexthops[1].VrfID)
	require.Zero(t, body.Nexthops[1].LabelNum)
	require.Empty(t, body.Nexthops[1].MplsLabels)
}
