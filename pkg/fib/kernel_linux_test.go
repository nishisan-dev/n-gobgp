//go:build linux

package fib

import (
	"net/netip"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/osrg/gobgp/v4/pkg/config/oc"
)

func TestNetlinkKernelOwnsOnlyItsRoutes(t *testing.T) {
	if !inNetns(t) {
		return
	}
	k := newKernel(200)
	r := kroute{Table: 100, Prefix: netip.MustParsePrefix("198.51.100.0/24"), Metric: 20, Gateway: netip.MustParseAddr("10.0.1.1"), Interface: "wan1"}
	require.NoError(t, k.addRoute(r))
	owned, err := k.ownedRoutes()
	require.NoError(t, err)
	require.Equal(t, []kroute{r}, owned)

	moved := r
	moved.Gateway, moved.Interface = netip.MustParseAddr("10.0.2.1"), "wan2"
	require.NoError(t, k.replaceRoute(moved))
	require.Contains(t, sh(t, "ip", "route", "show", "table", "100"), "198.51.100.0/24 via 10.0.2.1 dev wan2 proto 200 metric 20")

	def := kroute{Table: 254, Prefix: netip.MustParsePrefix("0.0.0.0/0"), Metric: 20, Gateway: netip.MustParseAddr("10.0.1.1"), Interface: "wan1"}
	require.NoError(t, k.addRoute(def))
	owned, err = k.ownedRoutes()
	require.NoError(t, err)
	require.ElementsMatch(t, []kroute{moved, def}, owned)

	require.NoError(t, k.deleteRoute(moved))
	require.NoError(t, k.deleteRoute(moved), "deleting a missing route is not an error")
	require.NoError(t, k.deleteRoute(def))
	owned, err = k.ownedRoutes()
	require.NoError(t, err)
	require.Empty(t, owned)
}

func TestNetlinkKernelNeverTouchesForeignRoutes(t *testing.T) {
	if !inNetns(t) {
		return
	}
	k := newKernel(200)
	sh(t, "ip", "route", "add", "203.0.113.0/24", "via", "10.0.1.1", "metric", "20", "proto", "static")
	foreign := "203.0.113.0/24 via 10.0.1.1 dev wan1 proto static metric 20"
	r := kroute{Table: 254, Prefix: netip.MustParsePrefix("203.0.113.0/24"), Metric: 20, Gateway: netip.MustParseAddr("10.0.2.1"), Interface: "wan2"}

	var c *conflictError
	require.ErrorAs(t, k.addRoute(r), &c)
	require.Equal(t, 4, c.Protocol)
	require.ErrorAs(t, k.replaceRoute(r), &c)
	require.NoError(t, k.deleteRoute(r))
	require.Contains(t, sh(t, "ip", "route"), foreign)

	// A different metric is a different route: both coexist.
	r.Metric = 30
	require.NoError(t, k.addRoute(r))
	out := sh(t, "ip", "route")
	require.Contains(t, out, foreign)
	require.Contains(t, out, "203.0.113.0/24 via 10.0.2.1 dev wan2 proto 200 metric 30")
	owned, err := k.ownedRoutes()
	require.NoError(t, err)
	require.Equal(t, []kroute{r}, owned)
}

func TestNetlinkCheckerProbesTheGateway(t *testing.T) {
	if !inNetns(t) {
		return
	}
	c := newChecker()
	wan1 := oc.FibExitPath{Name: "wan1", Interface: "wan1", Gateway: netip.MustParseAddr("10.0.1.1"), Probe: oc.FIB_PROBE_ARP}
	linkUp, reachable, err := c.check(wan1, 500*time.Millisecond)
	require.NoError(t, err)
	require.True(t, linkUp)
	require.True(t, reachable)

	// The link stays up but the gateway stops answering.
	sh(t, "ip", "-n", "cpe", "addr", "del", "10.0.1.1/24", "dev", "c1")
	linkUp, reachable, err = c.check(wan1, 300*time.Millisecond)
	require.True(t, linkUp)
	require.False(t, reachable)
	require.ErrorContains(t, err, "no ARP reply")

	link := wan1
	link.Probe = oc.FIB_PROBE_LINK
	_, reachable, _ = c.check(link, time.Second)
	require.True(t, reachable, "the link probe ignores the gateway")

	// Losing the carrier takes the link down.
	sh(t, "ip", "-n", "cpe", "link", "set", "c1", "down")
	require.Eventually(t, func() bool { up, _, _ := c.check(link, time.Second); return !up }, 2*time.Second, 50*time.Millisecond)

	other := oc.FibExitPath{Name: "x", Interface: "wan2", Gateway: netip.MustParseAddr("10.9.9.9"), Probe: oc.FIB_PROBE_ARP}
	_, _, err = c.check(other, 200*time.Millisecond)
	require.ErrorContains(t, err, "no address in the subnet")
}

func TestMonitorFollowsRealLinks(t *testing.T) {
	if !inNetns(t) {
		return
	}
	cfg := oc.FibHealth{Interval: 100 * time.Millisecond, Failures: 2, Successes: 2, HoldDown: 300 * time.Millisecond, MaxHoldDown: time.Second}
	changes := make(chan pathChange, 16)
	m := newMonitor(cfg, []oc.FibExitPath{{Name: "wan1", Interface: "wan1", Gateway: netip.MustParseAddr("10.0.1.1"), Probe: oc.FIB_PROBE_ARP}}, newChecker(), func(c pathChange) { changes <- c })
	require.NoError(t, m.start())
	defer m.stop()
	next := func() pathChange {
		select {
		case c := <-changes:
			return c
		case <-time.After(5 * time.Second):
			t.Fatal("no path change")
		}
		return pathChange{}
	}
	require.True(t, next().Usable)

	start := time.Now()
	sh(t, "ip", "-n", "cpe", "link", "set", "c1", "down")
	c := next()
	require.False(t, c.Usable)
	require.Equal(t, reasonLinkDown, c.Reason)
	require.Less(t, time.Since(start), 90*time.Millisecond+cfg.Interval, "link events are handled without waiting for the next check")

	sh(t, "ip", "-n", "cpe", "link", "set", "c1", "up")
	require.True(t, next().Usable)
	require.True(t, m.usable("wan1"))
	require.True(t, strings.HasPrefix(m.reason("wan1"), "usable"))
}

func TestNetlinkKernelRules(t *testing.T) {
	if !inNetns(t) {
		return
	}
	k := newKernel(200)
	sh(t, "ip", "rule", "add", "priority", "1500", "from", "172.16.0.0/16", "lookup", "100")
	rules := []krule{
		{Priority: 1000, Table: oc.FIB_TABLE_MAIN, To: netip.MustParsePrefix("192.168.0.0/24")},
		{Priority: 1500, Table: 100, From: netip.MustParsePrefix("172.16.0.0/16")},
		{Priority: 1600, Table: 300, From: netip.MustParsePrefix("172.16.0.0/16"), To: netip.MustParsePrefix("203.0.113.0/24"), Iif: "wan1", Mark: 0x10, Mask: 0xff},
		{Priority: 1700, Table: 100, Mark: 7, Mask: 0xffffffff},
	}
	for _, r := range rules {
		require.NoError(t, k.addRule(r))
	}
	owned, foreign, err := k.listRules()
	require.NoError(t, err)
	require.Equal(t, rules, owned)
	require.True(t, foreign[1500])
	require.True(t, foreign[32766], "the kernel's main rule")

	for _, r := range rules {
		require.NoError(t, k.deleteRule(r))
	}
	require.NoError(t, k.deleteRule(rules[0]), "deleting a missing rule is not an error")
	owned, _, err = k.listRules()
	require.NoError(t, err)
	require.Empty(t, owned)
	require.Contains(t, sh(t, "ip", "rule"), "1500:\tfrom 172.16.0.0/16 lookup 100", "the foreign rule with the same selector stays")
}

func TestNetlinkKernelTakesOverAndRestoresAForeignRoute(t *testing.T) {
	if !inNetns(t) {
		return
	}
	k := newKernel(200)
	sh(t, "ip", "route", "add", "default", "via", "10.0.1.1", "metric", "20", "proto", "dhcp")
	r := kroute{Table: oc.FIB_TABLE_MAIN, Prefix: netip.MustParsePrefix("0.0.0.0/0"), Metric: 20, Gateway: netip.MustParseAddr("10.0.2.1"), Interface: "wan2"}
	restore, err := k.takeOverRoute(r)
	require.NoError(t, err)
	out := sh(t, "ip", "route", "show", "default")
	require.Equal(t, "default via 10.0.2.1 dev wan2 proto 200 metric 20 \n", out)
	require.NoError(t, restore())
	require.Equal(t, "default via 10.0.1.1 dev wan1 proto dhcp metric 20 \n", sh(t, "ip", "route", "show", "default"))
}
