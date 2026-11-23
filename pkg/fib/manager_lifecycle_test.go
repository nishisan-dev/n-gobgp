package fib

import (
	"net/netip"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/osrg/gobgp/v4/pkg/config/oc"
	"github.com/osrg/gobgp/v4/pkg/packet/bgp"
	"github.com/osrg/gobgp/v4/pkg/server"
)

const testTraffic = `
[[fib.traffic-rules]]
name = "lan"
priority = 1010
table = 100
from = "172.16.0.0/16"
iif = "eth0"
fwmark = "0x10/0xff"
`

var peer1, peer2 = netip.MustParseAddr("192.0.2.1"), netip.MustParseAddr("192.0.2.2")

func peerEvent(m *Manager, addr netip.Addr, up, eor bool) {
	ev := &server.PeerEvent{Address: addr, Established: up || eor, EndOfRib: eor}
	if eor {
		ev.Family = bgp.RF_IPv4_UC
	}
	m.apply(server.BestPathEvent{Peer: ev})
}

func withStartup(text, startup string) string {
	return strings.Replace(text, "mode = \"enforce\"", "mode = \"enforce\"\n"+startup, 1)
}

var leftover = route(oc.FIB_TABLE_MAIN, "10.99.0.0/16", "10.0.1.1", "eth1")

func seedPreviousRun(k *fakeKernel) {
	k.seed(route(100, "198.51.100.0/24", "10.0.1.1", "eth1"), fakeProtocol)
	k.seed(leftover, fakeProtocol)
	k.seed(anchorViaW2, fakeProtocol)
}

func TestRestartKeepsInheritedRoutesUntilEndOfRib(t *testing.T) {
	k, ps := newFakeKernel(), newFakePaths("wan1", "wan2")
	seedPreviousRun(k)
	m := newTestManager(t, enforce(testSets+testFib+testAnchor), k, ps)
	require.Equal(t, []string{"replace " + anchorKey.String()}, k.takeOps(), "the inherited anchor is adopted and moved to the preferred path")
	require.False(t, m.Reconciled())

	peerEvent(m, peer1, true, false)
	peerEvent(m, peer2, true, false)
	announce(m, testPath(t, "198.51.100.0/24", pathSpec{}))
	require.Empty(t, k.takeOps(), "a relearned identical route is adopted without a change")

	peerEvent(m, peer1, false, true)
	require.False(t, m.Reconciled(), "every established peer must finish")
	peerEvent(m, peer2, false, true)
	require.True(t, m.Reconciled())
	require.Equal(t, []string{"delete " + leftover.key().String()}, k.takeOps())
	require.ElementsMatch(t, []kroute{anchorViaW1, route(100, "198.51.100.0/24", "10.0.1.1", "eth1")}, ownedRoutes(t, k))
}

func TestRestartReconcilesAfterEorTimeout(t *testing.T) {
	k, ps := newFakeKernel(), newFakePaths("wan1", "wan2")
	seedPreviousRun(k)
	m := newTestManager(t, withStartup(enforce(testSets+testFib+testAnchor), "[fib.startup]\neor-timeout = \"50ms\""), k, ps)
	time.Sleep(100 * time.Millisecond)
	require.False(t, m.Reconciled(), "the End-of-RIB timer starts with the first session")
	peerEvent(m, peer1, true, false)
	require.Eventually(t, m.Reconciled, 2*time.Second, 10*time.Millisecond)
	_, ok := k.get(leftover.key())
	require.False(t, ok)
}

func TestRestartReconcilesAfterMaxRetentionWithoutSession(t *testing.T) {
	k, ps := newFakeKernel(), newFakePaths("wan1", "wan2")
	seedPreviousRun(k)
	m := newTestManager(t, withStartup(enforce(testSets+testFib+testAnchor), "[fib.startup]\nmax-retention = \"50ms\""), k, ps)
	require.Eventually(t, m.Reconciled, 2*time.Second, 10*time.Millisecond)
	require.Equal(t, []kroute{anchorViaW1}, ownedRoutes(t, k), "BGP routes of the previous run are gone, the anchor stays")
}

func TestStartWithoutInheritedRoutesIsReconciled(t *testing.T) {
	m := newTestManager(t, enforce(testSets+testFib), newFakeKernel(), newFakePaths("wan1"))
	require.True(t, m.Reconciled())
}

func TestResyncRepairsTheKernel(t *testing.T) {
	k, ps := newFakeKernel(), newFakePaths("wan1", "wan2")
	m := newTestManager(t, enforce(testSets+testFib+testAnchor), k, ps)
	announce(m, testPath(t, "198.51.100.0/24", pathSpec{}))
	k.remove(customersKey)
	k.remove(anchorKey)
	stray := route(oc.FIB_TABLE_MAIN, "10.77.0.0/16", "10.0.1.1", "eth1")
	k.seed(stray, fakeProtocol)

	m.resync()
	require.ElementsMatch(t, []kroute{anchorViaW1, route(100, "198.51.100.0/24", "10.0.1.1", "eth1")}, ownedRoutes(t, k))
}

func TestTrafficRulesAndExemptions(t *testing.T) {
	k, ps := newFakeKernel(), newFakePaths("wan1", "wan2")
	stale := krule{Priority: 1500, Table: 100}
	k.seedRule(stale, fakeProtocol)
	text := strings.Replace(enforce(testSets+testFib+testAnchor+testTraffic), "mode = \"enforce\"", "mode = \"enforce\"\nprotected-prefixes = [\"192.168.0.0/24\"]", 1)
	m := newTestManager(t, text, k, ps)

	rules, _, err := k.listRules()
	require.NoError(t, err)
	require.Equal(t, []krule{
		{Priority: 1000, Table: oc.FIB_TABLE_MAIN, To: netip.MustParsePrefix("192.168.0.0/24")},
		{Priority: 1001, Table: oc.FIB_TABLE_MAIN, To: netip.MustParsePrefix("192.0.2.10/32")},
		{Priority: 1010, Table: 100, From: netip.MustParsePrefix("172.16.0.0/16"), Iif: "eth0", Mark: 0x10, Mask: 0xff},
	}, rules, "the stale rule is removed and exemptions come first")
	require.Equal(t, StatusInstalled, m.RuleStatus("lan"))
}

func TestTrafficRuleDoesNotShareAPriorityWithAForeignRule(t *testing.T) {
	k, ps := newFakeKernel(), newFakePaths("wan1")
	k.seedRule(krule{Priority: 1010, Table: 50}, 0)
	m := newTestManager(t, enforce(testSets+testFib+testAnchor+testTraffic), k, ps)
	require.Equal(t, StatusConflict, m.RuleStatus("lan"))
	rules, _, err := k.listRules()
	require.NoError(t, err)
	require.Len(t, rules, 1, "only the anchor exemption")
}

func TestDefaultRouteModes(t *testing.T) {
	def := netip.MustParsePrefix("0.0.0.0/0")
	defKey := routeKey{oc.FIB_TABLE_MAIN, def, oc.FIB_DEFAULT_METRIC}
	external := route(oc.FIB_TABLE_MAIN, "0.0.0.0/0", "10.0.9.1", "eth0")

	t.Run("table", func(t *testing.T) {
		k := newFakeKernel()
		m := newTestManager(t, enforce(testSets+testFib)+"[fib.default-route]\nmode = \"table\"\ntable = 300\nexit-paths = [\"wan2\", \"wan1\"]\n", k, newFakePaths("wan1", "wan2"))
		announce(m, testPath(t, "0.0.0.0/0", pathSpec{}))
		require.Equal(t, []kroute{route(300, "0.0.0.0/0", "10.0.2.1", "eth2")}, ownedRoutes(t, k))
	})
	t.Run("main coexists with a different metric", func(t *testing.T) {
		k := newFakeKernel()
		other := external
		other.Metric = 100
		k.seed(other, 16)
		m := newTestManager(t, enforce(testSets+testFib)+"[fib.default-route]\nmode = \"main\"\nexit-paths = [\"wan1\"]\n", k, newFakePaths("wan1"))
		announce(m, testPath(t, "0.0.0.0/0", pathSpec{}))
		require.Len(t, k.all(), 2)
		require.Equal(t, StatusInstalled, m.Status(def))
	})
	t.Run("main conflicts without authorization", func(t *testing.T) {
		k := newFakeKernel()
		k.seed(external, 4)
		m := newTestManager(t, enforce(testSets+testFib)+"[fib.default-route]\nmode = \"main\"\nexit-paths = [\"wan1\"]\n", k, newFakePaths("wan1"))
		announce(m, testPath(t, "0.0.0.0/0", pathSpec{}))
		r, _ := k.get(defKey)
		require.Equal(t, fakeRoute{external, 4}, r)
		require.Equal(t, StatusConflict, m.Status(def))
	})
	t.Run("main replaces and restores when authorized", func(t *testing.T) {
		k := newFakeKernel()
		k.seed(external, 4)
		m := newTestManager(t, enforce(testSets+testFib)+"[fib.default-route]\nmode = \"main\"\nexit-paths = [\"wan1\"]\nreplace-external = true\n", k, newFakePaths("wan1"))
		p := testPath(t, "0.0.0.0/0", pathSpec{})
		announce(m, p)
		require.Equal(t, []kroute{route(oc.FIB_TABLE_MAIN, "0.0.0.0/0", "10.0.1.1", "eth1")}, ownedRoutes(t, k))
		require.Equal(t, StatusReplaced, m.Status(def))

		announce(m, withdraw(t, p))
		r, _ := k.get(defKey)
		require.Equal(t, fakeRoute{external, 4}, r, "the original route is back")

		announce(m, p)
		m.cleanup()
		r, _ = k.get(defKey)
		require.Equal(t, fakeRoute{external, 4}, r, "and also after a clean stop")
	})
}
