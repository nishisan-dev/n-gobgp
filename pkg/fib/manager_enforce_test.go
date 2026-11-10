package fib

import (
	"log/slog"
	"net/netip"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/osrg/gobgp/v4/internal/pkg/table"
	"github.com/osrg/gobgp/v4/pkg/config/oc"
	"github.com/osrg/gobgp/v4/pkg/packet/bgp"
	"github.com/osrg/gobgp/v4/pkg/server"
)

const testAnchor = `
[[fib.anchors]]
name = "session"
prefix = "192.0.2.10/32"
exit-paths = ["wan1", "wan2"]
`

func enforce(text string) string {
	return strings.Replace(text, "[fib]\nenabled = true", "[fib]\nenabled = true\nmode = \"enforce\"", 1)
}

func newTestManager(t *testing.T, text string, k *fakeKernel, ps *fakePaths) *Manager {
	t.Helper()
	m, err := newManager(testConfig(t, text), slog.New(slog.DiscardHandler), k, ps)
	require.NoError(t, err)
	require.NoError(t, m.begin())
	t.Cleanup(m.stopTimers)
	return m
}

func route(table uint32, prefix, gw, ifname string) kroute {
	return kroute{Table: table, Prefix: netip.MustParsePrefix(prefix), Metric: oc.FIB_DEFAULT_METRIC, Gateway: netip.MustParseAddr(gw), Interface: ifname}
}

func ownedRoutes(t *testing.T, k *fakeKernel) []kroute {
	t.Helper()
	l, err := k.ownedRoutes()
	require.NoError(t, err)
	return l
}

func announce(m *Manager, paths ...*table.Path) {
	m.apply(server.BestPathEvent{Paths: paths})
}

func withdraw(t *testing.T, p *table.Path) *table.Path {
	t.Helper()
	return p.Clone(true)
}

func pathDown(m *Manager, ps *fakePaths, name string, up bool) {
	ps.set(name, up)
	m.onPathChange(pathChange{Name: name, Usable: up})
}

var (
	anchorKey    = routeKey{oc.FIB_TABLE_MAIN, netip.MustParsePrefix("192.0.2.10/32"), oc.FIB_DEFAULT_METRIC}
	anchorViaW1  = route(oc.FIB_TABLE_MAIN, "192.0.2.10/32", "10.0.1.1", "eth1")
	anchorViaW2  = route(oc.FIB_TABLE_MAIN, "192.0.2.10/32", "10.0.2.1", "eth2")
	customersKey = routeKey{100, netip.MustParsePrefix("198.51.100.0/24"), oc.FIB_DEFAULT_METRIC}
)

func TestAnchorsFollowTheirExitPaths(t *testing.T) {
	k, ps := newFakeKernel(), newFakePaths("wan1", "wan2")
	m := newTestManager(t, enforce(testSets+testFib+testAnchor), k, ps)
	require.Equal(t, []kroute{anchorViaW1}, ownedRoutes(t, k))
	k.takeOps()

	pathDown(m, ps, "wan1", false)
	require.Equal(t, []kroute{anchorViaW2}, ownedRoutes(t, k))
	require.Equal(t, []string{"replace " + anchorKey.String()}, k.takeOps(), "same key: replaced in place")

	pathDown(m, ps, "wan2", false)
	require.Equal(t, []kroute{anchorViaW2}, ownedRoutes(t, k), "with no usable path, the anchor stays on its last active path")
	require.Equal(t, StatusLastActive, m.AnchorStatus("session"))

	pathDown(m, ps, "wan1", true)
	require.Equal(t, []kroute{anchorViaW1}, ownedRoutes(t, k), "the preferred path is used again")
	require.Equal(t, StatusInstalled, m.AnchorStatus("session"))
}

func TestAnchorWithoutAnyUsablePathIsNotInstalled(t *testing.T) {
	k, ps := newFakeKernel(), newFakePaths()
	m := newTestManager(t, enforce(testSets+testFib+testAnchor), k, ps)
	require.Empty(t, ownedRoutes(t, k))
	require.Equal(t, StatusNoPath, m.AnchorStatus("session"))
	pathDown(m, ps, "wan2", true)
	require.Equal(t, []kroute{anchorViaW2}, ownedRoutes(t, k))
}

func TestRoutesFollowDecisionsAndExitPaths(t *testing.T) {
	k, ps := newFakeKernel(), newFakePaths("wan1", "wan2")
	m := newTestManager(t, enforce(testSets+testFib), k, ps)

	customers := testPath(t, "198.51.100.0/24", pathSpec{})
	tagged := testPath(t, "203.0.113.0/24", pathSpec{communities: []uint32{65002<<16 | 200}})
	announce(m, customers, tagged)
	require.ElementsMatch(t, []kroute{
		route(100, "198.51.100.0/24", "10.0.1.1", "eth1"),
		route(100, "203.0.113.0/24", "10.0.2.1", "eth2"),
	}, ownedRoutes(t, k))
	require.Equal(t, StatusInstalled, m.Status(customers.GetNlri().(*bgp.IPAddrPrefix).Prefix))

	pathDown(m, ps, "wan2", false)
	require.ElementsMatch(t, []kroute{
		route(100, "198.51.100.0/24", "10.0.1.1", "eth1"),
		route(100, "203.0.113.0/24", "10.0.1.1", "eth1"),
	}, ownedRoutes(t, k), "the next exit path in the rule takes over")

	pathDown(m, ps, "wan1", false)
	require.Empty(t, ownedRoutes(t, k), "without a usable exit path, routes are withdrawn from the kernel")
	require.Equal(t, StatusNoPath, m.Status(netip.MustParsePrefix("198.51.100.0/24")))

	pathDown(m, ps, "wan1", true)
	require.Len(t, ownedRoutes(t, k), 2)

	announce(m, withdraw(t, customers))
	require.Equal(t, []kroute{route(100, "203.0.113.0/24", "10.0.1.1", "eth1")}, ownedRoutes(t, k), "a BGP withdrawal removes the route at once")

	stale := tagged.Clone(false)
	stale.MarkStale(true)
	announce(m, stale)
	require.Empty(t, ownedRoutes(t, k), "stale paths are not forwarded")
}

func TestRouteMovesBeforeTheOldOneIsRemoved(t *testing.T) {
	k, ps := newFakeKernel(), newFakePaths("wan1", "wan2")
	m := newTestManager(t, enforce(testSets+testFib), k, ps)
	announce(m, testPath(t, "203.0.113.0/24", pathSpec{}))
	require.Equal(t, []kroute{route(oc.FIB_TABLE_MAIN, "203.0.113.0/24", "10.0.1.1", "eth1")}, ownedRoutes(t, k))
	k.takeOps()

	announce(m, testPath(t, "203.0.113.0/24", pathSpec{communities: []uint32{65002<<16 | 200}}))
	require.Equal(t, []kroute{route(100, "203.0.113.0/24", "10.0.2.1", "eth2")}, ownedRoutes(t, k))
	require.Equal(t, []string{
		"add 203.0.113.0/24 table 100 metric 20",
		"delete 203.0.113.0/24 table 254 metric 20",
	}, k.takeOps())

	announce(m, testPath(t, "203.0.113.0/24", pathSpec{asPath: []uint32{65002, 65010}}))
	require.Empty(t, ownedRoutes(t, k), "an ignore rule removes the route")
}

func TestConflictsLeaveForeignRoutesAlone(t *testing.T) {
	k, ps := newFakeKernel(), newFakePaths("wan1", "wan2")
	foreign := route(100, "198.51.100.0/24", "10.0.9.1", "eth9")
	k.seed(foreign, 4)
	m := newTestManager(t, enforce(testSets+testFib), k, ps)

	announce(m, testPath(t, "198.51.100.0/24", pathSpec{}))
	r, _ := k.get(customersKey)
	require.Equal(t, fakeRoute{foreign, 4}, r)
	require.Equal(t, StatusConflict, m.Status(customersKey.Prefix))

	// Path changes do not overwrite the foreign route either.
	pathDown(m, ps, "wan2", false)
	r, _ = k.get(customersKey)
	require.Equal(t, fakeRoute{foreign, 4}, r)

	// Once the other owner removes it, the next resync installs ours.
	k.remove(customersKey)
	m.resync()
	require.Equal(t, []kroute{route(100, "198.51.100.0/24", "10.0.1.1", "eth1")}, ownedRoutes(t, k))
	require.Equal(t, StatusInstalled, m.Status(customersKey.Prefix))
}

func TestOwnedRouteTakenByAnotherOwnerIsNotOverwritten(t *testing.T) {
	k, ps := newFakeKernel(), newFakePaths("wan1", "wan2")
	m := newTestManager(t, enforce(testSets+testFib), k, ps)
	announce(m, testPath(t, "198.51.100.0/24", pathSpec{}))
	foreign := route(100, "198.51.100.0/24", "10.0.9.1", "eth9")
	k.seed(foreign, 4)

	pathDown(m, ps, "wan1", false)
	r, _ := k.get(customersKey)
	require.Equal(t, fakeRoute{foreign, 4}, r)
	require.Equal(t, StatusNoPath, m.Status(customersKey.Prefix))
	pathDown(m, ps, "wan1", true)
	r, _ = k.get(customersKey)
	require.Equal(t, fakeRoute{foreign, 4}, r)
	require.Equal(t, StatusConflict, m.Status(customersKey.Prefix))
}

func TestProtectedPrefixesAndAnchorsAreNotOverriddenByBGP(t *testing.T) {
	k, ps := newFakeKernel(), newFakePaths("wan1", "wan2")
	text := strings.Replace(enforce(testSets+testFib+testAnchor), "mode = \"enforce\"", "mode = \"enforce\"\nprotected-prefixes = [\"203.0.113.0/24\"]", 1)
	m := newTestManager(t, text, k, ps)
	k.takeOps()

	announce(m,
		testPath(t, "203.0.113.128/25", pathSpec{}),                                     // catch-all, main: inside a protected prefix
		testPath(t, "203.0.0.0/16", pathSpec{}),                                         // less specific: allowed
		testPath(t, "192.0.2.10/32", pathSpec{}),                                        // the anchor itself, same table
		testPath(t, "203.0.113.0/24", pathSpec{communities: []uint32{65002<<16 | 200}}), // table 100: allowed
	)
	require.Equal(t, StatusProtected, m.Status(netip.MustParsePrefix("203.0.113.128/25")))
	require.Equal(t, StatusProtected, m.Status(netip.MustParsePrefix("192.0.2.10/32")))
	require.Equal(t, []string{"add 203.0.0.0/16 table 254 metric 20", "add 203.0.113.0/24 table 100 metric 20"}, k.takeOps())
	r, _ := k.get(anchorKey)
	require.Equal(t, anchorViaW1, r.r)
}

func TestSessionLossRemovesBGPRoutesButKeepsAnchors(t *testing.T) {
	k, ps := newFakeKernel(), newFakePaths("wan1", "wan2")
	m := newTestManager(t, enforce(testSets+testFib+testAnchor), k, ps)
	p1, p2 := testPath(t, "198.51.100.0/24", pathSpec{}), testPath(t, "203.0.113.0/24", pathSpec{})
	announce(m, p1, p2)
	require.Len(t, ownedRoutes(t, k), 3)
	// Without graceful restart, a session loss withdraws every path of the peer.
	m.apply(server.BestPathEvent{Peer: &server.PeerEvent{Address: netip.MustParseAddr("192.0.2.1")}})
	announce(m, withdraw(t, p1), withdraw(t, p2))
	require.Equal(t, []kroute{anchorViaW1}, ownedRoutes(t, k))
}

func TestObserveModeNeverTouchesTheKernel(t *testing.T) {
	k, ps := newFakeKernel(), newFakePaths("wan1", "wan2")
	m := newTestManager(t, testSets+testFib+testAnchor, k, ps)
	announce(m, testPath(t, "198.51.100.0/24", pathSpec{}))
	pathDown(m, ps, "wan1", false)
	m.resync()
	m.cleanup()
	require.Empty(t, k.all())
	require.Empty(t, k.takeOps())
	require.Equal(t, StatusObserved, m.Status(customersKey.Prefix))
}

func TestCloseRemovesOnlyOwnedObjects(t *testing.T) {
	k, ps := newFakeKernel(), newFakePaths("wan1", "wan2")
	foreign := route(oc.FIB_TABLE_MAIN, "10.10.0.0/16", "10.0.1.1", "eth1")
	k.seed(foreign, 3)
	k.seedRule(krule{Priority: 500, Table: 100}, 0)
	m := newTestManager(t, enforce(testSets+testFib+testAnchor+testTraffic), k, ps)
	announce(m, testPath(t, "198.51.100.0/24", pathSpec{}))
	require.Len(t, ownedRoutes(t, k), 2)
	k.takeOps()

	m.cleanup()
	require.Equal(t, map[routeKey]fakeRoute{foreign.key(): {foreign, 3}}, k.all())
	require.Equal(t, []fakeRule{{krule{Priority: 500, Table: 100}, 0}}, k.allRules())
	ops := k.takeOps()
	require.Equal(t, "delete "+customersKey.String(), ops[0], "BGP routes go first")
	require.Equal(t, "delete "+anchorKey.String(), ops[len(ops)-1], "anchors go last")
}

func TestRouteChangesAreLoggedReadably(t *testing.T) {
	k, ps := newFakeKernel(), newFakePaths("wan2")
	logs := &syncBuffer{}
	m, err := newManager(testConfig(t, enforce(testSets+testFib+testAnchor)), slog.New(slog.NewTextHandler(logs, nil)), k, ps)
	require.NoError(t, err)
	require.NoError(t, m.begin())
	pathDown(m, ps, "wan1", true)
	out := logs.String()
	require.Contains(t, out, `msg="fib anchor" Topic=Fib Anchor=session Route="192.0.2.10/32 via 10.0.2.1 dev eth2 table 254 metric 20" Status=installed`)
	require.Contains(t, out, `msg="fib anchor" Topic=Fib Anchor=session Route="192.0.2.10/32 via 10.0.1.1 dev eth1 table 254 metric 20" Status=installed`)
}

func TestPathChangesBeforeStartDoNotTouchTheKernel(t *testing.T) {
	k, ps := newFakeKernel(), newFakePaths("wan1")
	m, err := newManager(testConfig(t, enforce(testSets+testFib+testAnchor)), slog.New(slog.DiscardHandler), k, ps)
	require.NoError(t, err)
	m.onPathChange(pathChange{Name: "wan1", Usable: true})
	require.Empty(t, k.all())
}
