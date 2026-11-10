package fib

import (
	"net/netip"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/osrg/gobgp/v4/internal/pkg/table"
	"github.com/osrg/gobgp/v4/pkg/config/oc"
	"github.com/osrg/gobgp/v4/pkg/packet/bgp"
)

const testSets = `
[[defined-sets.prefix-sets]]
prefix-set-name = "customers"
[[defined-sets.prefix-sets.prefix-list]]
ip-prefix = "198.51.100.0/24"
masklength-range = "24..32"

[[defined-sets.bgp-defined-sets.community-sets]]
community-set-name = "via-wan2"
community-list = ["65002:200"]

[[defined-sets.bgp-defined-sets.as-path-sets]]
as-path-set-name = "from-65010"
as-path-list = ["_65010$"]
`

const testFib = `
[fib]
enabled = true

[[fib.exit-paths]]
name = "wan1"
interface = "eth1"
gateway = "10.0.1.1"

[[fib.exit-paths]]
name = "wan2"
interface = "eth2"
gateway = "10.0.2.1"

# Listed first, but evaluated last.
[[fib.rules]]
name = "catch-all"
priority = 1000
table = "main"
exit-paths = ["wan1", "wan2"]

[[fib.rules]]
name = "community-wan2"
priority = 10
table = 100
exit-paths = ["wan2", "wan1"]
[fib.rules.conditions.bgp-conditions.match-community-set]
community-set = "via-wan2"

[[fib.rules]]
name = "customers"
priority = 20
table = 100
exit-paths = ["wan1"]
[fib.rules.conditions.match-prefix-set]
prefix-set = "customers"

[[fib.rules]]
name = "skip-65010"
priority = 15
action = "ignore"
[fib.rules.conditions.bgp-conditions.match-as-path-set]
as-path-set = "from-65010"

[[fib.rules]]
name = "next-hop"
priority = 30
table = 200
exit-paths = ["wan2"]
[fib.rules.conditions.bgp-conditions]
next-hop-in-list = ["192.0.2.99"]
`

func testConfig(t *testing.T, text string) *oc.BgpConfigSet {
	t.Helper()
	c, err := oc.ReadConfig(strings.NewReader("[global.config]\nas = 65001\nrouter-id = \"192.0.2.254\"\n"+text), "toml")
	require.NoError(t, err)
	return c
}

type pathSpec struct {
	nexthop     string
	asPath      []uint32
	communities []uint32
}

func testPath(t *testing.T, prefix string, spec pathSpec) *table.Path {
	t.Helper()
	nlri, err := bgp.NewIPAddrPrefix(netip.MustParsePrefix(prefix))
	require.NoError(t, err)
	if spec.nexthop == "" {
		spec.nexthop = "192.0.2.1"
	}
	nh, err := bgp.NewPathAttributeNextHop(netip.MustParseAddr(spec.nexthop))
	require.NoError(t, err)
	if spec.asPath == nil {
		spec.asPath = []uint32{65002}
	}
	attrs := []bgp.PathAttributeInterface{
		bgp.NewPathAttributeOrigin(0), nh,
		bgp.NewPathAttributeAsPath([]bgp.AsPathParamInterface{bgp.NewAs4PathParam(bgp.BGP_ASPATH_ATTR_TYPE_SEQ, spec.asPath)}),
	}
	if len(spec.communities) > 0 {
		attrs = append(attrs, bgp.NewPathAttributeCommunities(spec.communities))
	}
	return table.NewPath(bgp.RF_IPv4_UC, &table.PeerInfo{AS: 65002, Address: netip.MustParseAddr("192.0.2.1")}, bgp.PathNLRI{NLRI: nlri}, false, attrs, time.Now(), false)
}

func TestEngineSelectsLowestPriorityMatchingRule(t *testing.T) {
	e, err := NewEngine(testConfig(t, testSets+testFib))
	require.NoError(t, err)

	cases := []struct {
		name   string
		prefix string
		spec   pathSpec
		want   Decision
	}{
		{"community beats prefix", "198.51.100.0/24", pathSpec{communities: []uint32{65002<<16 | 200}},
			Decision{Action: ActionInstall, Rule: "community-wan2", Table: 100, ExitPaths: []string{"wan2", "wan1"}}},
		{"prefix set", "198.51.100.128/25", pathSpec{},
			Decision{Action: ActionInstall, Rule: "customers", Table: 100, ExitPaths: []string{"wan1"}}},
		{"ignore rule beats prefix", "198.51.100.0/24", pathSpec{asPath: []uint32{65002, 65010}},
			Decision{Action: ActionIgnore, Rule: "skip-65010", Reason: ReasonRule}},
		{"next hop", "203.0.113.0/24", pathSpec{nexthop: "192.0.2.99"},
			Decision{Action: ActionInstall, Rule: "next-hop", Table: 200, ExitPaths: []string{"wan2"}}},
		{"catch-all", "203.0.113.0/24", pathSpec{},
			Decision{Action: ActionInstall, Rule: "catch-all", Table: oc.FIB_TABLE_MAIN, ExitPaths: []string{"wan1", "wan2"}}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			tc.want.Prefix = netip.MustParsePrefix(tc.prefix)
			if tc.want.Action == ActionInstall {
				tc.want.Reason = ReasonRule
			}
			require.Equal(t, tc.want, e.Evaluate(testPath(t, tc.prefix, tc.spec)))
		})
	}
}

func TestEngineIgnoresUnmatchedPaths(t *testing.T) {
	restricted := `exit-paths = ["wan1", "wan2"]` + "\n[fib.rules.conditions.match-prefix-set]\nprefix-set = \"customers\""
	e, err := NewEngine(testConfig(t, testSets+strings.Replace(testFib, `exit-paths = ["wan1", "wan2"]`, restricted, 1)))
	require.NoError(t, err)
	require.Equal(t, Decision{Prefix: netip.MustParsePrefix("203.0.113.0/24"), Action: ActionIgnore, Reason: ReasonNoMatch},
		e.Evaluate(testPath(t, "203.0.113.0/24", pathSpec{})))
}

func TestEngineNeverMatchesTheDefaultRouteWithGenericRules(t *testing.T) {
	e, err := NewEngine(testConfig(t, testSets+testFib))
	require.NoError(t, err)
	require.Equal(t, Decision{Prefix: netip.MustParsePrefix("0.0.0.0/0"), Action: ActionIgnore, Reason: ReasonDefaultRoute},
		e.Evaluate(testPath(t, "0.0.0.0/0", pathSpec{})))
}

func TestEngineInstallsTheDefaultRouteOnlyThroughItsOwnSection(t *testing.T) {
	defaultRoute := `
[fib.default-route]
mode = "table"
table = 300
exit-paths = ["wan2"]
[fib.default-route.conditions.bgp-conditions.match-community-set]
community-set = "via-wan2"
`
	e, err := NewEngine(testConfig(t, testSets+testFib+defaultRoute))
	require.NoError(t, err)
	def := netip.MustParsePrefix("0.0.0.0/0")
	require.Equal(t, Decision{Prefix: def, Action: ActionInstall, Rule: DefaultRouteRule, Reason: ReasonRule, Table: 300, ExitPaths: []string{"wan2"}},
		e.Evaluate(testPath(t, "0.0.0.0/0", pathSpec{communities: []uint32{65002<<16 | 200}})))
	require.Equal(t, Decision{Prefix: def, Action: ActionIgnore, Reason: ReasonDefaultNoMatch},
		e.Evaluate(testPath(t, "0.0.0.0/0", pathSpec{})))

	e, err = NewEngine(testConfig(t, testSets+testFib+"[fib.default-route]\nmode = \"main\"\nexit-paths = [\"wan1\", \"wan2\"]\n"))
	require.NoError(t, err)
	require.Equal(t, Decision{Prefix: def, Action: ActionInstall, Rule: DefaultRouteRule, Reason: ReasonRule, Table: oc.FIB_TABLE_MAIN, ExitPaths: []string{"wan1", "wan2"}},
		e.Evaluate(testPath(t, "0.0.0.0/0", pathSpec{})))

	_, err = NewEngine(testConfig(t, testSets+testFib+"[fib.default-route]\nmode = \"main\"\nexit-paths = [\"wan1\"]\n[fib.default-route.conditions]\ncall-policy = \"x\"\n"))
	require.ErrorContains(t, err, "default-route")
}

func TestEngineRejectsUnresolvableOrUnsupportedConditions(t *testing.T) {
	cases := map[string]struct{ conditions, err string }{
		"unknown set":    {"[fib.rules.conditions.match-prefix-set]\nprefix-set = \"missing\"", "missing"},
		"call policy":    {"[fib.rules.conditions]\ncall-policy = \"p\"", "call-policy"},
		"tag set":        {"[fib.rules.conditions.match-tag-set]\ntag-set = \"t\"", "tag-set"},
		"protocol":       {"[fib.rules.conditions]\ninstall-protocol-eq = \"bgp\"", "install-protocol-eq"},
		"rpki":           {"[fib.rules.conditions.bgp-conditions]\nrpki-validation-result = \"valid\"", "rpki"},
		"other families": {"[fib.rules.conditions.bgp-conditions]\nafi-safi-in-list = [\"ipv6-unicast\"]", "afi-safi-in-list"},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			text := "[fib]\nenabled = true\n[[fib.exit-paths]]\nname = \"wan1\"\ninterface = \"eth1\"\ngateway = \"10.0.1.1\"\n" +
				"[[fib.rules]]\nname = \"r\"\npriority = 1\ntable = 100\nexit-paths = [\"wan1\"]\n" + tc.conditions
			_, err := NewEngine(testConfig(t, text))
			require.ErrorContains(t, err, tc.err)
		})
	}
}

func TestValidateWarnsAboutGracefulRestartAndZebraInMain(t *testing.T) {
	text := testSets + testFib + `
[zebra.config]
enabled = true
url = "unix:/var/run/frr/zserv.api"
version = 6

[[neighbors]]
[neighbors.config]
neighbor-address = "192.0.2.1"
peer-as = 65002
[neighbors.graceful-restart.config]
enabled = true
`
	warnings, err := Validate(testConfig(t, text))
	require.NoError(t, err)
	require.Len(t, warnings, 2)
	require.Contains(t, warnings[0], "192.0.2.1")
	require.Contains(t, warnings[0], "graceful restart")
	require.Contains(t, warnings[1], "zebra")
	require.Contains(t, warnings[1], "catch-all")

	warnings, err = Validate(testConfig(t, testSets+testFib))
	require.NoError(t, err)
	require.Empty(t, warnings)
}

func TestValidateIgnoresDisabledFib(t *testing.T) {
	warnings, err := Validate(testConfig(t, "[fib]\nenabled = false\n"))
	require.NoError(t, err)
	require.Empty(t, warnings)
}
