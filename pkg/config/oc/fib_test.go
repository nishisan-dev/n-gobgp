package oc

import (
	"net/netip"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

const fibTestConfig = `[global.config]
as = 65001
router-id = "192.0.2.254"

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

[[fib.rules]]
name = "customers"
priority = 10
table = 100
exit-paths = ["wan1", "wan2"]
[fib.rules.conditions.match-prefix-set]
prefix-set = "customers"

[[fib.rules]]
name = "rest"
priority = 20
table = "main"
exit-paths = ["wan2"]

[[fib.rules]]
name = "blackholed"
priority = 5
action = "ignore"
`

func readFibConfig(t *testing.T, text string) (*BgpConfigSet, error) {
	t.Helper()
	return ReadConfig(strings.NewReader(text), "toml")
}

func TestFibConfigDefaultsAndNormalization(t *testing.T) {
	c, err := readFibConfig(t, fibTestConfig)
	require.NoError(t, err)
	f := c.Fib
	require.True(t, f.Enabled)
	require.Equal(t, FIB_MODE_OBSERVE, f.Mode)
	require.Equal(t, FIB_DEFAULT_PROTOCOL, f.Protocol)
	require.Equal(t, netip.MustParseAddr("10.0.1.1"), f.ExitPaths[0].Gateway)

	require.Equal(t, "customers", f.Rules[0].Name)
	require.Equal(t, FIB_FAMILY_IPV4_UNICAST, f.Rules[0].Family)
	require.Equal(t, FIB_ACTION_INSTALL, f.Rules[0].Action)
	require.Equal(t, uint32(100), f.Rules[0].TableID)
	require.Equal(t, "customers", f.Rules[0].Conditions.MatchPrefixSet.PrefixSet)
	require.Equal(t, uint32(254), f.Rules[1].TableID)
	require.Equal(t, FIB_ACTION_IGNORE, f.Rules[2].Action)
}

const fibAnchorConfig = `
[[fib.anchors]]
name = "bgp-session"
prefix = "192.0.2.10/32"
exit-paths = ["wan1", "wan2"]
`

func TestFibConfigHealthAndAnchors(t *testing.T) {
	c, err := readFibConfig(t, strings.Replace(fibTestConfig, `enabled = true`, "enabled = true\nmode = \"enforce\"\nmetric = 50\n[fib.health]\ninterval = \"500ms\"\nhold-down = \"2s\"", 1)+fibAnchorConfig)
	require.NoError(t, err)
	f := c.Fib
	require.Equal(t, FIB_MODE_ENFORCE, f.Mode)
	require.Equal(t, uint32(50), f.Metric)
	require.Equal(t, 500*time.Millisecond, f.Health.Interval)
	require.Equal(t, 2*time.Second, f.Health.HoldDown)
	require.Equal(t, FIB_DEFAULT_FAILURES, f.Health.Failures)
	require.Equal(t, FIB_DEFAULT_SUCCESSES, f.Health.Successes)
	require.Equal(t, FIB_DEFAULT_MAX_HOLD_DOWN, f.Health.MaxHoldDown)
	require.Equal(t, FIB_DEFAULT_ANCHOR_WAIT, f.AnchorWait)
	require.Equal(t, FIB_PROBE_ARP, f.ExitPaths[0].Probe)
	require.Equal(t, netip.MustParsePrefix("192.0.2.10/32"), f.Anchors[0].Prefix)
	require.Equal(t, uint32(FIB_TABLE_MAIN), f.Anchors[0].TableID)

	c, err = readFibConfig(t, fibTestConfig)
	require.NoError(t, err)
	require.Equal(t, uint32(FIB_DEFAULT_METRIC), c.Fib.Metric)
	require.Equal(t, FIB_DEFAULT_INTERVAL, c.Fib.Health.Interval)
	require.Equal(t, FIB_DEFAULT_HOLD_DOWN, c.Fib.Health.HoldDown)
}

func TestFibConfigRejectsInvalidHealthAndAnchors(t *testing.T) {
	base := fibTestConfig + fibAnchorConfig
	cases := map[string]struct{ from, to, err string }{
		"unknown probe":         {`gateway = "10.0.1.1"`, "gateway = \"10.0.1.1\"\nprobe = \"icmp\"", "probe"},
		"anchor unknown path":   {"/32\"\nexit-paths = [\"wan1\", \"wan2\"]", "/32\"\nexit-paths = [\"wan9\"]", "wan9"},
		"anchor without paths":  {"/32\"\nexit-paths = [\"wan1\", \"wan2\"]", "/32\"\nexit-paths = []", "exit path"},
		"anchor not masked":     {`prefix = "192.0.2.10/32"`, `prefix = "192.0.2.10/24"`, "prefix"},
		"anchor ipv6":           {`prefix = "192.0.2.10/32"`, `prefix = "2001:db8::1/128"`, "prefix"},
		"anchor default":        {`prefix = "192.0.2.10/32"`, `prefix = "0.0.0.0/0"`, "prefix"},
		"anchor reserved table": {`prefix = "192.0.2.10/32"`, "prefix = \"192.0.2.10/32\"\ntable = 255", "table"},
		"short interval":        {`enabled = true`, "enabled = true\n[fib.health]\ninterval = \"10ms\"", "interval"},
		"max below hold":        {`enabled = true`, "enabled = true\n[fib.health]\nhold-down = \"10m\"\nmax-hold-down = \"1m\"", "hold-down"},
		"failures too high":     {`enabled = true`, "enabled = true\n[fib.health]\nfailures = 1000", "failures"},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			require.Contains(t, base, tc.from)
			_, err := readFibConfig(t, strings.Replace(base, tc.from, tc.to, 1))
			require.ErrorContains(t, err, tc.err)
		})
	}
	_, err := readFibConfig(t, base+fibAnchorConfig)
	require.ErrorContains(t, err, "duplicate")
}

const fibRoutingConfig = `
[fib]
enabled = true
protected-prefixes = ["192.168.0.0/24"]
rule-priority-min = 2000
rule-priority-max = 2099

[[fib.exit-paths]]
name = "wan1"
interface = "eth1"
gateway = "10.0.1.1"

[[fib.anchors]]
name = "session"
prefix = "192.0.2.10/32"
exit-paths = ["wan1"]

[[fib.traffic-rules]]
name = "lan"
priority = 2010
table = 100
from = "172.16.0.0/16"
iif = "eth0"
fwmark = "0x10/0xff"

[[fib.traffic-rules]]
name = "marked"
priority = 2020
table = "main"
to = "203.0.113.0/24"
fwmark = "7"

[fib.default-route]
mode = "table"
table = 100
exit-paths = ["wan1"]
[fib.default-route.conditions.bgp-conditions]
next-hop-in-list = ["192.0.2.1"]
`

func TestFibConfigRoutingSections(t *testing.T) {
	c, err := readFibConfig(t, fibRoutingConfig)
	require.NoError(t, err)
	f := c.Fib
	require.Equal(t, []netip.Prefix{netip.MustParsePrefix("192.168.0.0/24")}, f.ProtectedPrefixes)
	require.Equal(t, FIB_DEFAULT_EOR_TIMEOUT, f.Startup.EorTimeout)
	require.Equal(t, FIB_DEFAULT_MAX_RETENTION, f.Startup.MaxRetention)
	require.Equal(t, FIB_DEFAULT_RESYNC_INTERVAL, f.ResyncInterval)

	lan := f.TrafficRules[0]
	require.Equal(t, uint32(100), lan.TableID)
	require.Equal(t, netip.MustParsePrefix("172.16.0.0/16"), lan.From)
	require.Equal(t, uint32(0x10), lan.Mark)
	require.Equal(t, uint32(0xff), lan.Mask)
	marked := f.TrafficRules[1]
	require.Equal(t, uint32(FIB_TABLE_MAIN), marked.TableID)
	require.Equal(t, uint32(7), marked.Mark)
	require.Equal(t, uint32(0xffffffff), marked.Mask)

	require.Equal(t, FIB_DEFAULT_ROUTE_TABLE, f.DefaultRoute.Mode)
	require.Equal(t, uint32(100), f.DefaultRoute.TableID)
	require.Len(t, f.DefaultRoute.Conditions.BgpConditions.NextHopInList, 1)

	c, err = readFibConfig(t, fibTestConfig)
	require.NoError(t, err)
	require.Equal(t, FIB_DEFAULT_ROUTE_IGNORE, c.Fib.DefaultRoute.Mode)
	require.Equal(t, uint32(FIB_DEFAULT_RULE_PRIORITY_MIN), c.Fib.RulePriorityMin)
	require.Equal(t, uint32(FIB_DEFAULT_RULE_PRIORITY_MAX), c.Fib.RulePriorityMax)
}

func TestFibConfigRejectsInvalidRoutingSections(t *testing.T) {
	cases := map[string]struct{ from, to, err string }{
		"protected not masked":     {`"192.168.0.0/24"]`, `"192.168.0.1/24"]`, "protected"},
		"priority below exemption": {`priority = 2010`, `priority = 2001`, "2002"},
		"priority above range":     {`priority = 2010`, `priority = 2100`, "range"},
		"inverted range":           {`rule-priority-max = 2099`, `rule-priority-max = 1999`, "rule-priority"},
		"range reaches main rule":  {`rule-priority-max = 2099`, `rule-priority-max = 32766`, "rule-priority"},
		"duplicate rule priority":  {`priority = 2020`, `priority = 2010`, "2010"},
		"invalid fwmark":           {`fwmark = "7"`, `fwmark = "seven"`, "fwmark"},
		"invalid from":             {`from = "172.16.0.0/16"`, `from = "172.16.0.1/16"`, "from"},
		"traffic rule bad table":   {`table = "main"` + "\nto", `table = 0` + "\nto", "table"},
		"unknown default mode":     {`mode = "table"`, `mode = "always"`, "mode"},
		"default table is main":    {`mode = "table"` + "\ntable = 100", `mode = "table"` + "\ntable = \"main\"", "main"},
		"default main with table":  {`mode = "table"`, `mode = "main"`, "table"},
		"default without paths":    {`exit-paths = ["wan1"]` + "\n[fib.default-route.conditions", `exit-paths = []` + "\n[fib.default-route.conditions", "exit path"},
		"replace outside main":     {`mode = "table"`, "mode = \"table\"\nreplace-external = true", "replace-external"},
		"invalid startup":          {`rule-priority-max = 2099`, "rule-priority-max = 2099\n[fib.startup]\neor-timeout = \"-1s\"", "startup"},
		"invalid resync":           {`rule-priority-min = 2000`, "rule-priority-min = 2000\nresync-interval = \"10ms\"", "resync"},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			require.Contains(t, fibRoutingConfig, tc.from)
			_, err := readFibConfig(t, strings.Replace(fibRoutingConfig, tc.from, tc.to, 1))
			require.ErrorContains(t, err, tc.err)
		})
	}
}

func TestFibConfigYAML(t *testing.T) {
	c, err := ReadConfig(strings.NewReader(`fib:
  enabled: true
  exit-paths:
    - {name: wan1, interface: eth1, gateway: 10.0.1.1}
  rules:
    - name: all
      priority: 1
      table: 100
      exit-paths: [wan1]
      conditions:
        bgp-conditions:
          next-hop-in-list: [192.0.2.1]
`), "yaml")
	require.NoError(t, err)
	require.Equal(t, uint32(100), c.Fib.Rules[0].TableID)
	require.Equal(t, []netip.Addr{netip.MustParseAddr("192.0.2.1")}, c.Fib.Rules[0].Conditions.BgpConditions.NextHopInList)
}

func TestFibConfigDisabledIsNotValidated(t *testing.T) {
	_, err := readFibConfig(t, "[fib]\nenabled = false\nmode = \"bogus\"\n")
	require.NoError(t, err)
}

func TestFibConfigRejectsInvalidConfiguration(t *testing.T) {
	cases := map[string]struct{ from, to, err string }{
		"unknown mode":            {`enabled = true`, "enabled = true\nmode = \"install\"", "mode"},
		"reserved protocol":       {`enabled = true`, "enabled = true\nprotocol = 186", "protocol 186"},
		"kernel protocol":         {`enabled = true`, "enabled = true\nprotocol = 2", "protocol 2"},
		"protocol out of range":   {`enabled = true`, "enabled = true\nprotocol = 256", "protocol"},
		"duplicate priority":      {`priority = 20`, `priority = 10`, "priority 10"},
		"missing priority":        {`priority = 20`, ``, "priority"},
		"duplicate rule name":     {`name = "rest"`, `name = "customers"`, "duplicate"},
		"duplicate exit path":     {`name = "wan2"`, `name = "wan1"`, "duplicate"},
		"unknown exit path":       {`exit-paths = ["wan2"]`, `exit-paths = ["wan3"]`, "wan3"},
		"repeated exit path":      {`exit-paths = ["wan2"]`, `exit-paths = ["wan2", "wan2"]`, "wan2"},
		"install without paths":   {`exit-paths = ["wan2"]`, `exit-paths = []`, "exit path"},
		"install without table":   {`table = "main"`, ``, "table"},
		"reserved table":          {`table = "main"`, `table = 255`, "table"},
		"invalid table":           {`table = "main"`, `table = "wan"`, "table"},
		"ignore with table":       {`action = "ignore"`, "action = \"ignore\"\ntable = 100", "ignore"},
		"unknown action":          {`action = "ignore"`, `action = "drop"`, "action"},
		"unsupported family":      {`priority = 20`, "priority = 20\nfamily = \"ipv6-unicast\"", "family"},
		"ipv6 gateway":            {`gateway = "10.0.2.1"`, `gateway = "2001:db8::1"`, "gateway"},
		"missing gateway":         {`gateway = "10.0.2.1"`, ``, "gateway"},
		"missing interface":       {`interface = "eth2"`, ``, "interface"},
		"interface name too long": {`interface = "eth2"`, `interface = "a-very-long-ifname"`, "interface"},
		"invalid name":            {`name = "rest"`, `name = "bad name"`, "name"},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			require.Contains(t, fibTestConfig, tc.from)
			_, err := readFibConfig(t, strings.Replace(fibTestConfig, tc.from, tc.to, 1))
			require.Error(t, err)
			require.ErrorContains(t, err, tc.err)
		})
	}
}
