package oc

import (
	"net/netip"
	"strings"
	"testing"

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
