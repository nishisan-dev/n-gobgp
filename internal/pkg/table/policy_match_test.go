package table

import (
	"net/netip"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/osrg/gobgp/v4/pkg/config/oc"
	"github.com/osrg/gobgp/v4/pkg/packet/bgp"
)

func matchTestPath(t *testing.T, prefix string, communities ...uint32) *Path {
	t.Helper()
	nlri, err := bgp.NewIPAddrPrefix(netip.MustParsePrefix(prefix))
	require.NoError(t, err)
	nh, err := bgp.NewPathAttributeNextHop(netip.MustParseAddr("192.0.2.1"))
	require.NoError(t, err)
	attrs := []bgp.PathAttributeInterface{bgp.NewPathAttributeOrigin(0), nh}
	if len(communities) > 0 {
		attrs = append(attrs, bgp.NewPathAttributeCommunities(communities))
	}
	return NewPath(bgp.RF_IPv4_UC, &PeerInfo{AS: 65002, Address: netip.MustParseAddr("192.0.2.1")}, bgp.PathNLRI{NLRI: nlri}, false, attrs, time.Now(), false)
}

func TestNewMatchStatementBindsDefinedSets(t *testing.T) {
	sets := oc.DefinedSets{
		PrefixSets: []oc.PrefixSet{{PrefixSetName: "customers", PrefixList: []oc.Prefix{{IpPrefix: netip.MustParsePrefix("198.51.100.0/24"), MasklengthRange: "24..32"}}}},
		BgpDefinedSets: oc.BgpDefinedSets{
			CommunitySets: []oc.CommunitySet{{CommunitySetName: "wan1", CommunityList: []string{"65002:100"}}},
		},
	}
	conds := oc.Conditions{
		MatchPrefixSet: oc.MatchPrefixSet{PrefixSet: "customers"},
		BgpConditions:  oc.BgpConditions{MatchCommunitySet: oc.MatchCommunitySet{CommunitySet: "wan1"}},
	}
	s, err := NewMatchStatement("rule", sets, conds)
	require.NoError(t, err)

	require.True(t, s.Evaluate(matchTestPath(t, "198.51.100.0/25", 65002<<16|100), nil))
	require.False(t, s.Evaluate(matchTestPath(t, "198.51.100.0/25"), nil), "community must match")
	require.False(t, s.Evaluate(matchTestPath(t, "203.0.113.0/24", 65002<<16|100), nil), "prefix must match")
}

func TestNewMatchStatementWithoutConditionsMatchesEverything(t *testing.T) {
	s, err := NewMatchStatement("catch-all", oc.DefinedSets{}, oc.Conditions{})
	require.NoError(t, err)
	require.True(t, s.Evaluate(matchTestPath(t, "203.0.113.0/24"), nil))
}

func TestNewMatchStatementRejectsUnknownSet(t *testing.T) {
	_, err := NewMatchStatement("rule", oc.DefinedSets{}, oc.Conditions{MatchPrefixSet: oc.MatchPrefixSet{PrefixSet: "missing"}})
	require.ErrorContains(t, err, "missing")
}

func TestNewMatchStatementRejectsInvalidDefinedSets(t *testing.T) {
	sets := oc.DefinedSets{BgpDefinedSets: oc.BgpDefinedSets{CommunitySets: []oc.CommunitySet{{CommunitySetName: "bad", CommunityList: []string{"not-a-community("}}}}}
	_, err := NewMatchStatement("rule", sets, oc.Conditions{})
	require.Error(t, err)
}
