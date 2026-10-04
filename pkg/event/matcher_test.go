package event

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMatcherSemantics(t *testing.T) {
	matcher, err := NewMatcher(MatchConfig{
		EventTypes: Filter[string]{Include: []string{"route", "withdraw"}},
		Families:   Filter[string]{Include: []string{"bgp-ls", "ipv4-unicast"}, Exclude: []string{"ipv4-unicast"}},
		Sources:    Filter[string]{Include: []string{"adj-in"}},
		Peers:      PeerMatch{Addresses: Filter[string]{Include: []string{"::ffff:192.0.2.1"}}, ASNs: Filter[uint32]{Include: []uint32{65001}, Exclude: []uint32{65002}}},
	})
	require.NoError(t, err)
	base := Metadata{EventType: "route", Family: "ls", Source: "adj-in", PeerAddress: "192.0.2.1", PeerASN: 65001}
	require.True(t, matcher.Match(base))
	for name, alter := range map[string]func(*Metadata){
		"excluded family": func(m *Metadata) { m.Family = "ipv4-unicast" }, "different source": func(m *Metadata) { m.Source = "best" },
		"excluded ASN": func(m *Metadata) { m.PeerASN = 65002 }, "missing peer": func(m *Metadata) { m.PeerAddress = "" },
		"missing family": func(m *Metadata) { m.Family = "" }, "peer state": func(m *Metadata) { m.EventType = "peer-state" },
	} {
		t.Run(name, func(t *testing.T) { v := base; alter(&v); require.False(t, matcher.Match(v)) })
	}
	all, err := NewMatcher(MatchConfig{})
	require.NoError(t, err)
	require.True(t, all.Match(Metadata{EventType: "peer-state", Source: "peer"}))
}

func TestMatcherRejectsUnsupportedFilters(t *testing.T) {
	for _, c := range []MatchConfig{
		{Sources: Filter[string]{Include: []string{"adj-out"}}},
		{Sources: Filter[string]{Include: []string{"local-rib"}}},
		{EventTypes: Filter[string]{Include: []string{"bgp-ls"}}},
		{Families: Filter[string]{Include: []string{"nonsense"}}},
		{Peers: PeerMatch{Addresses: Filter[string]{Include: []string{"not-an-ip"}}}},
		{Peers: PeerMatch{ASNs: Filter[uint32]{Include: []uint32{0}}}},
	} {
		_, err := NewMatcher(c)
		require.Error(t, err)
	}
}
