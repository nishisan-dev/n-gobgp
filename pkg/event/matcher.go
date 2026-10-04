package event

import (
	"fmt"
	"net/netip"
	"slices"

	"github.com/osrg/gobgp/v4/pkg/packet/bgp"
)

type Filter[T comparable] struct {
	Include []T `mapstructure:"include"`
	Exclude []T `mapstructure:"exclude"`
}

type PeerMatch struct {
	Addresses Filter[string] `mapstructure:"addresses"`
	ASNs      Filter[uint32] `mapstructure:"asns"`
}

type MatchConfig struct {
	EventTypes Filter[string] `mapstructure:"eventTypes"`
	Families   Filter[string] `mapstructure:"families"`
	Sources    Filter[string] `mapstructure:"sources"`
	Peers      PeerMatch      `mapstructure:"peers"`
}

type Matcher struct{ config MatchConfig }

func matches[T comparable](f Filter[T], value T) bool {
	return (len(f.Include) == 0 || slices.Contains(f.Include, value)) && !slices.Contains(f.Exclude, value)
}

func NewMatcher(c MatchConfig) (*Matcher, error) {
	normalize := func(f Filter[string], fn func(string) (string, error)) (Filter[string], error) {
		out := Filter[string]{}
		for _, pair := range []struct {
			src []string
			dst *[]string
		}{{f.Include, &out.Include}, {f.Exclude, &out.Exclude}} {
			for _, value := range pair.src {
				v, err := fn(value)
				if err != nil {
					return out, err
				}
				*pair.dst = append(*pair.dst, v)
			}
		}
		return out, nil
	}
	enum := func(values []string) func(string) (string, error) {
		return func(v string) (string, error) {
			if !slices.Contains(values, v) {
				return "", fmt.Errorf("unsupported filter value %q (supported: %v)", v, values)
			}
			return v, nil
		}
	}
	var err error
	if c.EventTypes, err = normalize(c.EventTypes, enum(Types)); err != nil {
		return nil, err
	}
	if c.Sources, err = normalize(c.Sources, enum(Sources)); err != nil {
		return nil, err
	}
	if c.Families, err = normalize(c.Families, func(v string) (string, error) {
		if v == "bgp-ls" {
			v = "ls"
		}
		family, err := bgp.GetFamily(v)
		if err != nil {
			return "", err
		}
		return family.String(), nil
	}); err != nil {
		return nil, err
	}
	if c.Peers.Addresses, err = normalize(c.Peers.Addresses, func(v string) (string, error) {
		a, err := netip.ParseAddr(v)
		if err != nil {
			return "", fmt.Errorf("invalid peer address %q: %w", v, err)
		}
		return a.Unmap().String(), nil
	}); err != nil {
		return nil, err
	}
	for _, asn := range append(slices.Clone(c.Peers.ASNs.Include), c.Peers.ASNs.Exclude...) {
		if asn == 0 {
			return nil, fmt.Errorf("peer ASN must be greater than zero")
		}
	}
	return &Matcher{config: c}, nil
}

func (m *Matcher) AllowsSource(source string) bool { return matches(m.config.Sources, source) }
func (m *Matcher) AllowsFamily(family string) bool { return matches(m.config.Families, family) }
func (m *Matcher) Match(v Metadata) bool {
	c := m.config
	return matches(c.EventTypes, v.EventType) && matches(c.Families, v.Family) &&
		matches(c.Sources, v.Source) && matches(c.Peers.Addresses, v.PeerAddress) && matches(c.Peers.ASNs, v.PeerASN)
}
