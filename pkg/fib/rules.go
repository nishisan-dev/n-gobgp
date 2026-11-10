// Package fib derives Linux forwarding decisions from the global RIB best
// paths, according to rules that match BGP attributes.
package fib

import (
	"fmt"
	"net/netip"
	"slices"
	"sort"

	"github.com/osrg/gobgp/v4/internal/pkg/table"
	"github.com/osrg/gobgp/v4/pkg/config/oc"
	"github.com/osrg/gobgp/v4/pkg/packet/bgp"
)

const (
	ActionInstall = oc.FIB_ACTION_INSTALL
	ActionIgnore  = oc.FIB_ACTION_IGNORE

	ReasonRule           = "rule"
	ReasonNoMatch        = "no matching rule"
	ReasonDefaultRoute   = "default route"
	ReasonDefaultNoMatch = "default route conditions not matched"

	// DefaultRouteRule names decisions taken by the default-route section.
	DefaultRouteRule = "default-route"
)

// Decision is the forwarding outcome for one destination. ExitPaths are in
// order of preference.
type Decision struct {
	Prefix    netip.Prefix
	Action    string
	Rule      string
	Table     uint32
	ExitPaths []string
	Reason    string
}

func (d Decision) equal(o Decision) bool {
	return d.Prefix == o.Prefix && d.Action == o.Action && d.Rule == o.Rule && d.Table == o.Table &&
		d.Reason == o.Reason && slices.Equal(d.ExitPaths, o.ExitPaths)
}

type rule struct {
	config    oc.FibRule
	statement *table.Statement
}

// Engine evaluates the configured rules. It is immutable after construction.
type Engine struct {
	rules        []rule
	defaultRoute oc.FibDefaultRoute
	// defaultMatch is nil when the default route is ignored.
	defaultMatch *table.Statement
}

func unsupportedConditions(c oc.Conditions) error {
	switch {
	case c.CallPolicy != "":
		return fmt.Errorf("call-policy is not supported")
	case c.MatchTagSet.TagSet != "":
		return fmt.Errorf("match-tag-set is not supported")
	case c.InstallProtocolEq != "":
		return fmt.Errorf("install-protocol-eq is not supported")
	case c.BgpConditions.RpkiValidationResult != "" && c.BgpConditions.RpkiValidationResult != oc.RPKI_VALIDATION_RESULT_TYPE_NONE:
		return fmt.Errorf("rpki-validation-result is not supported")
	case len(c.BgpConditions.AfiSafiInList) > 0:
		return fmt.Errorf("afi-safi-in-list is not supported; use the rule family")
	}
	return nil
}

// NewEngine builds the rules of an enabled, normalized configuration and
// resolves their references to the defined sets.
func NewEngine(c *oc.BgpConfigSet) (*Engine, error) {
	e := &Engine{}
	for _, r := range c.Fib.Rules {
		if err := unsupportedConditions(r.Conditions); err != nil {
			return nil, fmt.Errorf("fib rule %s: %w", r.Name, err)
		}
		s, err := table.NewMatchStatement(r.Name, c.DefinedSets, r.Conditions)
		if err != nil {
			return nil, fmt.Errorf("fib rule %s: %w", r.Name, err)
		}
		e.rules = append(e.rules, rule{config: r, statement: s})
	}
	sort.Slice(e.rules, func(i, j int) bool { return e.rules[i].config.Priority < e.rules[j].config.Priority })
	if d := c.Fib.DefaultRoute; d.Mode != "" && d.Mode != oc.FIB_DEFAULT_ROUTE_IGNORE {
		if err := unsupportedConditions(d.Conditions); err != nil {
			return nil, fmt.Errorf("fib default-route: %w", err)
		}
		s, err := table.NewMatchStatement(DefaultRouteRule, c.DefinedSets, d.Conditions)
		if err != nil {
			return nil, fmt.Errorf("fib default-route: %w", err)
		}
		e.defaultRoute, e.defaultMatch = d, s
	}
	return e, nil
}

func pathPrefix(path *table.Path) (netip.Prefix, bool) {
	if p, ok := path.GetNlri().(*bgp.IPAddrPrefix); ok {
		return p.Prefix, true
	}
	return netip.Prefix{}, false
}

// Evaluate returns the decision of the matching rule with the lowest
// priority. The default route is never matched by these rules, only by the
// default-route section.
func (e *Engine) Evaluate(path *table.Path) Decision {
	prefix, _ := pathPrefix(path)
	d := Decision{Prefix: prefix, Action: ActionIgnore}
	if prefix.Bits() == 0 {
		switch {
		case e.defaultMatch == nil:
			d.Reason = ReasonDefaultRoute
		case !e.defaultMatch.Evaluate(path, nil):
			d.Reason = ReasonDefaultNoMatch
		default:
			d.Action, d.Rule, d.Reason = ActionInstall, DefaultRouteRule, ReasonRule
			d.Table, d.ExitPaths = e.defaultRoute.TableID, e.defaultRoute.ExitPaths
		}
		return d
	}
	for _, r := range e.rules {
		if !r.statement.Evaluate(path, nil) {
			continue
		}
		d.Action, d.Rule, d.Reason = r.config.Action, r.config.Name, ReasonRule
		if d.Action == ActionInstall {
			d.Table, d.ExitPaths = r.config.TableID, r.config.ExitPaths
		}
		return d
	}
	d.Reason = ReasonNoMatch
	return d
}

// Validate checks an enabled configuration as a whole and returns warnings
// for combinations that are allowed but deserve the operator's attention.
func Validate(c *oc.BgpConfigSet) ([]string, error) {
	if !c.Fib.Enabled {
		return nil, nil
	}
	if !supported {
		return nil, fmt.Errorf("fib is not supported on this platform")
	}
	if _, err := NewEngine(c); err != nil {
		return nil, err
	}
	var warnings []string
	for _, n := range c.Neighbors {
		if gr := n.GracefulRestart.Config; gr.Enabled || gr.LongLivedEnabled {
			warnings = append(warnings, fmt.Sprintf("neighbor %s has graceful restart enabled; the fib treats its stale paths as withdrawn", n.Config.NeighborAddress))
		}
	}
	if c.Zebra.Config.Enabled {
		var main []string
		for _, r := range c.Fib.Rules {
			if r.Action == ActionInstall && r.TableID == oc.FIB_TABLE_MAIN {
				main = append(main, r.Name)
			}
		}
		if len(main) > 0 {
			warnings = append(warnings, fmt.Sprintf("zebra is enabled and fib rules %v use the main table; prefer a separate table to avoid conflicts", main))
		}
	}
	return warnings, nil
}
