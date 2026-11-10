package table

import (
	"fmt"
	"log/slog"

	"github.com/osrg/gobgp/v4/pkg/config/oc"
)

// NewMatchStatement builds the conditions of a policy statement, bound to the
// given defined sets, for consumers that only need to evaluate paths outside
// the policy assignment points. Actions are not part of the statement.
func NewMatchStatement(name string, sets oc.DefinedSets, conditions oc.Conditions) (*Statement, error) {
	r := NewRoutingPolicy(slog.Default())
	if err := r.reload(oc.RoutingPolicy{DefinedSets: sets}); err != nil {
		return nil, err
	}
	s, err := NewStatement(oc.Statement{Name: name, Conditions: conditions})
	if err != nil {
		return nil, err
	}
	for _, c := range s.Conditions {
		if err := r.validateCondition(c); err != nil {
			return nil, fmt.Errorf("statement %s: %w", name, err)
		}
	}
	return s, nil
}
