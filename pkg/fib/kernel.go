package fib

import (
	"fmt"
	"net/netip"
)

// routeKey identifies an IPv4 route in the kernel: two routes with the same
// key cannot coexist, whoever owns them.
type routeKey struct {
	Table  uint32
	Prefix netip.Prefix
	Metric uint32
}

func (k routeKey) String() string {
	return fmt.Sprintf("%s table %d metric %d", k.Prefix, k.Table, k.Metric)
}

// kroute is a unicast route through one gateway.
type kroute struct {
	Table     uint32
	Prefix    netip.Prefix
	Metric    uint32
	Gateway   netip.Addr
	Interface string
}

func (r kroute) key() routeKey { return routeKey{r.Table, r.Prefix, r.Metric} }

func (r kroute) String() string {
	return fmt.Sprintf("%s via %s dev %s table %d metric %d", r.Prefix, r.Gateway, r.Interface, r.Table, r.Metric)
}

func routeText(r *kroute) string {
	if r == nil {
		return "none"
	}
	return r.String()
}

func sameRoute(a, b *kroute) bool {
	return a == nil && b == nil || a != nil && b != nil && *a == *b
}

// krule is a policy routing rule that looks up a table. Unset selectors
// match everything.
type krule struct {
	Priority uint32
	Table    uint32
	From     netip.Prefix
	To       netip.Prefix
	Iif      string
	Mark     uint32
	Mask     uint32
}

func (r krule) String() string {
	s := fmt.Sprintf("priority %d", r.Priority)
	if r.From.IsValid() {
		s += " from " + r.From.String()
	}
	if r.To.IsValid() {
		s += " to " + r.To.String()
	}
	if r.Iif != "" {
		s += " iif " + r.Iif
	}
	if r.Mask != 0 {
		s += fmt.Sprintf(" fwmark %#x/%#x", r.Mark, r.Mask)
	}
	return s + fmt.Sprintf(" lookup %d", r.Table)
}

// conflictError reports that another component owns the kernel object.
type conflictError struct {
	Protocol int
}

func (e *conflictError) Error() string {
	return fmt.Sprintf("held by another owner (protocol %d)", e.Protocol)
}

// kernel manipulates only the objects carrying the fib protocol number,
// except takeOverRoute, which replaces a foreign route on explicit request.
type kernel interface {
	ownedRoutes() ([]kroute, error)
	// addRoute fails with *conflictError when another owner holds the key.
	addRoute(kroute) error
	// replaceRoute changes an owned route in place, atomically.
	replaceRoute(kroute) error
	deleteRoute(kroute) error
	// takeOverRoute replaces the foreign route at the key with r and returns
	// a function that puts the foreign route back.
	takeOverRoute(r kroute) (restore func() error, err error)

	// listRules returns the owned rules and the priorities used by others.
	listRules() (owned []krule, foreign map[uint32]bool, err error)
	addRule(krule) error
	deleteRule(krule) error
}
