package oc

import (
	"fmt"
	"net/netip"
	"regexp"
	"slices"
	"strconv"
	"strings"
)

// Fib configures the n-gobgp extension that derives Linux forwarding entries
// from the global RIB best paths. It is not part of the OpenConfig model, but
// its rule conditions reuse the policy Conditions syntax and defined sets.
type Fib struct {
	Enabled   bool          `mapstructure:"enabled"`
	Mode      string        `mapstructure:"mode"`
	Protocol  int           `mapstructure:"protocol"`
	ExitPaths []FibExitPath `mapstructure:"exit-paths"`
	Rules     []FibRule     `mapstructure:"rules"`
}

// FibExitPath is a named way out of the server: an interface and the gateway
// reached through it.
type FibExitPath struct {
	Name      string     `mapstructure:"name"`
	Interface string     `mapstructure:"interface"`
	Gateway   netip.Addr `mapstructure:"gateway"`
}

// FibRule selects best paths by their BGP attributes. The matching rule with
// the lowest priority decides the forwarding of a destination.
type FibRule struct {
	Name       string     `mapstructure:"name"`
	Priority   uint32     `mapstructure:"priority"`
	Family     string     `mapstructure:"family"`
	Action     string     `mapstructure:"action"`
	Table      string     `mapstructure:"table"`
	ExitPaths  []string   `mapstructure:"exit-paths"`
	Conditions Conditions `mapstructure:"conditions"`
	// TableID is the kernel table resolved from Table by Normalize.
	TableID uint32 `mapstructure:"-"`
}

const (
	FIB_MODE_OBSERVE        = "observe"
	FIB_ACTION_INSTALL      = "install"
	FIB_ACTION_IGNORE       = "ignore"
	FIB_FAMILY_IPV4_UNICAST = "ipv4-unicast"
	FIB_DEFAULT_PROTOCOL    = 200

	FIB_TABLE_MAIN = 254
)

var (
	fibNamePattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$`)

	// Route protocol numbers owned by the kernel, by iproute2 and by other
	// routing daemons (including the FRR range). Sharing one would make the
	// ownership of a route ambiguous.
	fibReservedProtocols = []int{0, 1, 2, 3, 4, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 42, 99,
		186, 187, 188, 189, 190, 191, 192, 193, 194, 195, 196, 197, 198}

	// Kernel tables that cannot hold managed routes: unspecified, default and local.
	fibReservedTables = []uint32{0, 253, 255}
)

func parseFibTable(s string) (uint32, error) {
	if s == "main" {
		return FIB_TABLE_MAIN, nil
	}
	id, err := strconv.ParseUint(s, 10, 32)
	if err != nil {
		return 0, fmt.Errorf("table must be \"main\" or a number, got %q", s)
	}
	if slices.Contains(fibReservedTables, uint32(id)) {
		return 0, fmt.Errorf("table %d is reserved by the kernel", id)
	}
	return uint32(id), nil
}

func (p *FibExitPath) validate() error {
	if p.Interface == "" || len(p.Interface) > 15 || strings.ContainsAny(p.Interface, "/ \t") {
		return fmt.Errorf("fib exit path %s: invalid interface name %q", p.Name, p.Interface)
	}
	if !p.Gateway.IsValid() || !p.Gateway.Is4() || p.Gateway.IsUnspecified() || p.Gateway.IsMulticast() {
		return fmt.Errorf("fib exit path %s: gateway must be an IPv4 unicast address", p.Name)
	}
	return nil
}

func (r *FibRule) normalize(paths map[string]bool) error {
	if r.Priority == 0 {
		return fmt.Errorf("fib rule %s: priority must be set and greater than zero", r.Name)
	}
	if r.Family == "" {
		r.Family = FIB_FAMILY_IPV4_UNICAST
	}
	if r.Family != FIB_FAMILY_IPV4_UNICAST {
		return fmt.Errorf("fib rule %s: unsupported family %q (only %s is supported)", r.Name, r.Family, FIB_FAMILY_IPV4_UNICAST)
	}
	if r.Action == "" {
		r.Action = FIB_ACTION_INSTALL
	}
	switch r.Action {
	case FIB_ACTION_IGNORE:
		if r.Table != "" || len(r.ExitPaths) > 0 {
			return fmt.Errorf("fib rule %s: action ignore takes no table or exit paths", r.Name)
		}
	case FIB_ACTION_INSTALL:
		if r.Table == "" {
			return fmt.Errorf("fib rule %s: action install requires a table", r.Name)
		}
		id, err := parseFibTable(r.Table)
		if err != nil {
			return fmt.Errorf("fib rule %s: %w", r.Name, err)
		}
		r.TableID = id
		if len(r.ExitPaths) == 0 {
			return fmt.Errorf("fib rule %s: action install requires at least one exit path", r.Name)
		}
		seen := map[string]bool{}
		for _, name := range r.ExitPaths {
			if !paths[name] {
				return fmt.Errorf("fib rule %s: unknown exit path %q", r.Name, name)
			}
			if seen[name] {
				return fmt.Errorf("fib rule %s: exit path %q is listed twice", r.Name, name)
			}
			seen[name] = true
		}
	default:
		return fmt.Errorf("fib rule %s: unknown action %q (use install or ignore)", r.Name, r.Action)
	}
	return nil
}

// Normalize fills defaults and validates the self-contained part of an
// enabled configuration. References to defined sets are resolved when the
// rules are built, together with the rest of the BGP configuration.
func (f *Fib) Normalize() error {
	if !f.Enabled {
		return nil
	}
	if f.Mode == "" {
		f.Mode = FIB_MODE_OBSERVE
	}
	if f.Mode != FIB_MODE_OBSERVE {
		return fmt.Errorf("fib: unsupported mode %q (only %s is available)", f.Mode, FIB_MODE_OBSERVE)
	}
	if f.Protocol == 0 {
		f.Protocol = FIB_DEFAULT_PROTOCOL
	}
	if f.Protocol < 0 || f.Protocol > 255 {
		return fmt.Errorf("fib: protocol must be between 1 and 255")
	}
	if slices.Contains(fibReservedProtocols, f.Protocol) {
		return fmt.Errorf("fib: protocol %d is used by the kernel or another routing daemon", f.Protocol)
	}
	paths := map[string]bool{}
	for i := range f.ExitPaths {
		p := &f.ExitPaths[i]
		if !fibNamePattern.MatchString(p.Name) {
			return fmt.Errorf("fib: invalid exit path name %q", p.Name)
		}
		if paths[p.Name] {
			return fmt.Errorf("fib: duplicate exit path name %q", p.Name)
		}
		paths[p.Name] = true
		if err := p.validate(); err != nil {
			return err
		}
	}
	names := map[string]bool{}
	priorities := map[uint32]string{}
	for i := range f.Rules {
		r := &f.Rules[i]
		if !fibNamePattern.MatchString(r.Name) {
			return fmt.Errorf("fib: invalid rule name %q", r.Name)
		}
		if names[r.Name] {
			return fmt.Errorf("fib: duplicate rule name %q", r.Name)
		}
		names[r.Name] = true
		if err := r.normalize(paths); err != nil {
			return err
		}
		if other, ok := priorities[r.Priority]; ok {
			return fmt.Errorf("fib: rules %s and %s share priority %d", other, r.Name, r.Priority)
		}
		priorities[r.Priority] = r.Name
	}
	return nil
}
