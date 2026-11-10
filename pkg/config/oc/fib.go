package oc

import (
	"fmt"
	"net/netip"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"
)

// Fib configures the n-gobgp extension that derives Linux forwarding entries
// from the global RIB best paths. It is not part of the OpenConfig model, but
// its rule conditions reuse the policy Conditions syntax and defined sets.
type Fib struct {
	Enabled           bool             `mapstructure:"enabled"`
	Mode              string           `mapstructure:"mode"`
	Protocol          int              `mapstructure:"protocol"`
	Metric            uint32           `mapstructure:"metric"`
	AnchorWait        time.Duration    `mapstructure:"anchor-wait"`
	ResyncInterval    time.Duration    `mapstructure:"resync-interval"`
	ProtectedPrefixes []netip.Prefix   `mapstructure:"protected-prefixes"`
	RulePriorityMin   uint32           `mapstructure:"rule-priority-min"`
	RulePriorityMax   uint32           `mapstructure:"rule-priority-max"`
	Startup           FibStartup       `mapstructure:"startup"`
	Health            FibHealth        `mapstructure:"health"`
	ExitPaths         []FibExitPath    `mapstructure:"exit-paths"`
	Anchors           []FibAnchor      `mapstructure:"anchors"`
	Rules             []FibRule        `mapstructure:"rules"`
	TrafficRules      []FibTrafficRule `mapstructure:"traffic-rules"`
	DefaultRoute      FibDefaultRoute  `mapstructure:"default-route"`
}

// FibStartup bounds how long routes left by a previous run are kept while
// BGP converges again.
type FibStartup struct {
	EorTimeout   time.Duration `mapstructure:"eor-timeout"`
	MaxRetention time.Duration `mapstructure:"max-retention"`
}

// FibTrafficRule is a policy routing rule (ip rule) that sends the selected
// traffic to a table. It does not depend on BGP attributes.
type FibTrafficRule struct {
	Name     string       `mapstructure:"name"`
	Priority uint32       `mapstructure:"priority"`
	Table    string       `mapstructure:"table"`
	From     netip.Prefix `mapstructure:"from"`
	To       netip.Prefix `mapstructure:"to"`
	Iif      string       `mapstructure:"iif"`
	Fwmark   string       `mapstructure:"fwmark"`
	// Resolved by Normalize.
	TableID uint32 `mapstructure:"-"`
	Mark    uint32 `mapstructure:"-"`
	Mask    uint32 `mapstructure:"-"`
}

// FibDefaultRoute decides what happens to a default route learned from BGP.
type FibDefaultRoute struct {
	Mode            string     `mapstructure:"mode"`
	Table           string     `mapstructure:"table"`
	ExitPaths       []string   `mapstructure:"exit-paths"`
	ReplaceExternal bool       `mapstructure:"replace-external"`
	Conditions      Conditions `mapstructure:"conditions"`
	// TableID is the kernel table resolved by Normalize.
	TableID uint32 `mapstructure:"-"`
}

// FibHealth controls how exit path failures are detected and how quickly a
// recovered path is used again.
type FibHealth struct {
	Interval    time.Duration `mapstructure:"interval"`
	Failures    int           `mapstructure:"failures"`
	Successes   int           `mapstructure:"successes"`
	HoldDown    time.Duration `mapstructure:"hold-down"`
	MaxHoldDown time.Duration `mapstructure:"max-hold-down"`
}

// FibExitPath is a named way out of the server: an interface and the gateway
// reached through it.
type FibExitPath struct {
	Name      string     `mapstructure:"name"`
	Interface string     `mapstructure:"interface"`
	Gateway   netip.Addr `mapstructure:"gateway"`
	Probe     string     `mapstructure:"probe"`
}

// FibAnchor is a route that does not come from BGP, such as the route to the
// BGP neighbor itself. It follows the health of its exit paths.
type FibAnchor struct {
	Name      string       `mapstructure:"name"`
	Prefix    netip.Prefix `mapstructure:"prefix"`
	Table     string       `mapstructure:"table"`
	ExitPaths []string     `mapstructure:"exit-paths"`
	// TableID is the kernel table resolved from Table by Normalize.
	TableID uint32 `mapstructure:"-"`
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
	FIB_MODE_ENFORCE        = "enforce"
	FIB_PROBE_ARP           = "arp"
	FIB_PROBE_LINK          = "link"
	FIB_ACTION_INSTALL      = "install"
	FIB_ACTION_IGNORE       = "ignore"
	FIB_FAMILY_IPV4_UNICAST = "ipv4-unicast"
	FIB_DEFAULT_PROTOCOL    = 200
	FIB_DEFAULT_METRIC      = 20

	FIB_DEFAULT_ROUTE_IGNORE = "ignore"
	FIB_DEFAULT_ROUTE_TABLE  = "table"
	FIB_DEFAULT_ROUTE_MAIN   = "main"

	FIB_DEFAULT_RULE_PRIORITY_MIN = 1000
	FIB_DEFAULT_RULE_PRIORITY_MAX = 1999
	// The kernel's own rule for the main table.
	fibMainRulePriority = 32766

	FIB_DEFAULT_RESYNC_INTERVAL = 30 * time.Second
	FIB_DEFAULT_EOR_TIMEOUT     = 30 * time.Second
	FIB_DEFAULT_MAX_RETENTION   = 2 * time.Minute

	FIB_DEFAULT_ANCHOR_WAIT   = 10 * time.Second
	FIB_DEFAULT_INTERVAL      = time.Second
	FIB_DEFAULT_FAILURES      = 3
	FIB_DEFAULT_SUCCESSES     = 3
	FIB_DEFAULT_HOLD_DOWN     = 10 * time.Second
	FIB_DEFAULT_MAX_HOLD_DOWN = 5 * time.Minute

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
	if p.Probe == "" {
		p.Probe = FIB_PROBE_ARP
	}
	if p.Probe != FIB_PROBE_ARP && p.Probe != FIB_PROBE_LINK {
		return fmt.Errorf("fib exit path %s: unknown probe %q (use arp or link)", p.Name, p.Probe)
	}
	return nil
}

func validateExitPathList(owner string, list []string, paths map[string]bool) error {
	if len(list) == 0 {
		return fmt.Errorf("%s: at least one exit path is required", owner)
	}
	seen := map[string]bool{}
	for _, name := range list {
		if !paths[name] {
			return fmt.Errorf("%s: unknown exit path %q", owner, name)
		}
		if seen[name] {
			return fmt.Errorf("%s: exit path %q is listed twice", owner, name)
		}
		seen[name] = true
	}
	return nil
}

func (a *FibAnchor) normalize(paths map[string]bool) error {
	owner := "fib anchor " + a.Name
	if !a.Prefix.IsValid() || !a.Prefix.Addr().Is4() || a.Prefix != a.Prefix.Masked() || a.Prefix.Bits() == 0 {
		return fmt.Errorf("%s: prefix must be a masked, non-default IPv4 prefix", owner)
	}
	if a.Table == "" {
		a.Table = "main"
	}
	id, err := parseFibTable(a.Table)
	if err != nil {
		return fmt.Errorf("%s: %w", owner, err)
	}
	a.TableID = id
	return validateExitPathList(owner, a.ExitPaths, paths)
}

func (h *FibHealth) normalize() error {
	if h.Interval == 0 {
		h.Interval = FIB_DEFAULT_INTERVAL
	}
	if h.Failures == 0 {
		h.Failures = FIB_DEFAULT_FAILURES
	}
	if h.Successes == 0 {
		h.Successes = FIB_DEFAULT_SUCCESSES
	}
	if h.HoldDown == 0 {
		h.HoldDown = FIB_DEFAULT_HOLD_DOWN
	}
	if h.MaxHoldDown == 0 {
		h.MaxHoldDown = max(FIB_DEFAULT_MAX_HOLD_DOWN, h.HoldDown)
	}
	switch {
	case h.Interval < 100*time.Millisecond:
		return fmt.Errorf("fib health: interval must be at least 100ms")
	case h.Failures < 1 || h.Failures > 100 || h.Successes < 1 || h.Successes > 100:
		return fmt.Errorf("fib health: failures and successes must be between 1 and 100")
	case h.HoldDown < 0 || h.MaxHoldDown < h.HoldDown:
		return fmt.Errorf("fib health: max-hold-down must not be shorter than hold-down")
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
		if err := validateExitPathList("fib rule "+r.Name, r.ExitPaths, paths); err != nil {
			return err
		}
	default:
		return fmt.Errorf("fib rule %s: unknown action %q (use install or ignore)", r.Name, r.Action)
	}
	return nil
}

func validIPv4Prefix(p netip.Prefix) bool {
	return p.IsValid() && p.Addr().Is4() && p == p.Masked()
}

// parseFwmark parses "mark" or "mark/mask", in decimal or hexadecimal.
func parseFwmark(s string) (uint32, uint32, error) {
	markText, maskText, hasMask := strings.Cut(s, "/")
	mark, err := strconv.ParseUint(markText, 0, 32)
	if err != nil {
		return 0, 0, err
	}
	mask := uint64(0xffffffff)
	if hasMask {
		if mask, err = strconv.ParseUint(maskText, 0, 32); err != nil {
			return 0, 0, err
		}
	}
	return uint32(mark), uint32(mask), nil
}

func (r *FibTrafficRule) normalize(first, last uint32) error {
	owner := "fib traffic rule " + r.Name
	if r.Priority < first || r.Priority > last {
		return fmt.Errorf("%s: priority %d is outside the range %d-%d (rule-priority-min plus one rule per protected prefix and anchor)", owner, r.Priority, first, last)
	}
	if r.Table == "" {
		return fmt.Errorf("%s: table is required", owner)
	}
	id, err := parseFibTable(r.Table)
	if err != nil {
		return fmt.Errorf("%s: %w", owner, err)
	}
	r.TableID = id
	if r.From.IsValid() && !validIPv4Prefix(r.From) {
		return fmt.Errorf("%s: from must be a masked IPv4 prefix", owner)
	}
	if r.To.IsValid() && !validIPv4Prefix(r.To) {
		return fmt.Errorf("%s: to must be a masked IPv4 prefix", owner)
	}
	if len(r.Iif) > 15 || strings.ContainsAny(r.Iif, "/ \t") {
		return fmt.Errorf("%s: invalid iif %q", owner, r.Iif)
	}
	if r.Fwmark != "" {
		if r.Mark, r.Mask, err = parseFwmark(r.Fwmark); err != nil {
			return fmt.Errorf("%s: invalid fwmark %q", owner, r.Fwmark)
		}
	}
	return nil
}

func (d *FibDefaultRoute) normalize(paths map[string]bool) error {
	if d.Mode == "" {
		d.Mode = FIB_DEFAULT_ROUTE_IGNORE
	}
	switch d.Mode {
	case FIB_DEFAULT_ROUTE_IGNORE:
		if d.Table != "" || len(d.ExitPaths) > 0 || d.ReplaceExternal {
			return fmt.Errorf("fib default-route: mode ignore takes no table, exit paths or replace-external")
		}
		return nil
	case FIB_DEFAULT_ROUTE_TABLE:
		id, err := parseFibTable(d.Table)
		if err != nil {
			return fmt.Errorf("fib default-route: %w", err)
		}
		if id == FIB_TABLE_MAIN {
			return fmt.Errorf("fib default-route: mode table needs a table other than main; use mode main")
		}
		if d.ReplaceExternal {
			return fmt.Errorf("fib default-route: replace-external is only valid in mode main")
		}
		d.TableID = id
	case FIB_DEFAULT_ROUTE_MAIN:
		if d.Table != "" {
			return fmt.Errorf("fib default-route: mode main takes no table")
		}
		d.TableID = FIB_TABLE_MAIN
	default:
		return fmt.Errorf("fib default-route: unknown mode %q (use ignore, table or main)", d.Mode)
	}
	return validateExitPathList("fib default-route", d.ExitPaths, paths)
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
	if f.Mode != FIB_MODE_OBSERVE && f.Mode != FIB_MODE_ENFORCE {
		return fmt.Errorf("fib: unknown mode %q (use %s or %s)", f.Mode, FIB_MODE_OBSERVE, FIB_MODE_ENFORCE)
	}
	if f.Metric == 0 {
		f.Metric = FIB_DEFAULT_METRIC
	}
	if f.AnchorWait == 0 {
		f.AnchorWait = FIB_DEFAULT_ANCHOR_WAIT
	}
	if f.AnchorWait < 0 {
		return fmt.Errorf("fib: anchor-wait must not be negative")
	}
	if err := f.Health.normalize(); err != nil {
		return err
	}
	if f.ResyncInterval == 0 {
		f.ResyncInterval = FIB_DEFAULT_RESYNC_INTERVAL
	}
	if f.ResyncInterval < time.Second {
		return fmt.Errorf("fib: resync-interval must be at least 1s")
	}
	if f.Startup.EorTimeout == 0 {
		f.Startup.EorTimeout = FIB_DEFAULT_EOR_TIMEOUT
	}
	if f.Startup.MaxRetention == 0 {
		f.Startup.MaxRetention = FIB_DEFAULT_MAX_RETENTION
	}
	if f.Startup.EorTimeout < 0 || f.Startup.MaxRetention < 0 {
		return fmt.Errorf("fib startup: timeouts must not be negative")
	}
	for _, p := range f.ProtectedPrefixes {
		if !validIPv4Prefix(p) {
			return fmt.Errorf("fib: protected prefix %s must be a masked IPv4 prefix", p)
		}
	}
	if f.RulePriorityMin == 0 {
		f.RulePriorityMin = FIB_DEFAULT_RULE_PRIORITY_MIN
	}
	if f.RulePriorityMax == 0 {
		f.RulePriorityMax = FIB_DEFAULT_RULE_PRIORITY_MAX
	}
	if f.RulePriorityMin > f.RulePriorityMax || f.RulePriorityMax >= fibMainRulePriority {
		return fmt.Errorf("fib: rule-priority-min and rule-priority-max must form a range below %d", fibMainRulePriority)
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
	anchors := map[string]bool{}
	anchorRoutes := map[string]string{}
	for i := range f.Anchors {
		a := &f.Anchors[i]
		if !fibNamePattern.MatchString(a.Name) {
			return fmt.Errorf("fib: invalid anchor name %q", a.Name)
		}
		if anchors[a.Name] {
			return fmt.Errorf("fib: duplicate anchor name %q", a.Name)
		}
		anchors[a.Name] = true
		if err := a.normalize(paths); err != nil {
			return err
		}
		key := fmt.Sprintf("%s/%d", a.Prefix, a.TableID)
		if other, ok := anchorRoutes[key]; ok {
			return fmt.Errorf("fib: anchors %s and %s install the same route", other, a.Name)
		}
		anchorRoutes[key] = a.Name
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
	if err := f.DefaultRoute.normalize(paths); err != nil {
		return err
	}
	// Exemption rules for protected prefixes and anchors come first.
	first := f.RulePriorityMin + uint32(len(f.ProtectedPrefixes)+len(f.Anchors))
	trafficNames := map[string]bool{}
	trafficPriorities := map[uint32]string{}
	for i := range f.TrafficRules {
		r := &f.TrafficRules[i]
		if !fibNamePattern.MatchString(r.Name) {
			return fmt.Errorf("fib: invalid traffic rule name %q", r.Name)
		}
		if trafficNames[r.Name] {
			return fmt.Errorf("fib: duplicate traffic rule name %q", r.Name)
		}
		trafficNames[r.Name] = true
		if err := r.normalize(first, f.RulePriorityMax); err != nil {
			return err
		}
		if other, ok := trafficPriorities[r.Priority]; ok {
			return fmt.Errorf("fib: traffic rules %s and %s share priority %d", other, r.Name, r.Priority)
		}
		trafficPriorities[r.Priority] = r.Name
	}
	return nil
}
