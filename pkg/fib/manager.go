package fib

import (
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"net/netip"
	"sync"
	"time"

	"github.com/osrg/gobgp/v4/internal/pkg/table"
	"github.com/osrg/gobgp/v4/pkg/config/oc"
	"github.com/osrg/gobgp/v4/pkg/packet/bgp"
	"github.com/osrg/gobgp/v4/pkg/server"
)

// Route and rule statuses.
const (
	StatusInstalled  = "installed"
	StatusReplaced   = "installed in place of a foreign route"
	StatusLastActive = "kept on the last active exit path"
	StatusInherited  = "kept from the previous run"
	StatusObserved   = "observed"
	StatusIgnored    = "ignored"
	StatusNoPath     = "no usable exit path"
	StatusProtected  = "protected prefix"
	StatusConflict   = "conflict"
	StatusError      = "error"
)

type routeEntry struct {
	decision Decision
	route    *kroute
	status   string
}

type anchorEntry struct {
	config     oc.FibAnchor
	route      *kroute
	status     string
	lastActive string
}

// Manager follows the global RIB best paths and keeps one forwarding decision
// per destination. In enforce mode it programs the kernel: anchors, BGP
// routes and traffic rules, all marked with the fib protocol number. Objects
// with another protocol number are never changed, except a foreign default
// route when default-route.replace-external allows it.
type Manager struct {
	config  oc.Fib
	engine  *Engine
	logger  *slog.Logger
	kernel  kernel
	paths   pathStates
	exits   map[string]oc.FibExitPath
	enforce bool
	monitor *monitor
	metrics *metrics

	mu      sync.Mutex
	entries map[netip.Prefix]*routeEntry
	anchors []*anchorEntry
	// installed holds the owned kernel routes; owners maps their keys to the
	// anchor or destination that wants them. A key without owner was left by
	// a previous run and waits for reconciliation.
	installed map[routeKey]kroute
	owners    map[routeKey]string
	restores  map[routeKey]func() error
	rules     map[string]string

	// started is set once the kernel state has been taken over; path
	// changes before that only update the monitor.
	started     bool
	reconciled  bool
	established map[netip.Addr]bool
	endOfRib    map[netip.Addr]bool
	eorTimer    *time.Timer
	retainTimer *time.Timer
	closed      bool

	sub        *server.BestPathSubscription
	resyncStop chan struct{}
	resyncDone chan struct{}
}

// New builds a manager for an enabled configuration read by oc.ReadConfig.
func New(c *oc.BgpConfigSet, logger *slog.Logger) (*Manager, error) {
	if !c.Fib.Enabled {
		return nil, fmt.Errorf("fib is not enabled")
	}
	var k kernel
	if c.Fib.Mode == oc.FIB_MODE_ENFORCE {
		k = newKernel(c.Fib.Protocol)
	}
	m, err := newManager(c, logger, k, nil)
	if err != nil {
		return nil, err
	}
	m.monitor = newMonitor(c.Fib.Health, c.Fib.ExitPaths, newChecker(), m.onPathChange)
	m.paths = m.monitor
	return m, nil
}

func newManager(c *oc.BgpConfigSet, logger *slog.Logger, k kernel, paths pathStates) (*Manager, error) {
	warnings, err := Validate(c)
	if err != nil {
		return nil, err
	}
	engine, err := NewEngine(c)
	if err != nil {
		return nil, err
	}
	for _, w := range warnings {
		logger.Warn(w, slog.String("Topic", "Fib"))
	}
	m := &Manager{
		config: c.Fib, engine: engine, logger: logger.With(slog.String("Topic", "Fib")), kernel: k, paths: paths,
		enforce: c.Fib.Mode == oc.FIB_MODE_ENFORCE, exits: map[string]oc.FibExitPath{},
		entries: map[netip.Prefix]*routeEntry{}, installed: map[routeKey]kroute{}, owners: map[routeKey]string{},
		restores: map[routeKey]func() error{}, rules: map[string]string{},
		established: map[netip.Addr]bool{}, endOfRib: map[netip.Addr]bool{}, reconciled: true,
	}
	m.metrics = newMetrics(m)
	for _, p := range c.Fib.ExitPaths {
		m.exits[p.Name] = p
	}
	for _, a := range c.Fib.Anchors {
		m.anchors = append(m.anchors, &anchorEntry{config: a})
	}
	return m, nil
}

// Start takes over the kernel state left by a previous run, installs anchors
// and traffic rules, waits up to anchor-wait for the anchors, and then
// follows the best paths, starting with the current table. Call it before
// the BGP neighbors are configured.
func (m *Manager) Start(s *server.BgpServer) error {
	if m.enforce {
		if err := kernelRequirements(len(m.config.TrafficRules) > 0); err != nil {
			return fmt.Errorf("fib: %w", err)
		}
	}
	if err := m.monitor.start(); err != nil {
		m.logger.Warn("fib cannot follow link changes; relying on periodic checks", slog.Any("Error", err))
	}
	// Let every exit path reach a verdict first, so that anchors start on
	// their preferred path instead of the first one to answer.
	deadline := time.Now().Add(m.config.AnchorWait)
	for m.enforce && len(m.anchors) > 0 && !m.monitor.settled() && time.Now().Before(deadline) {
		time.Sleep(50 * time.Millisecond)
	}
	m.mu.Lock()
	err := m.begin()
	m.mu.Unlock()
	if err != nil {
		m.monitor.stop()
		return fmt.Errorf("fib: %w", err)
	}
	m.waitForAnchors(deadline)
	sub, err := s.SubscribeBestPaths([]bgp.Family{bgp.RF_IPv4_UC}, m.apply)
	if err != nil {
		m.monitor.stop()
		m.stopTimers()
		m.cleanup()
		return fmt.Errorf("fib: %w", err)
	}
	m.mu.Lock()
	m.sub = sub
	m.mu.Unlock()
	if m.enforce {
		m.resyncStop, m.resyncDone = make(chan struct{}), make(chan struct{})
		go m.resyncLoop()
	}
	m.logger.Info("fib started", slog.String("Mode", m.config.Mode), slog.Int("Rules", len(m.config.Rules)),
		slog.Int("Anchors", len(m.anchors)), slog.Int("TrafficRules", len(m.config.TrafficRules)))
	return nil
}

// begin runs with m.mu held, or before the manager is shared.
func (m *Manager) begin() error {
	m.started = true
	if !m.enforce {
		return nil
	}
	owned, err := m.kernel.ownedRoutes()
	if err != nil {
		return fmt.Errorf("listing owned routes: %w", err)
	}
	for _, r := range owned {
		m.installed[r.key()] = r
	}
	m.syncAnchors()
	m.syncRules()
	inherited := 0
	for key := range m.installed {
		if m.owners[key] == "" {
			inherited++
		}
	}
	if inherited > 0 {
		m.reconciled = false
		m.retainTimer = time.AfterFunc(m.config.Startup.MaxRetention, func() { m.timedReconcile("max-retention expired") })
		m.logger.Info("fib keeps the routes of the previous run until BGP converges", slog.Int("Routes", inherited))
	}
	return nil
}

func (m *Manager) waitForAnchors(deadline time.Time) {
	if !m.enforce || len(m.anchors) == 0 {
		return
	}
	for {
		m.mu.Lock()
		missing := []string{}
		for _, a := range m.anchors {
			if a.route == nil {
				missing = append(missing, a.config.Name)
			}
		}
		m.mu.Unlock()
		if len(missing) == 0 {
			return
		}
		if time.Now().After(deadline) {
			m.logger.Warn("fib anchors are not installed; starting BGP anyway", slog.Any("Anchors", missing))
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (m *Manager) stopTimers() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.closed = true
	for _, t := range []*time.Timer{m.eorTimer, m.retainTimer} {
		if t != nil {
			t.Stop()
		}
	}
}

// Close stops following the RIB and the exit paths, then removes every owned
// kernel object: BGP routes first, then traffic rules, then anchors.
func (m *Manager) Close() {
	m.mu.Lock()
	sub := m.sub
	m.sub = nil
	m.mu.Unlock()
	if sub == nil {
		return
	}
	sub.Stop()
	<-sub.Done()
	if m.resyncStop != nil {
		close(m.resyncStop)
		<-m.resyncDone
	}
	m.monitor.stop()
	m.stopTimers()
	m.cleanup()
	m.logger.Info("fib stopped")
}

func (m *Manager) cleanup() {
	if !m.enforce {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, e := range m.entries {
		e.route, _ = m.syncRoute(e.decision.Prefix.String(), e.route, nil, false)
	}
	for key, r := range m.installed {
		if m.owners[key] == "" {
			m.removeRoute(r)
		}
	}
	if owned, _, err := m.kernel.listRules(); err == nil {
		for _, r := range owned {
			if err := m.kernel.deleteRule(r); err != nil {
				m.logger.Warn("fib failed to remove a traffic rule", slog.String("Rule", r.String()), slog.Any("Error", err))
			}
		}
	}
	for _, a := range m.anchors {
		a.route, _ = m.syncRoute("anchor "+a.config.Name, a.route, nil, false)
	}
	// Anything left with the fib protocol number belongs to nobody else.
	if owned, err := m.kernel.ownedRoutes(); err == nil {
		for _, r := range owned {
			m.removeRoute(r)
		}
	}
}

// Decisions returns a copy of the current decisions.
func (m *Manager) Decisions() map[netip.Prefix]Decision {
	m.mu.Lock()
	defer m.mu.Unlock()
	d := make(map[netip.Prefix]Decision, len(m.entries))
	for p, e := range m.entries {
		d[p] = e.decision
	}
	return d
}

// Status returns the kernel status of a destination.
func (m *Manager) Status(prefix netip.Prefix) string {
	m.mu.Lock()
	defer m.mu.Unlock()
	if e, ok := m.entries[prefix]; ok {
		return e.status
	}
	return ""
}

func (m *Manager) AnchorStatus(name string) string {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, a := range m.anchors {
		if a.config.Name == name {
			return a.status
		}
	}
	return ""
}

func (m *Manager) RuleStatus(name string) string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.rules[name]
}

// Reconciled reports whether the routes of a previous run were reconciled.
func (m *Manager) Reconciled() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.reconciled
}

func (m *Manager) apply(ev server.BestPathEvent) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if ev.Peer != nil {
		m.onPeer(*ev.Peer)
		return
	}
	for _, path := range ev.Paths {
		if path.IsEOR() {
			continue
		}
		prefix, ok := pathPrefix(path)
		if !ok {
			continue
		}
		e, known := m.entries[prefix]
		// Stale paths are kept by graceful restart only; forwarding must not use them.
		if path.IsWithdraw || path.IsStale() {
			if known {
				e.route, _ = m.syncRoute(prefix.String(), e.route, nil, false)
				delete(m.entries, prefix)
				m.logger.Info("fib decision removed", slog.String("Mode", m.config.Mode),
					slog.String("Prefix", prefix.String()), slog.Bool("Stale", !path.IsWithdraw))
			}
			continue
		}
		d := m.engine.Evaluate(path)
		if !known {
			e = &routeEntry{}
			m.entries[prefix] = e
		} else if e.decision.equal(d) {
			continue
		}
		e.decision = d
		m.logDecision(d, path)
		m.syncEntry(e)
	}
	if ev.Initial {
		m.logger.Info("fib evaluated the initial table", slog.Int("Destinations", len(m.entries)))
	}
}

func (m *Manager) logDecision(d Decision, path *table.Path) {
	m.logger.Info("fib decision", slog.String("Mode", m.config.Mode),
		slog.String("Prefix", d.Prefix.String()), slog.String("Action", d.Action), slog.String("Rule", d.Rule),
		slog.String("Reason", d.Reason), slog.Any("Table", d.Table), slog.Any("ExitPaths", d.ExitPaths),
		slog.String("Nexthop", path.GetNexthop().String()))
}

func (m *Manager) onPathChange(c pathChange) {
	attrs := []any{slog.String("ExitPath", c.Name), slog.Bool("Usable", c.Usable), slog.String("Reason", c.Reason)}
	if c.Err != nil {
		attrs = append(attrs, slog.String("Detail", c.Err.Error()))
	}
	if c.Usable {
		m.logger.Info("fib exit path is usable", attrs...)
	} else {
		m.logger.Warn("fib exit path is not usable", attrs...)
	}
	m.metrics.pathChanged(c.Name)
	m.mu.Lock()
	defer m.mu.Unlock()
	if !m.started {
		return
	}
	m.syncAnchors()
	for _, e := range m.entries {
		// Conflicts are retried by the resync, not on every path change.
		if e.decision.Action == ActionInstall && e.status != StatusConflict {
			m.syncEntry(e)
		}
	}
}

func (m *Manager) firstUsable(names []string) (oc.FibExitPath, bool) {
	for _, n := range names {
		if m.paths.usable(n) {
			return m.exits[n], true
		}
	}
	return oc.FibExitPath{}, false
}

func (m *Manager) routeVia(table uint32, prefix netip.Prefix, p oc.FibExitPath) *kroute {
	return &kroute{Table: table, Prefix: prefix, Metric: m.config.Metric, Gateway: p.Gateway, Interface: p.Interface}
}

// protected reports whether BGP must not install the prefix in the table:
// protected prefixes guard the main table, anchors guard their own table,
// each against equal or more specific routes.
func (m *Manager) protected(prefix netip.Prefix, tableID uint32) bool {
	covers := func(p netip.Prefix) bool { return p.Bits() <= prefix.Bits() && p.Contains(prefix.Addr()) }
	if tableID == oc.FIB_TABLE_MAIN {
		for _, p := range m.config.ProtectedPrefixes {
			if covers(p) {
				return true
			}
		}
	}
	for _, a := range m.anchors {
		if a.config.TableID == tableID && covers(a.config.Prefix) {
			return true
		}
	}
	return false
}

func (m *Manager) syncEntry(e *routeEntry) {
	d := e.decision
	var want *kroute
	status := StatusInstalled
	switch {
	case !m.enforce:
		status = StatusObserved
	case d.Action != ActionInstall:
		status = StatusIgnored
	case m.protected(d.Prefix, d.Table):
		status = StatusProtected
	default:
		p, ok := m.firstUsable(d.ExitPaths)
		if !ok {
			status = StatusNoPath
		} else {
			want = m.routeVia(d.Table, d.Prefix, p)
		}
	}
	before := e.route
	if m.enforce {
		takeOver := d.Prefix.Bits() == 0 && m.config.DefaultRoute.ReplaceExternal
		var err error
		e.route, err = m.syncRoute(d.Prefix.String(), e.route, want, takeOver)
		if err != nil {
			status = errorStatus(err)
		} else if e.route != nil && m.restores[e.route.key()] != nil {
			status = StatusReplaced
		}
	}
	m.report(&e.status, status, !sameRoute(before, e.route), "fib route", slog.String("Prefix", d.Prefix.String()), slog.String("Route", routeText(e.route)))
}

func (m *Manager) syncAnchors() {
	if !m.enforce {
		return
	}
	for _, a := range m.anchors {
		var want *kroute
		status := StatusInstalled
		key := routeKey{a.config.TableID, a.config.Prefix, m.config.Metric}
		if p, ok := m.firstUsable(a.config.ExitPaths); ok {
			a.lastActive = p.Name
			want = m.routeVia(a.config.TableID, a.config.Prefix, p)
		} else if a.lastActive != "" {
			// Keep the session reachable: the last path may still work for
			// traffic even if its probes fail.
			want = m.routeVia(a.config.TableID, a.config.Prefix, m.exits[a.lastActive])
			status = StatusLastActive
		} else if r, ok := m.installed[key]; ok && m.owners[key] == "" {
			want = &r
			status = StatusInherited
		} else {
			status = StatusNoPath
		}
		before := a.route
		var err error
		a.route, err = m.syncRoute("anchor "+a.config.Name, a.route, want, false)
		if err != nil {
			status = errorStatus(err)
		}
		m.report(&a.status, status, !sameRoute(before, a.route), "fib anchor", slog.String("Anchor", a.config.Name), slog.String("Route", routeText(a.route)))
	}
}

func errorStatus(err error) string {
	var c *conflictError
	if errors.As(err, &c) {
		return StatusConflict
	}
	return StatusError
}

// report logs a status change, or a route change with the same status.
func (m *Manager) report(current *string, status string, routeChanged bool, msg string, attrs ...any) {
	if *current == status && !routeChanged {
		return
	}
	*current = status
	attrs = append(attrs, slog.String("Status", status))
	switch status {
	case StatusConflict, StatusError, StatusNoPath, StatusLastActive:
		m.logger.Warn(msg, attrs...)
	default:
		m.logger.Info(msg, attrs...)
	}
}

// syncRoute moves the kernel from cur, the route installed for owner, to
// want, and returns the route installed afterwards. A new key is installed
// before the old one is removed. Errors are logged here.
func (m *Manager) syncRoute(owner string, cur, want *kroute, takeOver bool) (*kroute, error) {
	if want == nil {
		if cur != nil {
			if err := m.removeRoute(*cur); err != nil {
				return cur, err
			}
		}
		return nil, nil
	}
	if cur != nil && *cur == *want {
		return cur, nil
	}
	key := want.key()
	var err error
	if cur != nil && cur.key() == key {
		if err = m.kernel.replaceRoute(*want); err != nil {
			m.kernelError(owner, "replace", *want, err)
			if errorStatus(err) == StatusConflict {
				// Another owner took the key: it is not ours anymore.
				m.forget(key)
				return nil, err
			}
			return cur, err
		}
		m.installed[key] = *want
		return want, nil
	}
	if inst, ok := m.installed[key]; ok {
		switch {
		case m.owners[key] != "" && m.owners[key] != owner:
			err = fmt.Errorf("route %s is already installed for %s", key, m.owners[key])
		case inst != *want:
			err = m.kernel.replaceRoute(*want)
		}
	} else {
		err = m.kernel.addRoute(*want)
		if takeOver && errorStatus(err) == StatusConflict {
			var restore func() error
			if restore, err = m.kernel.takeOverRoute(*want); err == nil {
				m.restores[key] = restore
				m.logger.Warn("fib replaced a foreign route as allowed by replace-external", slog.String("Route", key.String()))
			}
		}
	}
	var installed *kroute
	if err != nil {
		m.kernelError(owner, "install", *want, err)
	} else {
		m.installed[key], m.owners[key] = *want, owner
		installed = want
	}
	if cur != nil {
		if rmErr := m.removeRoute(*cur); rmErr != nil && err == nil {
			err = rmErr
		}
	}
	return installed, err
}

func (m *Manager) kernelError(owner, op string, r kroute, err error) {
	m.metrics.kernelError()
	m.logger.Warn("fib kernel operation failed", slog.String("Owner", owner), slog.String("Operation", op),
		slog.String("Route", r.key().String()), slog.String("Error", err.Error()))
}

func (m *Manager) forget(key routeKey) {
	delete(m.installed, key)
	delete(m.owners, key)
	delete(m.restores, key)
}

// removeRoute deletes an owned route, or puts back the foreign route it
// replaced. On failure the route stays known without owner, so that the
// resync retries.
func (m *Manager) removeRoute(r kroute) error {
	key := r.key()
	var err error
	if restore := m.restores[key]; restore != nil {
		err = restore()
	} else {
		err = m.kernel.deleteRoute(r)
	}
	if err != nil {
		m.kernelError(m.owners[key], "remove", r, err)
		m.owners[key] = ""
		return err
	}
	m.forget(key)
	return nil
}

func (m *Manager) onPeer(ev server.PeerEvent) {
	if m.reconciled {
		return
	}
	switch {
	case ev.EndOfRib:
		m.endOfRib[ev.Address] = true
	case ev.Established:
		m.established[ev.Address] = true
		if m.eorTimer == nil {
			m.eorTimer = time.AfterFunc(m.config.Startup.EorTimeout, func() { m.timedReconcile("eor-timeout expired") })
		}
		return
	default:
		delete(m.established, ev.Address)
		delete(m.endOfRib, ev.Address)
		return
	}
	for addr := range m.established {
		if !m.endOfRib[addr] {
			return
		}
	}
	m.reconcile("End-of-RIB received from every established peer")
}

func (m *Manager) timedReconcile(reason string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if !m.closed && !m.reconciled {
		m.reconcile(reason)
	}
}

// reconcile removes the routes of a previous run that BGP did not confirm.
func (m *Manager) reconcile(reason string) {
	m.reconciled = true
	for _, t := range []*time.Timer{m.eorTimer, m.retainTimer} {
		if t != nil {
			t.Stop()
		}
	}
	removed := 0
	for key, r := range m.installed {
		if m.owners[key] == "" && m.removeRoute(r) == nil {
			removed++
		}
	}
	// Anchors may have kept an inherited route while no path was usable.
	m.syncAnchors()
	m.logger.Info("fib reconciled the routes of the previous run", slog.String("Reason", reason), slog.Int("Removed", removed))
}

func (m *Manager) resyncLoop() {
	defer close(m.resyncDone)
	ticker := time.NewTicker(m.config.ResyncInterval)
	defer ticker.Stop()
	for {
		select {
		case <-m.resyncStop:
			return
		case <-ticker.C:
			m.resync()
		}
	}
}

// resync compares the kernel with the owned state: routes removed by others
// are installed again, unknown owned routes are removed once reconciled, and
// conflicts and errors are retried.
func (m *Manager) resync() {
	if !m.enforce {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	owned, err := m.kernel.ownedRoutes()
	if err != nil {
		m.logger.Warn("fib resync failed", slog.String("Error", err.Error()))
		return
	}
	present := map[routeKey]kroute{}
	for _, r := range owned {
		present[r.key()] = r
	}
	for key, r := range maps.Clone(m.installed) {
		if p, ok := present[key]; ok && p == r {
			continue
		}
		if m.restores[key] != nil {
			continue
		}
		m.logger.Warn("fib route changed by another component; installing it again", slog.String("Route", key.String()))
		m.metrics.repaired()
		m.forget(key)
		m.dropOwnerRoute(key)
	}
	for key, r := range present {
		if _, ok := m.installed[key]; ok {
			continue
		}
		if m.reconciled {
			m.logger.Warn("fib removes an unknown route with its protocol number", slog.String("Route", key.String()))
			_ = m.kernel.deleteRoute(r)
		} else {
			m.installed[key] = r
		}
	}
	m.syncAnchors()
	for _, e := range m.entries {
		if e.decision.Action == ActionInstall && (e.route == nil || e.status == StatusConflict || e.status == StatusError) {
			m.syncEntry(e)
		}
	}
	m.syncRules()
}

// dropOwnerRoute forgets the route recorded for whoever owned the key.
func (m *Manager) dropOwnerRoute(key routeKey) {
	for _, a := range m.anchors {
		if a.route != nil && a.route.key() == key {
			a.route = nil
		}
	}
	for _, e := range m.entries {
		if e.route != nil && e.route.key() == key {
			e.route = nil
		}
	}
}

// desiredRules lists the exemptions, then the traffic rules. Exemptions keep
// management traffic and BGP sessions on their tables whatever the traffic
// rules select; they are only needed when there are traffic rules.
func (m *Manager) desiredRules() ([]string, map[string]krule) {
	if len(m.config.TrafficRules) == 0 {
		return nil, nil
	}
	var names []string
	rules := map[string]krule{}
	priority := m.config.RulePriorityMin
	add := func(name string, r krule) {
		names = append(names, name)
		rules[name] = r
	}
	for _, p := range m.config.ProtectedPrefixes {
		add("protected "+p.String(), krule{Priority: priority, Table: oc.FIB_TABLE_MAIN, To: p})
		priority++
	}
	for _, a := range m.anchors {
		add("anchor "+a.config.Name, krule{Priority: priority, Table: a.config.TableID, To: a.config.Prefix})
		priority++
	}
	for _, r := range m.config.TrafficRules {
		add(r.Name, krule{Priority: r.Priority, Table: r.TableID, From: r.From, To: r.To, Iif: r.Iif, Mark: r.Mark, Mask: r.Mask})
	}
	return names, rules
}

func (m *Manager) syncRules() {
	if !m.enforce {
		return
	}
	owned, foreign, err := m.kernel.listRules()
	if err != nil {
		m.logger.Warn("fib cannot list rules", slog.String("Error", err.Error()))
		return
	}
	names, desired := m.desiredRules()
	wanted := map[krule]bool{}
	for _, r := range desired {
		wanted[r] = true
	}
	have := map[krule]bool{}
	for _, r := range owned {
		if !wanted[r] {
			if err := m.kernel.deleteRule(r); err != nil {
				m.logger.Warn("fib failed to remove a rule", slog.String("Rule", r.String()), slog.String("Error", err.Error()))
			} else {
				m.logger.Info("fib removed a rule", slog.String("Rule", r.String()))
			}
			continue
		}
		have[r] = true
	}
	for _, name := range names {
		r := desired[name]
		status := StatusInstalled
		switch {
		case have[r]:
		case foreign[r.Priority]:
			status = StatusConflict
		default:
			if err := m.kernel.addRule(r); err != nil {
				m.metrics.kernelError()
				m.logger.Warn("fib failed to add a rule", slog.String("Rule", r.String()), slog.String("Error", err.Error()))
				status = StatusError
			}
		}
		current := m.rules[name]
		m.report(&current, status, false, "fib traffic rule", slog.String("Name", name), slog.String("Rule", r.String()))
		m.rules[name] = current
	}
}
