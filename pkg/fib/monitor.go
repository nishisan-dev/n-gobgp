package fib

import (
	"sync"
	"time"

	"github.com/osrg/gobgp/v4/pkg/config/oc"
)

// checker tests one exit path: whether its link is up and whether its gateway
// answers.
type checker interface {
	check(p oc.FibExitPath, timeout time.Duration) (linkUp, reachable bool, err error)
	// watchLinks calls wake with the name of each link that changes, until
	// done is closed.
	watchLinks(done <-chan struct{}, wake func(string)) error
}

// pathStates tells whether an exit path is currently usable.
type pathStates interface {
	usable(name string) bool
	reason(name string) string
}

type pathChange struct {
	Name, Reason string
	Usable       bool
	Err          error
}

// monitor checks every exit path periodically, and at once when its link
// changes, and reports each change of the usable state.
type monitor struct {
	cfg      oc.FibHealth
	paths    []oc.FibExitPath
	checker  checker
	onChange func(pathChange)

	mu     sync.Mutex
	health map[string]*pathHealth
	wake   map[string]chan struct{}
	done   chan struct{}
	wg     sync.WaitGroup
}

func newMonitor(cfg oc.FibHealth, paths []oc.FibExitPath, c checker, onChange func(pathChange)) *monitor {
	m := &monitor{cfg: cfg, paths: paths, checker: c, onChange: onChange,
		health: map[string]*pathHealth{}, wake: map[string]chan struct{}{}, done: make(chan struct{})}
	for _, p := range paths {
		m.health[p.Name] = newPathHealth(cfg)
		m.wake[p.Name] = make(chan struct{}, 1)
	}
	return m
}

func (m *monitor) usable(name string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	h, ok := m.health[name]
	return ok && h.usable
}

func (m *monitor) reason(name string) string {
	m.mu.Lock()
	defer m.mu.Unlock()
	if h, ok := m.health[name]; ok {
		return h.reason
	}
	return ""
}

// settled reports whether every path reached its first verdict.
func (m *monitor) settled() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, h := range m.health {
		if !h.settled() {
			return false
		}
	}
	return true
}

func (m *monitor) probeTimeout() time.Duration {
	return min(max(m.cfg.Interval/2, 100*time.Millisecond), time.Second)
}

// start returns an error when link changes cannot be followed; the periodic
// checks still run.
func (m *monitor) start() error {
	err := m.checker.watchLinks(m.done, func(ifname string) {
		for _, p := range m.paths {
			if p.Interface == ifname {
				select {
				case m.wake[p.Name] <- struct{}{}:
				default:
				}
			}
		}
	})
	for _, p := range m.paths {
		m.wg.Add(1)
		go m.run(p)
	}
	return err
}

func (m *monitor) stop() {
	close(m.done)
	m.wg.Wait()
}

func (m *monitor) run(p oc.FibExitPath) {
	defer m.wg.Done()
	ticker := time.NewTicker(m.cfg.Interval)
	defer ticker.Stop()
	var lastErr string
	for {
		linkUp, reachable, err := m.checker.check(p, m.probeTimeout())
		m.mu.Lock()
		h := m.health[p.Name]
		changed := h.observe(time.Now(), linkUp, reachable)
		change := pathChange{Name: p.Name, Usable: h.usable, Reason: h.reason, Err: err}
		m.mu.Unlock()
		// Report a new error once, even without a state change, so that a
		// misconfiguration is visible while the path stays down.
		errText := ""
		if err != nil {
			errText = err.Error()
		}
		if changed || (errText != lastErr && errText != "" && !change.Usable) {
			m.onChange(change)
		}
		lastErr = errText
		select {
		case <-m.done:
			return
		case <-ticker.C:
		case <-m.wake[p.Name]:
		}
	}
}
