package fib

import (
	"fmt"
	"log/slog"
	"maps"
	"net/netip"
	"sync"

	"github.com/osrg/gobgp/v4/internal/pkg/table"
	"github.com/osrg/gobgp/v4/pkg/config/oc"
	"github.com/osrg/gobgp/v4/pkg/packet/bgp"
	"github.com/osrg/gobgp/v4/pkg/server"
)

// Manager follows the global RIB best paths and keeps one forwarding decision
// per destination. In observe mode, decisions are only logged.
type Manager struct {
	config oc.Fib
	engine *Engine
	logger *slog.Logger

	mu        sync.Mutex
	decisions map[netip.Prefix]Decision
	sub       *server.BestPathSubscription
}

// New builds a manager for an enabled configuration read by oc.ReadConfig.
func New(c *oc.BgpConfigSet, logger *slog.Logger) (*Manager, error) {
	if !c.Fib.Enabled {
		return nil, fmt.Errorf("fib is not enabled")
	}
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
	return &Manager{config: c.Fib, engine: engine, logger: logger, decisions: map[netip.Prefix]Decision{}}, nil
}

// Start subscribes to the best paths, starting with the current table.
func (m *Manager) Start(s *server.BgpServer) error {
	sub, err := s.SubscribeBestPaths([]bgp.Family{bgp.RF_IPv4_UC}, m.apply)
	if err != nil {
		return fmt.Errorf("fib: %w", err)
	}
	m.mu.Lock()
	m.sub = sub
	m.mu.Unlock()
	m.logger.Info("fib started", slog.String("Topic", "Fib"), slog.String("Mode", m.config.Mode), slog.Int("Rules", len(m.config.Rules)))
	return nil
}

// Close stops following the RIB and waits for the last event to be applied.
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
	m.logger.Info("fib stopped", slog.String("Topic", "Fib"))
}

// Decisions returns a copy of the current decisions.
func (m *Manager) Decisions() map[netip.Prefix]Decision {
	m.mu.Lock()
	defer m.mu.Unlock()
	return maps.Clone(m.decisions)
}

func (m *Manager) apply(ev server.BestPathEvent) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, path := range ev.Paths {
		if path.IsEOR() {
			continue
		}
		prefix, ok := pathPrefix(path)
		if !ok {
			continue
		}
		previous, known := m.decisions[prefix]
		// Stale paths are kept by graceful restart only; forwarding must not use them.
		if path.IsWithdraw || path.IsStale() {
			if known {
				delete(m.decisions, prefix)
				m.logger.Info("fib decision removed", slog.String("Topic", "Fib"), slog.String("Mode", m.config.Mode),
					slog.String("Prefix", prefix.String()), slog.Bool("Stale", !path.IsWithdraw))
			}
			continue
		}
		d := m.engine.Evaluate(path)
		if known && previous.equal(d) {
			continue
		}
		m.decisions[prefix] = d
		m.logDecision(d, path)
	}
	if ev.Initial {
		m.logger.Info("fib evaluated the initial table", slog.String("Topic", "Fib"), slog.Int("Destinations", len(m.decisions)))
	}
}

func (m *Manager) logDecision(d Decision, path *table.Path) {
	m.logger.Info("fib decision", slog.String("Topic", "Fib"), slog.String("Mode", m.config.Mode),
		slog.String("Prefix", d.Prefix.String()), slog.String("Action", d.Action), slog.String("Rule", d.Rule),
		slog.String("Reason", d.Reason), slog.Any("Table", d.Table), slog.Any("ExitPaths", d.ExitPaths),
		slog.String("Nexthop", path.GetNexthop().String()))
}
