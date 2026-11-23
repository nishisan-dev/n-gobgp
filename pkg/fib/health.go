package fib

import (
	"time"

	"github.com/osrg/gobgp/v4/pkg/config/oc"
)

const (
	reasonStarting    = "starting"
	reasonLinkDown    = "link down"
	reasonUnreachable = "gateway unreachable"
	reasonUsable      = "usable"
)

// pathHealth turns periodic checks of an exit path into a stable usable
// state. A link failure takes the path down at once; probe failures need
// several consecutive misses. A path that went down returns only after
// consecutive successes spanning a hold-down, which doubles while the path
// keeps failing soon after recovering.
type pathHealth struct {
	cfg      oc.FibHealth
	usable   bool
	reason   string
	ok, fail int
	okSince  time.Time
	// penalty is the stability required before returning; zero until the
	// first failure.
	penalty time.Duration
	upSince time.Time
}

func newPathHealth(cfg oc.FibHealth) *pathHealth {
	return &pathHealth{cfg: cfg, reason: reasonStarting}
}

// settled reports whether the checks reached a verdict: up after enough
// successes, down after enough failures or a link down.
func (h *pathHealth) settled() bool {
	return h.usable || h.reason == reasonLinkDown || h.fail >= h.cfg.Failures
}

// observe records one check and reports whether the usable state changed.
func (h *pathHealth) observe(now time.Time, linkUp, reachable bool) bool {
	if !linkUp || !reachable {
		h.ok = 0
		h.fail++
		reason := reasonUnreachable
		if !linkUp {
			reason = reasonLinkDown
		}
		if !h.usable {
			h.reason = reason
			return false
		}
		if linkUp && h.fail < h.cfg.Failures {
			return false
		}
		switch {
		case h.penalty == 0:
			h.penalty = h.cfg.HoldDown
		case now.Sub(h.upSince) < h.cfg.MaxHoldDown:
			h.penalty = min(2*h.penalty, h.cfg.MaxHoldDown)
		default:
			h.penalty = h.cfg.HoldDown
		}
		h.usable, h.reason = false, reason
		return true
	}
	h.fail = 0
	if h.ok == 0 {
		h.okSince = now
	}
	h.ok++
	if h.usable || h.ok < h.cfg.Successes || now.Sub(h.okSince) < h.penalty {
		return false
	}
	h.usable, h.reason, h.upSince = true, reasonUsable, now
	return true
}
