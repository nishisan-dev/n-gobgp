package server

import (
	"log/slog"
	"net/netip"
	"time"

	"github.com/osrg/gobgp/v4/pkg/packet/bgp"
)

type refreshDeadline struct {
	timer *time.Timer
}

// BoRR always refreshes the stale membership, as RFC 7313 requires. Repeated
// BoRRs in an unfinished cycle cannot extend the original retention deadline.
// The caller is the peer's serialized FSM callback, under shared.mu.RLock.
func (s *BgpServer) beginRefresh(p *peer, f bgp.Family) {
	p.beginRefresh(f)
	if s.refreshStaleTime <= 0 || p.refreshDeadlines[f] != nil {
		return
	}
	if p.refreshDeadlines == nil {
		p.refreshDeadlines = make(map[bgp.Family]*refreshDeadline)
	}
	d := &refreshDeadline{}
	p.refreshDeadlines[f] = d
	d.timer = time.AfterFunc(s.refreshStaleTime, func() {
		_ = s.mgmtOperation(func() error {
			s.expireRefresh(p, f, d)
			return nil
		}, false)
	})
}

// The dispatcher excludes FSM callbacks. Identity checks also reject a timer
// already queued when EoRR, a disconnect or peer replacement canceled it.
func (s *BgpServer) expireRefresh(p *peer, f bgp.Family, d *refreshDeadline) {
	if p.refreshDeadlines[f] != d || s.neighborMap[netip.MustParseAddr(p.ID())] != p || p.State() != bgp.BGP_FSM_ESTABLISHED {
		return
	}
	removed := s.endRefresh(p, f)
	p.fsm.logger.Warn("route refresh stale retention expired",
		slog.String("Family", f.String()), slog.Int("Removed", removed), slog.Duration("Limit", s.refreshStaleTime))
}

func (s *BgpServer) endRefresh(p *peer, f bgp.Family) int {
	if d := p.refreshDeadlines[f]; d != nil {
		d.timer.Stop()
		delete(p.refreshDeadlines, f)
	}
	removed, accepted := p.endRefresh(f)
	s.notifyAdjInWithdrawWatcher(p, removed)
	s.propagateUpdate(p, accepted)
	return len(removed)
}

func (p *peer) cancelRefresh() {
	for _, d := range p.refreshDeadlines {
		d.timer.Stop()
	}
	p.refreshDeadlines = nil
	p.refreshPending = nil
}
