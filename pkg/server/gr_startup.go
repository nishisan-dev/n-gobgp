package server

import (
	"context"
	"fmt"
	"net/netip"
	"slices"
	"time"

	"github.com/osrg/gobgp/v4/api"
	"github.com/osrg/gobgp/v4/internal/pkg/table"
	"github.com/osrg/gobgp/v4/pkg/packet/bgp"
)

// The configuration barrier controls BGP session activation, not rollback of
// management operations. Generation tokens prevent a delayed completion from
// releasing a subsequent StartBgp lifecycle.
func (s *BgpServer) initialConfigurationState() *api.InitialConfigurationState {
	return &api.InitialConfigurationState{Generation: s.initialConfigurationGeneration, Pending: s.initialConfigurationGate != nil}
}

func (s *BgpServer) CompleteInitialConfiguration(ctx context.Context, r *api.CompleteInitialConfigurationRequest) (*api.CompleteInitialConfigurationResponse, error) {
	if r == nil || r.Generation == "" {
		return nil, fmt.Errorf("initial configuration generation is required")
	}
	var response *api.CompleteInitialConfigurationResponse
	err := s.mgmtOperationWithContext(ctx, func() error {
		if r.Generation != s.initialConfigurationGeneration {
			return fmt.Errorf("initial configuration generation does not match the active BGP lifecycle")
		}
		if s.initialConfigurationGate != nil {
			close(s.initialConfigurationGate)
			s.initialConfigurationGate = nil
			s.queueLocalRestartCheck()
		}
		response = &api.CompleteInitialConfigurationResponse{InitialConfiguration: s.initialConfigurationState()}
		return nil
	}, true)
	return response, err
}

type localRestartPathKey struct {
	family      bgp.Family
	prefix      string
	id          uint32
	routeServer bool
	as          uint32
	address     netip.Addr
	routerID    netip.Addr
	localID     netip.Addr
}

type localRestartPath struct {
	peer *peer
	path *table.Path
}

type localRestartFamily struct {
	deferral time.Duration
	timer    *time.Timer
	expired  bool
	paths    map[localRestartPathKey]localRestartPath
}

type localRestartDestination struct {
	rib *table.TableManager
	key table.PathDestLocalKey
}

type localRestartDelta struct {
	peer   *peer
	path   *table.Path
	update *table.Update
}

func (s *BgpServer) registerLocalRestart(p *peer) {
	p.fsm.lock.Lock()
	c := p.fsm.pConf.ReadOnly()
	if !c.GracefulRestart.Config.Enabled || !c.GracefulRestart.State.LocalRestarting || p.localRestartRegistered {
		p.fsm.lock.Unlock()
		return
	}
	p.localRestartRegistered = true
	p.fsm.lock.Unlock()
	d := time.Duration(c.GracefulRestart.Config.DeferralTime) * time.Second
	s.localRestartMu.Lock()
	defer s.localRestartMu.Unlock()
	if s.localRestartFamilies == nil {
		s.localRestartFamilies = make(map[bgp.Family]*localRestartFamily)
	}
	s.localRestartActive.Store(true)
	for _, f := range p.toGlobalFamilies(p.configuredRFlist()) {
		if state := s.localRestartFamilies[f]; state != nil {
			state.deferral = min(state.deferral, d)
		} else {
			s.localRestartFamilies[f] = &localRestartFamily{deferral: d, paths: make(map[localRestartPathKey]localRestartPath)}
		}
	}
}

func (s *BgpServer) localRestartBlocked(p *peer, family bgp.Family) bool {
	if !s.localRestartActive.Load() {
		return false
	}
	if p != nil {
		// Callers handling RIB updates already supply the global VPN family.
		switch family {
		case bgp.RF_IPv4_UC, bgp.RF_IPv6_UC, bgp.RF_FS_IPv4_UC, bgp.RF_FS_IPv6_UC:
			family = p.toGlobalFamilies([]bgp.Family{family})[0]
		}
	}
	s.localRestartMu.Lock()
	defer s.localRestartMu.Unlock()
	return s.localRestartFamilies[family] != nil
}

// Membership learned during local restart is staged like any other family.
// A completed VPN family cannot export its initial view before that staged
// membership has entered the routing table.
func (s *BgpServer) localRestartExportBlocked(p *peer, family bgp.Family) bool {
	return s.localRestartBlocked(p, family) || family != bgp.RF_RTC_UC && p.IsFamilyEnabled(bgp.RF_RTC_UC) && s.localRestartBlocked(p, bgp.RF_RTC_UC)
}

// Keep only the last action for each input route, including withdrawals.
// Adj-RIB-In observation continues while selection and exports are deferred.
func (s *BgpServer) deferLocalRestartPath(p *peer, path *table.Path) bool {
	if !s.localRestartActive.Load() {
		return false
	}
	rib, candidate := s.localRestartInput(p, path)
	if candidate == nil {
		return false
	}
	f := candidate.GetFamily()
	if s.localRestartReplay == f {
		return false
	}
	s.localRestartMu.Lock()
	defer s.localRestartMu.Unlock()
	state := s.localRestartFamilies[f]
	if state == nil {
		return false
	}
	source := candidate.GetSource()
	key := localRestartPathKey{family: f, prefix: candidate.GetPrefix(), id: candidate.RemoteID(), routeServer: p != nil && p.isRouteServerClient(), as: source.AS, address: source.Address, routerID: source.ID, localID: source.LocalID}
	if path.IsWithdraw {
		installed := false
		if destination := rib.GetDestination(candidate); destination != nil {
			for _, known := range destination.GetAllKnownPathList() {
				installed = installed || candidate.EqualBySourceAndPathID(known)
			}
		}
		if !installed {
			delete(state.paths, key)
			return true
		}
	}
	state.paths[key] = localRestartPath{peer: p, path: path.Clone(path.IsWithdraw)}
	return true
}

func (s *BgpServer) localRestartInput(p *peer, path *table.Path) (*table.TableManager, *table.Path) {
	if p != nil {
		if p.isRouteServerClient() {
			return s.rsRib, path
		}
		if vrf := p.fsm.pConf.ReadOnly().Config.Vrf; vrf != "" {
			if v, ok := s.globalRib.GetVrf(vrf); ok {
				return s.globalRib, path.ToGlobal(v)
			}
			return s.globalRib, nil
		}
	}
	return s.globalRib, path
}

func (s *BgpServer) localRestartLocalPaths() []*table.Path {
	s.localRestartMu.Lock()
	defer s.localRestartMu.Unlock()
	var paths []*table.Path
	for _, state := range s.localRestartFamilies {
		for _, pending := range state.paths {
			if pending.peer == nil && pending.path.IsLocal() && !pending.path.IsWithdraw {
				paths = append(paths, pending.path)
			}
		}
	}
	return paths
}

func (s *BgpServer) queueLocalRestartCheck() {
	s.localRestartMu.Lock()
	active := len(s.localRestartFamilies) != 0
	s.localRestartMu.Unlock()
	if !active || !s.localRestartScheduled.CompareAndSwap(false, true) {
		return
	}
	go func() {
		_ = s.mgmtOperation(func() error {
			s.localRestartScheduled.Store(false)
			s.advanceLocalRestart()
			return nil
		}, false)
	}()
}

// A GR helper with no preservation tuples still sends EOR. Conversely, an
// R-bit peer is excluded without fabricating an inbound completion marker.
func (p *peer) waitingForLocalRestartEOR(globalFamily bgp.Family) bool {
	c := p.fsm.pConf.ReadOnly()
	if !c.GracefulRestart.Config.Enabled || p.AdminState() != adminStateUp {
		return false
	}
	if p.State() != bgp.BGP_FSM_ESTABLISHED {
		return slices.Contains(p.toGlobalFamilies(p.configuredRFlist()), globalFamily)
	}
	if !c.GracefulRestart.State.Enabled {
		return false
	}
	p.fsm.lock.Lock()
	gr := p.fsm.capMap[bgp.BGP_CAP_GRACEFUL_RESTART]
	restarting := len(gr) > 0 && gr[len(gr)-1].(*bgp.CapGracefulRestart).Flags&0x08 != 0
	p.fsm.lock.Unlock()
	if restarting {
		return false
	}
	negotiated := p.negotiatedRFList()
	for _, af := range c.AfiSafis {
		if slices.Contains(negotiated, af.State.Family) && p.toGlobalFamilies([]bgp.Family{af.State.Family})[0] == globalFamily {
			return !af.MpGracefulRestart.State.EndOfRibReceived
		}
	}
	return false
}

// Runs on the dispatcher. This is one selection deadline per family, shared
// by all peers and preserved across disconnects and later establishments.
func (s *BgpServer) advanceLocalRestart() {
	s.localRestartMu.Lock()
	states := make(map[bgp.Family]*localRestartFamily, len(s.localRestartFamilies))
	for f, state := range s.localRestartFamilies {
		states[f] = state
	}
	s.localRestartMu.Unlock()
	for f, state := range states {
		ready, established := true, false
		for _, p := range s.neighborMap {
			if p.waitingForLocalRestartEOR(f) {
				ready = false
			}
			if p.State() == bgp.BGP_FSM_ESTABLISHED && slices.Contains(p.toGlobalFamilies(p.negotiatedRFList()), f) {
				established = true
			}
		}
		if ready || state.expired {
			s.finishLocalRestartFamily(f, state)
			continue
		}
		if established && state.timer == nil {
			state.timer = time.AfterFunc(state.deferral, func() {
				_ = s.mgmtOperation(func() error {
					s.localRestartMu.Lock()
					current := s.localRestartFamilies[f] == state
					s.localRestartMu.Unlock()
					if current {
						state.expired = true
						s.advanceLocalRestart()
					}
					return nil
				}, false)
			})
		}
	}
}

func (s *BgpServer) collectLocalRestartUpdates(rib *table.TableManager, p *peer, path *table.Path, updates []*table.Update) {
	for _, update := range updates {
		representative := path
		if len(update.KnownPathList) > 0 {
			representative = update.KnownPathList[0]
		} else if len(update.OldKnownPathList) > 0 {
			representative = update.OldKnownPathList[0].Clone(true)
		}
		key := localRestartDestination{rib: rib, key: representative.GetDestLocalKey()}
		if old := s.localRestartBatch[key]; old != nil {
			old.update.KnownPathList = update.KnownPathList
			old.path, old.peer = representative, p
		} else {
			u := *update
			s.localRestartBatch[key] = &localRestartDelta{peer: p, path: representative, update: &u}
		}
	}
}

func (s *BgpServer) finishLocalRestartFamily(f bgp.Family, state *localRestartFamily) {
	if state.timer != nil {
		state.timer.Stop()
	}
	s.localRestartReplay = f
	s.localRestartBatch = make(map[localRestartDestination]*localRestartDelta)
	for _, pending := range state.paths {
		s.propagateUpdate(pending.peer, []*table.Path{pending.path})
	}
	batch := s.localRestartBatch
	s.localRestartBatch = nil
	s.localRestartReplay = 0
	s.localRestartMu.Lock()
	delete(s.localRestartFamilies, f)
	if len(s.localRestartFamilies) == 0 {
		s.localRestartActive.Store(false)
	}
	s.localRestartMu.Unlock()
	// All candidates are installed before publishing best-path changes. Each
	// destination retains the old view from before the batch, not its middle.
	for key, delta := range batch {
		s.propagateUpdateToNeighbors(key.rib, delta.peer, delta.path, []*table.Update{delta.update}, true)
	}
	for _, p := range s.neighborMap {
		var families []bgp.Family
		for _, native := range p.negotiatedRFList() {
			if p.toGlobalFamilies([]bgp.Family{native})[0] == f || f == bgp.RF_RTC_UC && p.IsFamilyEnabled(bgp.RF_RTC_UC) && !s.localRestartBlocked(p, native) {
				families = append(families, native)
			}
		}
		if p.State() == bgp.BGP_FSM_ESTABLISHED && len(families) > 0 {
			s.getBestFromLocalCallback(p, families, true, true, func(paths, _ []*table.Path) {
				p.updateRoutes(paths...)
				sendfsmOutgoingMsg(p, paths)
			})
			for _, family := range families {
				if _, requested := p.startupRefresh.LoadAndDelete(family); requested {
					s.getBestFromLocalCallback(p, []bgp.Family{family}, false, true, func(paths, _ []*table.Path) { sendRefreshPaths(p, []bgp.Family{family}, paths) })
				}
			}
		}
		pending := false
		for _, family := range p.configuredRFlist() {
			pending = pending || s.localRestartBlocked(p, family)
		}
		if !pending {
			p.fsm.lock.Lock()
			c := p.fsm.pConf.ReadCopy()
			c.GracefulRestart.State.LocalRestarting = false
			p.fsm.pConf.Update(&c)
			p.fsm.lock.Unlock()
		}
	}
}

func (s *BgpServer) cancelLocalRestart() {
	s.localRestartMu.Lock()
	defer s.localRestartMu.Unlock()
	for _, state := range s.localRestartFamilies {
		if state.timer != nil {
			state.timer.Stop()
		}
	}
	s.localRestartFamilies = nil
	s.localRestartActive.Store(false)
}
