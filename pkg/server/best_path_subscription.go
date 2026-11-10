package server

import (
	"fmt"
	"net/netip"
	"slices"

	"github.com/osrg/gobgp/v4/internal/pkg/table"
	"github.com/osrg/gobgp/v4/pkg/apiutil"
	"github.com/osrg/gobgp/v4/pkg/packet/bgp"
)

// BestPathEvent carries global RIB best path changes in their native form.
// A withdrawn path means the destination has no best path anymore. The first
// event of a subscription has Initial set and holds the table at subscription
// time, possibly empty.
type BestPathEvent struct {
	Initial bool
	Paths   []*table.Path
	// Peer is set, without paths, when a session becomes or stops being
	// established, and when an established peer sends End-of-RIB for one of
	// the subscribed families.
	Peer *PeerEvent
}

type PeerEvent struct {
	Address     netip.Addr
	Established bool
	EndOfRib    bool
	Family      bgp.Family
}

type BestPathSubscription struct {
	watcher *watcher
	done    chan struct{}
}

// Stop detaches the subscription. Done closes when the callback has returned
// for the last time.
func (s *BestPathSubscription) Stop()                 { s.watcher.Stop() }
func (s *BestPathSubscription) Done() <-chan struct{} { return s.done }

// SubscribeBestPaths delivers the best path changes of the given families to
// fn, from a single goroutine and in RIB order, together with the session
// and End-of-RIB events that tell when the RIB is complete. Unlike SubscribeEvents, the
// queue is unbounded and never drops: consumers that program forwarding state
// cannot recover from a lost withdrawal.
func (s *BgpServer) SubscribeBestPaths(families []bgp.Family, fn func(BestPathEvent)) (*BestPathSubscription, error) {
	if len(families) == 0 || fn == nil {
		return nil, fmt.Errorf("best path subscription requires families and a callback")
	}
	var initial []watchEvent
	w, err := s.watch(WatchBestPath(true), WatchEor(false), WatchPeer(), func(o *watchOptions) {
		o.noInitialPeer = true
		o.initialEvents = &initial
		o.initialFamilies = families
		o.limitInitialFamilies = true
	})
	if err != nil {
		return nil, err
	}
	sub := &BestPathSubscription{watcher: w, done: make(chan struct{})}
	filter := func(paths []*table.Path) []*table.Path {
		l := make([]*table.Path, 0, len(paths))
		for _, p := range paths {
			if p != nil && slices.Contains(families, p.GetFamily()) {
				l = append(l, p)
			}
		}
		return l
	}
	go func() {
		defer close(sub.done)
		first := BestPathEvent{Initial: true}
		for _, ev := range initial {
			if v, ok := ev.(*watchEventBestPath); ok {
				first.Paths = append(first.Paths, filter(v.PathList)...)
			}
		}
		initial = nil
		fn(first)
		for ev := range w.Event() {
			switch v := ev.(type) {
			case *watchEventBestPath:
				if paths := filter(v.PathList); len(paths) > 0 {
					fn(BestPathEvent{Paths: paths})
				}
			case *watchEventEor:
				if slices.Contains(families, v.Family) && v.PeerInfo != nil {
					fn(BestPathEvent{Peer: &PeerEvent{Address: v.PeerInfo.Address, Established: true, EndOfRib: true, Family: v.Family}})
				}
			case *watchEventPeer:
				up, wasUp := v.State == bgp.BGP_FSM_ESTABLISHED, v.OldState == bgp.BGP_FSM_ESTABLISHED
				if v.Type == apiutil.PEER_EVENT_STATE && up != wasUp {
					fn(BestPathEvent{Peer: &PeerEvent{Address: v.PeerAddress, Established: up}})
				}
			}
		}
	}()
	return sub, nil
}
