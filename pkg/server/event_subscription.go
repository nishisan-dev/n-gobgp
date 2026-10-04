package server

import (
	"fmt"
	"net/netip"
	"slices"
	"sort"
	"time"

	"github.com/google/uuid"

	"github.com/osrg/gobgp/v4/api"
	"github.com/osrg/gobgp/v4/internal/pkg/table"
	"github.com/osrg/gobgp/v4/pkg/apiutil"
	"github.com/osrg/gobgp/v4/pkg/event"
	"github.com/osrg/gobgp/v4/pkg/packet/bgp"
)

type eventSubscription struct {
	watcher *watcher
	done    chan struct{}
}

func (s *eventSubscription) Stop()                 { s.watcher.Stop() }
func (s *eventSubscription) Done() <-chan struct{} { return s.done }
func (s *eventSubscription) QueueSize() int        { return len(s.watcher.realCh) }

// SubscribeEvents reuses the native watch registry with bounded admission.
// Conversion and callbacks run in a separate goroutine. Stop drains admission.
func (s *BgpServer) SubscribeEvents(options event.SubscriptionOptions, fn func(event.Observation)) (event.Subscription, error) {
	if options.Capacity < 1 || fn == nil {
		return nil, fmt.Errorf("event subscription requires positive capacity and callback")
	}
	var initial []watchEvent
	var initialFamilies []bgp.Family
	for _, name := range options.InitialFamilies {
		f, err := bgp.GetFamily(name)
		if err != nil {
			return nil, err
		}
		initialFamilies = append(initialFamilies, f)
	}
	initialSource := func(source string) bool {
		return options.InitialDump && (options.InitialSources == nil || slices.Contains(options.InitialSources, source))
	}
	opts := []WatchOption{func(o *watchOptions) {
		o.boundedSize = options.Capacity
		o.onDrop = options.OnDrop
		o.noInitialPeer = !initialSource("peer")
		o.initialFamilies = initialFamilies
		o.limitInitialFamilies = options.InitialFamilies != nil
		if options.InitialDump {
			o.initialEvents = &initial
		}
	}}
	for _, source := range options.Sources {
		switch source {
		case "adj-in":
			opts = append(opts, WatchUpdate(initialSource(source), "", ""), WatchAdjInWithdraw(), WatchEor(initialSource(source)))
		case "post-policy":
			opts = append(opts, WatchPostUpdate(initialSource(source), "", ""))
		case "best":
			opts = append(opts, WatchBestPath(initialSource(source)))
		case "peer":
			opts = append(opts, WatchPeer())
		default:
			return nil, fmt.Errorf("unsupported event source %q", source)
		}
	}
	if len(options.Sources) == 0 {
		return nil, fmt.Errorf("no event sources")
	}
	w, err := s.watch(opts...)
	if err != nil {
		return nil, err
	}
	sub := &eventSubscription{w, make(chan struct{})}
	go func() {
		defer close(sub.done)
		if options.InitialDump {
			id, now := uuid.NewString(), time.Now()
			control := func(phase string) {
				fn(event.Observation{Message: &api.WatchEventResponse{}, Metadata: event.Metadata{EventType: "snapshot", SnapshotID: id, SnapshotPhase: phase, ObservedAt: now}})
			}
			control("begin")
			for _, ev := range initial {
				// Native init updates include synthetic EOR messages; route lists
				// are complete even when the ingress channel has capacity one.
				if v, ok := ev.(*watchEventUpdate); ok {
					copyEvent := *v
					copyEvent.Init = false
					copyEvent.Timestamp = now
					copyEvent.PathList = append([]*table.Path(nil), v.PathList...)
					sort.SliceStable(copyEvent.PathList, func(i, j int) bool {
						return copyEvent.PathList[i].GetNlri().String() < copyEvent.PathList[j].GetNlri().String()
					})
					ev = &copyEvent
				}
				if v, ok := ev.(*watchEventPeer); ok && v.Type == apiutil.PEER_EVENT_INIT {
					response, err := watchPeerToAPI(v)
					fn(event.Observation{Message: response, Err: err, Metadata: event.Metadata{EventType: "peer-state", Source: "peer", PeerAddress: peerAddress(v.PeerAddress), PeerASN: v.PeerAS, ObservedAt: now, SnapshotID: id, SnapshotPhase: "data"}})
					continue
				}
				observationsFromWatch(ev, func(v event.Observation) { v.Metadata.SnapshotID = id; v.Metadata.SnapshotPhase = "data"; fn(v) })
			}
			initial = nil
			control("end")
		}
		for ev := range w.Event() {
			observationsFromWatch(ev, fn)
		}
	}()
	return sub, nil
}

func peerAddress(a netip.Addr) string {
	if !a.IsValid() {
		return ""
	}
	return a.Unmap().String()
}

func observationsFromWatch(ev watchEvent, fn func(event.Observation)) {
	sendPaths := func(paths []*table.Path, source string, when time.Time) {
		for _, path := range paths {
			if path == nil {
				continue
			}
			kind := "route"
			if path.IsWithdraw {
				kind = "withdraw"
			}
			if path.IsEOR() {
				kind = "eor"
			}
			m := event.Metadata{EventType: kind, Family: path.GetFamily().String(), Source: source, ObservedAt: when}
			if info := path.GetSource(); info != nil {
				m.PeerAddress = peerAddress(info.Address)
				m.PeerASN = info.AS
			}
			p := toPathApiUtil(path)
			// Use the same protobuf conversion as gRPC, checking errors before delivery.
			var nlri *api.NLRI
			var err error
			if p.Nlri != nil {
				nlri, err = apiutil.MarshalNLRI(p.Nlri)
			}
			var attrs []*api.Attribute
			if err == nil {
				attrs, err = apiutil.MarshalPathAttributes(p.Attrs)
			}
			if err != nil {
				fn(event.Observation{Metadata: m, Err: err})
				continue
			}
			response := &api.WatchEventResponse{Event: &api.WatchEventResponse_Table{Table: &api.WatchEventResponse_TableEvent{Paths: []*api.Path{toPathAPI(nil, nil, nlri, attrs, p)}}}}
			fn(event.Observation{Message: response, Metadata: m})
		}
	}
	switch v := ev.(type) {
	case *watchEventUpdate:
		if v.Init {
			return
		}
		source := "adj-in"
		if v.PostPolicy {
			source = "post-policy"
		}
		sendPaths(v.PathList, source, v.Timestamp)
	case *watchEventBestPath:
		sendPaths(locRIBPathsForBMP(v), "best", v.Timestamp)
	case *watchEventEor:
		path := table.NewEOR(v.Family)
		path.SetSource(v.PeerInfo)
		sendPaths([]*table.Path{path}, "adj-in", v.Timestamp)
	case *watchEventPeer:
		if v.Type != apiutil.PEER_EVENT_STATE {
			return
		}
		response, err := watchPeerToAPI(v)
		fn(event.Observation{Message: response, Err: err, Metadata: event.Metadata{EventType: "peer-state", Source: "peer", PeerAddress: peerAddress(v.PeerAddress), PeerASN: v.PeerAS, ObservedAt: v.Timestamp}})
	}
}

func watchPeerToAPI(v *watchEventPeer) (*api.WatchEventResponse, error) {
	remote, err := apiutil.MarshalCapabilities(v.RemoteCap)
	if err != nil {
		return nil, err
	}
	local, err := apiutil.MarshalCapabilities(v.LocalCap)
	if err != nil {
		return nil, err
	}
	admin := api.PeerState_ADMIN_STATE_UP
	switch v.AdminState {
	case adminStateDown:
		admin = api.PeerState_ADMIN_STATE_DOWN
	case adminStatePfxCt:
		admin = api.PeerState_ADMIN_STATE_PFX_CT
	}
	reason, message := convertFSMStateReasonToAPI(v.StateReason)
	return &api.WatchEventResponse{Event: &api.WatchEventResponse_Peer{Peer: &api.WatchEventResponse_PeerEvent{
		Type: api.WatchEventResponse_PeerEvent_Type(v.Type),
		Peer: &api.Peer{
			Conf:      &api.PeerConf{PeerAsn: v.PeerAS, LocalAsn: v.LocalAS, NeighborAddress: apiutil.AddrOrEmpty(v.PeerAddress), NeighborInterface: v.PeerInterface, PeerGroup: v.PeerGroup},
			State:     &api.PeerState{PeerAsn: v.PeerAS, LocalAsn: v.LocalAS, NeighborAddress: apiutil.AddrOrEmpty(v.PeerAddress), SessionState: api.PeerState_SessionState(int(v.State) + 1), AdminState: admin, RouterId: apiutil.AddrOrEmpty(v.PeerID), PeerGroup: v.PeerGroup, RemoteCap: remote, LocalCap: local, DisconnectReason: reason, DisconnectMessage: message},
			Transport: &api.Transport{LocalAddress: apiutil.AddrOrEmpty(v.LocalAddress), LocalPort: uint32(v.LocalPort), RemotePort: uint32(v.PeerPort)},
		},
	}}}, nil
}

var _ event.Source = (*BgpServer)(nil)
