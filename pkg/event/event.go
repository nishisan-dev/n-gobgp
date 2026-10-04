// Package event defines operational metadata and contracts for BGP event sinks.
// BGP data is always carried by the existing GoBGP protobuf model.
package event

import (
	"context"
	"time"

	"github.com/osrg/gobgp/v4/api"
)

var (
	Sources = []string{"adj-in", "post-policy", "best", "peer"}
	Types   = []string{"route", "withdraw", "peer-state", "eor"}
)

// Metadata describes an observation, rather than a second representation of BGP.
type Metadata struct {
	SnapshotID    string `json:",omitempty"`
	SnapshotPhase string `json:",omitempty"`
	EventType     string
	Family        string
	Source        string
	PeerAddress   string
	PeerASN       uint32
	ObservedAt    time.Time
}

type Observation struct {
	Message  *api.WatchEventResponse
	Metadata Metadata
	Err      error
}

type SubscriptionOptions struct {
	InitialDump bool
	// Nil selects every subscribed source/family for bootstrap. Delta admission
	// is unaffected by these snapshot-only restrictions.
	InitialSources  []string
	InitialFamilies []string
	Sources         []string
	Capacity        int
	// OnDrop runs in the BGP notification path and must never block.
	OnDrop func()
}

// Stop detaches the subscription. Done closes after queued observations drain.
type Subscription interface {
	Stop()
	Done() <-chan struct{}
	QueueSize() int
}

type Source interface {
	SubscribeEvents(SubscriptionOptions, func(Observation)) (Subscription, error)
}

type Serializer interface {
	Serialize(*api.WatchEventResponse) ([]byte, error)
	ContentType() string
}

// Sink admission is non-blocking; persistence and delivery happen asynchronously.
type Sink interface {
	Name() string
	Match(Metadata) bool
	Offer(Observation) bool
	Close(context.Context) error
}
