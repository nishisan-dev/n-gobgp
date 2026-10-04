package kafka

import (
	"github.com/osrg/gobgp/v4/pkg/event"
	"github.com/osrg/gobgp/v4/pkg/packet/bgp"
	"github.com/prometheus/client_golang/prometheus"
)

type metrics struct {
	received                                                                             *prometheus.CounterVec
	ingressDropped                                                                       prometheus.Counter
	matched, published, dropped, serializationErrors, publishErrors, persisted, replayed *prometheus.CounterVec
	storageErrors, snapshotFailures                                                      *prometheus.CounterVec
	latency                                                                              *prometheus.HistogramVec
}

func newMetrics() *metrics {
	labels := []string{"cluster", "sink", "topic", "family", "event_type"}
	counter := func(name, help string) *prometheus.CounterVec {
		return prometheus.NewCounterVec(prometheus.CounterOpts{Namespace: "gobgp", Subsystem: "kafka", Name: name, Help: help}, labels)
	}
	return &metrics{
		received:            prometheus.NewCounterVec(prometheus.CounterOpts{Namespace: "gobgp", Subsystem: "kafka", Name: "events_received_total", Help: "Classified observations received from the native watcher."}, []string{"family", "event_type"}),
		ingressDropped:      prometheus.NewCounter(prometheus.CounterOpts{Namespace: "gobgp", Subsystem: "kafka", Name: "ingress_dropped_total", Help: "Internal watcher notifications discarded before classification."}),
		matched:             counter("events_matched_total", "Observations matched to a sink."),
		published:           counter("events_published_total", "Kafka-acknowledged records, including replay and duplicates."),
		dropped:             counter("events_dropped_total", "Matched records discarded before durable persistence."),
		serializationErrors: counter("serialization_errors_total", "Protobuf conversion or serialization failures."),
		publishErrors:       counter("publish_errors_total", "Failed Kafka delivery attempts."),
		persisted:           counter("events_persisted_total", "Records committed to durable storage."),
		replayed:            counter("events_replayed_total", "Delivery attempts for records recovered at startup."),
		snapshotFailures:    prometheus.NewCounterVec(prometheus.CounterOpts{Namespace: "gobgp", Subsystem: "kafka", Name: "snapshot_failures_total", Help: "Initial snapshots invalidated by loss or serialization/storage errors."}, []string{"cluster", "sink", "topic"}),
		storageErrors:       prometheus.NewCounterVec(prometheus.CounterOpts{Namespace: "gobgp", Subsystem: "kafka", Name: "storage_errors_total", Help: "Durable storage failures."}, []string{"cluster", "sink", "topic"}),
		latency:             prometheus.NewHistogramVec(prometheus.HistogramOpts{Namespace: "gobgp", Subsystem: "kafka", Name: "publish_latency_seconds", Help: "Time to acknowledge a Kafka delivery attempt.", Buckets: prometheus.DefBuckets}, labels),
	}
}

func (m *metrics) collectors() []prometheus.Collector {
	return []prometheus.Collector{m.received, m.ingressDropped, m.matched, m.published, m.dropped, m.serializationErrors, m.publishErrors, m.persisted, m.replayed, m.storageErrors, m.snapshotFailures, m.latency}
}

func labels(s *sink, meta event.Metadata) []string {
	return []string{s.config.Cluster, s.name, s.config.Topic, metricFamily(meta.Family), meta.EventType}
}

func metricFamily(f string) string {
	if f == "" {
		return "none"
	}
	if _, err := bgp.GetFamily(f); err != nil {
		return "unknown"
	}
	return f
}

func deliveryLabels(s *sink, r *record) []string {
	out := labels(s, r.Metadata)
	if s.config.ReplayDestination == "original" && r.Destination != nil {
		out[0] = r.Destination.Cluster
		out[2] = r.Destination.Topic
	}
	return out
}
