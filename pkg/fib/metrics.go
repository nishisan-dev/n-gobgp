package fib

import (
	"github.com/prometheus/client_golang/prometheus"
)

type metrics struct {
	m            *Manager
	pathChanges  *prometheus.CounterVec
	kernelErrors prometheus.Counter
	repairs      prometheus.Counter

	usable, destinations, anchors, rules, reconciled *prometheus.Desc
}

func newMetrics(m *Manager) *metrics {
	desc := func(name, help string, labels ...string) *prometheus.Desc {
		return prometheus.NewDesc(prometheus.BuildFQName("gobgp", "fib", name), help, labels, nil)
	}
	return &metrics{
		m: m,
		pathChanges: prometheus.NewCounterVec(prometheus.CounterOpts{Namespace: "gobgp", Subsystem: "fib", Name: "exit_path_changes_total",
			Help: "Changes of the usable state of an exit path."}, []string{"exit_path"}),
		kernelErrors: prometheus.NewCounter(prometheus.CounterOpts{Namespace: "gobgp", Subsystem: "fib", Name: "kernel_errors_total",
			Help: "Failed kernel operations, including conflicts with other owners."}),
		repairs: prometheus.NewCounter(prometheus.CounterOpts{Namespace: "gobgp", Subsystem: "fib", Name: "routes_repaired_total",
			Help: "Owned routes found missing or changed in the kernel and installed again."}),
		usable:       desc("exit_path_usable", "Whether an exit path is usable (1) or not (0).", "exit_path"),
		destinations: desc("destinations", "Destinations by kernel status.", "status"),
		anchors:      desc("anchors", "Anchors by kernel status.", "status"),
		rules:        desc("traffic_rules", "Traffic and exemption rules by kernel status.", "status"),
		reconciled:   desc("reconciled", "Whether the routes of the previous run were reconciled."),
	}
}

func (x *metrics) pathChanged(name string) { x.pathChanges.WithLabelValues(name).Inc() }
func (x *metrics) kernelError()            { x.kernelErrors.Inc() }
func (x *metrics) repaired()               { x.repairs.Inc() }

func (x *metrics) Describe(ch chan<- *prometheus.Desc) {
	x.pathChanges.Describe(ch)
	x.kernelErrors.Describe(ch)
	x.repairs.Describe(ch)
	for _, d := range []*prometheus.Desc{x.usable, x.destinations, x.anchors, x.rules, x.reconciled} {
		ch <- d
	}
}

func (x *metrics) Collect(ch chan<- prometheus.Metric) {
	x.pathChanges.Collect(ch)
	x.kernelErrors.Collect(ch)
	x.repairs.Collect(ch)
	m := x.m
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.paths != nil {
		for _, p := range m.config.ExitPaths {
			v := 0.0
			if m.paths.usable(p.Name) {
				v = 1
			}
			ch <- prometheus.MustNewConstMetric(x.usable, prometheus.GaugeValue, v, p.Name)
		}
	}
	count := func(desc *prometheus.Desc, statuses []string) {
		n := map[string]float64{}
		for _, s := range statuses {
			n[s]++
		}
		for s, v := range n {
			ch <- prometheus.MustNewConstMetric(desc, prometheus.GaugeValue, v, s)
		}
	}
	var statuses []string
	for _, e := range m.entries {
		statuses = append(statuses, e.status)
	}
	count(x.destinations, statuses)
	statuses = nil
	for _, a := range m.anchors {
		statuses = append(statuses, a.status)
	}
	count(x.anchors, statuses)
	statuses = nil
	for _, s := range m.rules {
		statuses = append(statuses, s)
	}
	count(x.rules, statuses)
	v := 0.0
	if m.reconciled {
		v = 1
	}
	ch <- prometheus.MustNewConstMetric(x.reconciled, prometheus.GaugeValue, v)
}

// Register adds the fib metrics to a Prometheus registry.
func (m *Manager) Register(r prometheus.Registerer) error {
	return r.Register(m.metrics)
}
