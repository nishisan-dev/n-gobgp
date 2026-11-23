package fib

import (
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

func TestMetricsDescribeTheKernelState(t *testing.T) {
	k, ps := newFakeKernel(), newFakePaths("wan1", "wan2")
	k.seed(route(100, "198.51.100.0/24", "10.0.9.1", "eth9"), 4)
	m := newTestManager(t, enforce(testSets+testFib+testAnchor), k, ps)
	announce(m, testPath(t, "198.51.100.0/24", pathSpec{}), testPath(t, "203.0.113.0/24", pathSpec{}))
	pathDown(m, ps, "wan2", false)

	reg := prometheus.NewRegistry()
	require.NoError(t, m.Register(reg))
	expected := `
# HELP gobgp_fib_exit_path_usable Whether an exit path is usable (1) or not (0).
# TYPE gobgp_fib_exit_path_usable gauge
gobgp_fib_exit_path_usable{exit_path="wan1"} 1
gobgp_fib_exit_path_usable{exit_path="wan2"} 0
# HELP gobgp_fib_exit_path_changes_total Changes of the usable state of an exit path.
# TYPE gobgp_fib_exit_path_changes_total counter
gobgp_fib_exit_path_changes_total{exit_path="wan2"} 1
# HELP gobgp_fib_destinations Destinations by kernel status.
# TYPE gobgp_fib_destinations gauge
gobgp_fib_destinations{status="conflict"} 1
gobgp_fib_destinations{status="installed"} 1
# HELP gobgp_fib_anchors Anchors by kernel status.
# TYPE gobgp_fib_anchors gauge
gobgp_fib_anchors{status="installed"} 1
# HELP gobgp_fib_reconciled Whether the routes of the previous run were reconciled.
# TYPE gobgp_fib_reconciled gauge
gobgp_fib_reconciled 1
`
	require.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(expected),
		"gobgp_fib_exit_path_usable", "gobgp_fib_exit_path_changes_total", "gobgp_fib_destinations", "gobgp_fib_anchors", "gobgp_fib_reconciled"))
	require.Equal(t, 1.0, testutil.ToFloat64(m.metrics.kernelErrors), "the conflict counts as a failed kernel operation")
}
