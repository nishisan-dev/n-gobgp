package fib

import (
	"bytes"
	"context"
	"log/slog"
	"net/netip"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/osrg/gobgp/v4/api"
	"github.com/osrg/gobgp/v4/internal/pkg/table"
	"github.com/osrg/gobgp/v4/pkg/apiutil"
	"github.com/osrg/gobgp/v4/pkg/packet/bgp"
	"github.com/osrg/gobgp/v4/pkg/server"
)

type syncBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.b.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.b.String()
}

func startServer(t *testing.T) *server.BgpServer {
	t.Helper()
	s := server.NewBgpServer()
	go s.Serve()
	t.Cleanup(s.Stop)
	require.NoError(t, s.StartBgp(context.Background(), &api.StartBgpRequest{Global: &api.Global{Asn: 65001, RouterId: "192.0.2.254", ListenPort: -1}}))
	return s
}

func addRoute(t *testing.T, s *server.BgpServer, path *table.Path, withdraw bool) {
	t.Helper()
	nh, err := bgp.NewPathAttributeNextHop(path.GetNexthop())
	require.NoError(t, err)
	p := &apiutil.Path{Family: path.GetFamily(), Nlri: path.GetNlri(), Attrs: append(path.GetPathAttrs(), nh)}
	if withdraw {
		require.NoError(t, s.DeletePath(apiutil.DeletePathRequest{Paths: []*apiutil.Path{p}}))
		return
	}
	_, err = s.AddPath(apiutil.AddPathRequest{Paths: []*apiutil.Path{p}})
	require.NoError(t, err)
}

func TestManagerObservesBestPathDecisions(t *testing.T) {
	s := startServer(t)
	addRoute(t, s, testPath(t, "198.51.100.0/24", pathSpec{}), false)

	logs := &syncBuffer{}
	m, err := New(testConfig(t, testSets+testFib), slog.New(slog.NewJSONHandler(logs, nil)))
	require.NoError(t, err)
	require.NoError(t, m.Start(s))
	t.Cleanup(m.Close)

	customers := netip.MustParsePrefix("198.51.100.0/24")
	other := netip.MustParsePrefix("203.0.113.0/24")
	require.Eventually(t, func() bool { _, ok := m.Decisions()[customers]; return ok }, 5*time.Second, 10*time.Millisecond, "initial table")
	require.Equal(t, "customers", m.Decisions()[customers].Rule)

	addRoute(t, s, testPath(t, "203.0.113.0/24", pathSpec{}), false)
	require.Eventually(t, func() bool { return m.Decisions()[other].Rule == "catch-all" }, 5*time.Second, 10*time.Millisecond)

	// A changed attribute moves the destination to another rule.
	addRoute(t, s, testPath(t, "203.0.113.0/24", pathSpec{communities: []uint32{65002<<16 | 200}}), false)
	require.Eventually(t, func() bool { return m.Decisions()[other].Rule == "community-wan2" }, 5*time.Second, 10*time.Millisecond)

	addRoute(t, s, testPath(t, "198.51.100.0/24", pathSpec{}), true)
	require.Eventually(t, func() bool { _, ok := m.Decisions()[customers]; return !ok }, 5*time.Second, 10*time.Millisecond, "withdrawal")

	out := logs.String()
	require.Contains(t, out, `"msg":"fib decision"`)
	require.Contains(t, out, `"Rule":"community-wan2"`)
	require.Contains(t, out, `"msg":"fib decision removed"`)
	require.Contains(t, out, `"Mode":"observe"`)
}

func TestManagerTreatsStalePathsAsWithdrawn(t *testing.T) {
	m, err := New(testConfig(t, testSets+testFib), slog.New(slog.DiscardHandler))
	require.NoError(t, err)
	path := testPath(t, "198.51.100.0/24", pathSpec{})
	m.apply(server.BestPathEvent{Initial: true, Paths: []*table.Path{path}})
	require.Len(t, m.Decisions(), 1)

	stale := path.Clone(false)
	stale.MarkStale(true)
	m.apply(server.BestPathEvent{Paths: []*table.Path{stale}})
	require.Empty(t, m.Decisions())
}

func TestManagerIgnoresOtherFamiliesAndKeepsIgnoredDecisions(t *testing.T) {
	m, err := New(testConfig(t, testSets+testFib), slog.New(slog.DiscardHandler))
	require.NoError(t, err)
	m.apply(server.BestPathEvent{Paths: []*table.Path{
		testPath(t, "0.0.0.0/0", pathSpec{}),
		table.NewEOR(bgp.RF_IPv4_UC),
	}})
	require.Equal(t, map[netip.Prefix]Decision{
		netip.MustParsePrefix("0.0.0.0/0"): {Prefix: netip.MustParsePrefix("0.0.0.0/0"), Action: ActionIgnore, Reason: ReasonDefaultRoute},
	}, m.Decisions())
}

func TestNewRejectsDisabledConfiguration(t *testing.T) {
	_, err := New(testConfig(t, "[fib]\nenabled = false\n"), slog.New(slog.DiscardHandler))
	require.Error(t, err)
}
