package fib

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/osrg/gobgp/v4/pkg/config/oc"
)

var testHealth = oc.FibHealth{Interval: time.Second, Failures: 3, Successes: 2, HoldDown: 10 * time.Second, MaxHoldDown: 40 * time.Second}

type healthClock struct{ now time.Time }

func (c *healthClock) tick() time.Time {
	c.now = c.now.Add(time.Second)
	return c.now
}

// run feeds n identical observations and returns the transitions seen.
func (c *healthClock) run(h *pathHealth, n int, link, reachable bool) (changes int) {
	for range n {
		if h.observe(c.tick(), link, reachable) {
			changes++
		}
	}
	return changes
}

func TestPathHealthStartsUnusableAndComesUpAfterSuccesses(t *testing.T) {
	c := &healthClock{now: time.Unix(0, 0)}
	h := newPathHealth(testHealth)
	require.False(t, h.usable)
	require.Equal(t, 0, c.run(h, 1, true, true))
	require.Equal(t, 1, c.run(h, 1, true, true), "second success brings the path up without hold-down")
	require.True(t, h.usable)
}

func TestPathHealthToleratesIsolatedProbeFailures(t *testing.T) {
	c := &healthClock{now: time.Unix(0, 0)}
	h := newPathHealth(testHealth)
	c.run(h, 2, true, true)
	require.Equal(t, 0, c.run(h, 2, true, false))
	c.run(h, 1, true, true)
	require.Equal(t, 0, c.run(h, 2, true, false), "a success resets the failure count")
	require.True(t, h.usable)
	require.Equal(t, 1, c.run(h, 1, true, false))
	require.False(t, h.usable)
	require.Equal(t, reasonUnreachable, h.reason)
}

func TestPathHealthLinkDownIsImmediate(t *testing.T) {
	c := &healthClock{now: time.Unix(0, 0)}
	h := newPathHealth(testHealth)
	c.run(h, 2, true, true)
	require.Equal(t, 1, c.run(h, 1, false, false))
	require.Equal(t, reasonLinkDown, h.reason)
}

func TestPathHealthHoldsDownARecoveredPath(t *testing.T) {
	c := &healthClock{now: time.Unix(0, 0)}
	h := newPathHealth(testHealth)
	c.run(h, 2, true, true)
	c.run(h, 1, false, false)  // down at t=3
	c.run(h, 20, false, false) // the outage lasts longer than the hold-down
	// Healthy again from t=24: it must stay healthy for the hold-down.
	require.Equal(t, 0, c.run(h, 10, true, true), "still inside the hold-down")
	require.False(t, h.usable)
	require.Equal(t, 1, c.run(h, 1, true, true), "usable after the hold-down of stability")
}

func TestPathHealthFailureDuringHoldDownRestartsIt(t *testing.T) {
	c := &healthClock{now: time.Unix(0, 0)}
	h := newPathHealth(testHealth)
	c.run(h, 2, true, true)
	c.run(h, 1, false, false)
	c.run(h, 8, true, true)
	c.run(h, 1, true, false)
	require.Equal(t, 0, c.run(h, 10, true, true))
	require.Equal(t, 1, c.run(h, 1, true, true))
}

func TestPathHealthDampsFlapsAndForgetsThemWhenStable(t *testing.T) {
	c := &healthClock{now: time.Unix(0, 0)}
	h := newPathHealth(testHealth)
	c.run(h, 2, true, true)

	flap := func() time.Duration {
		c.run(h, 1, false, false)
		down := c.now
		for !h.usable {
			c.run(h, 1, true, true)
		}
		return c.now.Sub(down)
	}
	// The first success comes one tick after the failure.
	require.Equal(t, 11*time.Second, flap())
	require.Equal(t, 21*time.Second, flap(), "a quick second failure doubles the hold-down")
	require.Equal(t, 41*time.Second, flap())
	require.Equal(t, 41*time.Second, flap(), "capped by max-hold-down")

	c.run(h, 40, true, true) // stable for max-hold-down
	require.Equal(t, 11*time.Second, flap(), "a stable period resets the penalty")
}

func TestPathHealthSettlesOnItsFirstVerdict(t *testing.T) {
	c := &healthClock{now: time.Unix(0, 0)}
	up, down, link := newPathHealth(testHealth), newPathHealth(testHealth), newPathHealth(testHealth)
	c.run(up, 1, true, true)
	c.run(down, 2, true, false)
	require.False(t, up.settled())
	require.False(t, down.settled())
	c.run(up, 1, true, true)
	c.run(down, 1, true, false)
	c.run(link, 1, false, false)
	require.True(t, up.settled())
	require.True(t, down.settled())
	require.True(t, link.settled(), "a link down needs no confirmation")
}

func TestPathHealthWithoutHoldDown(t *testing.T) {
	c := &healthClock{now: time.Unix(0, 0)}
	cfg := testHealth
	cfg.HoldDown = 0
	h := newPathHealth(cfg)
	c.run(h, 2, true, true)
	c.run(h, 1, false, false)
	require.Equal(t, 1, c.run(h, 2, true, true))
}
