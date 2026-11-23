//go:build !linux

package fib

import (
	"time"

	"github.com/osrg/gobgp/v4/pkg/config/oc"
)

type unsupportedChecker struct{}

func newChecker() checker { return unsupportedChecker{} }

func (unsupportedChecker) check(oc.FibExitPath, time.Duration) (bool, bool, error) {
	return false, false, errUnsupported
}

func (unsupportedChecker) watchLinks(<-chan struct{}, func(string)) error { return errUnsupported }
