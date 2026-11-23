//go:build !linux

package fib

import "errors"

var errUnsupported = errors.New("fib is not supported on this platform")

type unsupportedKernel struct{}

func newKernel(int) kernel { return unsupportedKernel{} }

func (unsupportedKernel) ownedRoutes() ([]kroute, error) { return nil, errUnsupported }
func (unsupportedKernel) addRoute(kroute) error          { return errUnsupported }
func (unsupportedKernel) replaceRoute(kroute) error      { return errUnsupported }
func (unsupportedKernel) deleteRoute(kroute) error       { return errUnsupported }

func (unsupportedKernel) takeOverRoute(kroute) (func() error, error) { return nil, errUnsupported }
func (unsupportedKernel) listRules() ([]krule, map[uint32]bool, error) {
	return nil, nil, errUnsupported
}
func (unsupportedKernel) addRule(krule) error    { return errUnsupported }
func (unsupportedKernel) deleteRule(krule) error { return errUnsupported }

func kernelRequirements(bool) error { return errUnsupported }
