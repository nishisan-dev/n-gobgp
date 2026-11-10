package fib

import (
	"errors"
	"fmt"
	"maps"
	"slices"
	"sync"
)

const fakeProtocol = 200

type fakeRoute struct {
	r     kroute
	proto int
}

type fakeRule struct {
	r     krule
	proto int
}

// fakeKernel keeps routes by key, like the kernel, and records the
// operations of the fib so that tests can check their order.
type fakeKernel struct {
	mu     sync.Mutex
	routes map[routeKey]fakeRoute
	rules  []fakeRule
	ops    []string
}

func newFakeKernel() *fakeKernel {
	return &fakeKernel{routes: map[routeKey]fakeRoute{}}
}

func (k *fakeKernel) seed(r kroute, proto int) {
	k.mu.Lock()
	defer k.mu.Unlock()
	k.routes[r.key()] = fakeRoute{r, proto}
}

func (k *fakeKernel) seedRule(r krule, proto int) {
	k.mu.Lock()
	defer k.mu.Unlock()
	k.rules = append(k.rules, fakeRule{r, proto})
}

func (k *fakeKernel) remove(key routeKey) {
	k.mu.Lock()
	defer k.mu.Unlock()
	delete(k.routes, key)
}

func (k *fakeKernel) get(key routeKey) (fakeRoute, bool) {
	k.mu.Lock()
	defer k.mu.Unlock()
	r, ok := k.routes[key]
	return r, ok
}

func (k *fakeKernel) all() map[routeKey]fakeRoute {
	k.mu.Lock()
	defer k.mu.Unlock()
	return maps.Clone(k.routes)
}

func (k *fakeKernel) allRules() []fakeRule {
	k.mu.Lock()
	defer k.mu.Unlock()
	return slices.Clone(k.rules)
}

func (k *fakeKernel) takeOps() []string {
	k.mu.Lock()
	defer k.mu.Unlock()
	ops := k.ops
	k.ops = nil
	return ops
}

func (k *fakeKernel) ownedRoutes() ([]kroute, error) {
	k.mu.Lock()
	defer k.mu.Unlock()
	var l []kroute
	for _, r := range k.routes {
		if r.proto == fakeProtocol {
			l = append(l, r.r)
		}
	}
	return l, nil
}

func (k *fakeKernel) addRoute(r kroute) error {
	k.mu.Lock()
	defer k.mu.Unlock()
	if old, ok := k.routes[r.key()]; ok {
		if old.proto != fakeProtocol {
			return &conflictError{Protocol: old.proto}
		}
		return errors.New("file exists")
	}
	k.routes[r.key()] = fakeRoute{r, fakeProtocol}
	k.ops = append(k.ops, "add "+r.key().String())
	return nil
}

func (k *fakeKernel) replaceRoute(r kroute) error {
	k.mu.Lock()
	defer k.mu.Unlock()
	if old, ok := k.routes[r.key()]; ok && old.proto != fakeProtocol {
		return &conflictError{Protocol: old.proto}
	}
	k.routes[r.key()] = fakeRoute{r, fakeProtocol}
	k.ops = append(k.ops, "replace "+r.key().String())
	return nil
}

func (k *fakeKernel) deleteRoute(r kroute) error {
	k.mu.Lock()
	defer k.mu.Unlock()
	if old, ok := k.routes[r.key()]; ok && old.proto == fakeProtocol {
		delete(k.routes, r.key())
		k.ops = append(k.ops, "delete "+r.key().String())
	}
	return nil
}

func (k *fakeKernel) takeOverRoute(r kroute) (func() error, error) {
	k.mu.Lock()
	defer k.mu.Unlock()
	saved, ok := k.routes[r.key()]
	if !ok || saved.proto == fakeProtocol {
		return nil, fmt.Errorf("nothing to take over at %s", r.key())
	}
	k.routes[r.key()] = fakeRoute{r, fakeProtocol}
	k.ops = append(k.ops, "take-over "+r.key().String())
	return func() error {
		k.mu.Lock()
		defer k.mu.Unlock()
		k.routes[r.key()] = saved
		k.ops = append(k.ops, "restore "+r.key().String())
		return nil
	}, nil
}

func (k *fakeKernel) listRules() ([]krule, map[uint32]bool, error) {
	k.mu.Lock()
	defer k.mu.Unlock()
	var owned []krule
	foreign := map[uint32]bool{}
	for _, r := range k.rules {
		if r.proto == fakeProtocol {
			owned = append(owned, r.r)
		} else {
			foreign[r.r.Priority] = true
		}
	}
	return owned, foreign, nil
}

func (k *fakeKernel) addRule(r krule) error {
	k.mu.Lock()
	defer k.mu.Unlock()
	k.rules = append(k.rules, fakeRule{r, fakeProtocol})
	k.ops = append(k.ops, fmt.Sprintf("add rule %d", r.Priority))
	return nil
}

func (k *fakeKernel) deleteRule(r krule) error {
	k.mu.Lock()
	defer k.mu.Unlock()
	k.rules = slices.DeleteFunc(k.rules, func(x fakeRule) bool { return x.proto == fakeProtocol && x.r == r })
	k.ops = append(k.ops, fmt.Sprintf("delete rule %d", r.Priority))
	return nil
}

type fakePaths struct {
	mu sync.Mutex
	up map[string]bool
}

func newFakePaths(up ...string) *fakePaths {
	f := &fakePaths{up: map[string]bool{}}
	for _, n := range up {
		f.up[n] = true
	}
	return f
}

func (f *fakePaths) set(name string, up bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.up[name] = up
}

func (f *fakePaths) usable(name string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.up[name]
}

func (f *fakePaths) reason(name string) string {
	if f.usable(name) {
		return reasonUsable
	}
	return reasonUnreachable
}
