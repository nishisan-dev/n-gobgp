//go:build linux

package fib

import (
	"errors"
	"fmt"
	"net"
	"net/netip"

	"github.com/vishvananda/netlink"
	"golang.org/x/sys/unix"
)

type netlinkKernel struct {
	protocol int
}

func newKernel(protocol int) kernel {
	return &netlinkKernel{protocol: protocol}
}

func prefixToIPNet(p netip.Prefix) *net.IPNet {
	return &net.IPNet{IP: p.Addr().AsSlice(), Mask: net.CIDRMask(p.Bits(), p.Addr().BitLen())}
}

func ipNetToPrefix(n *net.IPNet) netip.Prefix {
	if n == nil {
		return netip.PrefixFrom(netip.IPv4Unspecified(), 0)
	}
	addr, _ := netip.AddrFromSlice(n.IP)
	bits, _ := n.Mask.Size()
	return netip.PrefixFrom(addr.Unmap(), bits).Masked()
}

func (k *netlinkKernel) toNetlink(r kroute) (*netlink.Route, error) {
	link, err := netlink.LinkByName(r.Interface)
	if err != nil {
		return nil, fmt.Errorf("interface %s: %w", r.Interface, err)
	}
	return &netlink.Route{
		Family:    netlink.FAMILY_V4,
		Dst:       prefixToIPNet(r.Prefix),
		Gw:        r.Gateway.AsSlice(),
		LinkIndex: link.Attrs().Index,
		Table:     int(r.Table),
		Priority:  int(r.Metric),
		Protocol:  netlink.RouteProtocol(k.protocol),
		Scope:     netlink.SCOPE_UNIVERSE,
		Type:      unix.RTN_UNICAST,
	}, nil
}

// routesAt lists every route with the key, whatever its owner.
func (k *netlinkKernel) routesAt(key routeKey) ([]netlink.Route, error) {
	routes, err := netlink.RouteListFiltered(netlink.FAMILY_V4, &netlink.Route{Table: int(key.Table)}, netlink.RT_FILTER_TABLE)
	if err != nil {
		return nil, err
	}
	var l []netlink.Route
	for _, r := range routes {
		if ipNetToPrefix(r.Dst) == key.Prefix && r.Priority == int(key.Metric) && r.Tos == 0 {
			l = append(l, r)
		}
	}
	return l, nil
}

func (k *netlinkKernel) foreignOwner(key routeKey) (*conflictError, error) {
	routes, err := k.routesAt(key)
	if err != nil {
		return nil, err
	}
	for _, r := range routes {
		if int(r.Protocol) != k.protocol {
			return &conflictError{Protocol: int(r.Protocol)}, nil
		}
	}
	return nil, nil
}

func (k *netlinkKernel) ownedRoutes() ([]kroute, error) {
	routes, err := netlink.RouteListFiltered(netlink.FAMILY_V4, &netlink.Route{Protocol: netlink.RouteProtocol(k.protocol)}, netlink.RT_FILTER_PROTOCOL|netlink.RT_FILTER_TABLE)
	if err != nil {
		return nil, err
	}
	names := map[int]string{}
	l := make([]kroute, 0, len(routes))
	for _, r := range routes {
		gw, _ := netip.AddrFromSlice(r.Gw)
		name, ok := names[r.LinkIndex]
		if !ok && r.LinkIndex > 0 {
			if link, err := netlink.LinkByIndex(r.LinkIndex); err == nil {
				name = link.Attrs().Name
			}
			names[r.LinkIndex] = name
		}
		l = append(l, kroute{Table: uint32(r.Table), Prefix: ipNetToPrefix(r.Dst), Metric: uint32(r.Priority), Gateway: gw.Unmap(), Interface: name})
	}
	return l, nil
}

func (k *netlinkKernel) addRoute(r kroute) error {
	nr, err := k.toNetlink(r)
	if err != nil {
		return err
	}
	err = netlink.RouteAdd(nr)
	if errors.Is(err, unix.EEXIST) {
		if c, lookupErr := k.foreignOwner(r.key()); lookupErr == nil && c != nil {
			return c
		}
	}
	return err
}

func (k *netlinkKernel) replaceRoute(r kroute) error {
	// NLM_F_REPLACE ignores the owner, so look first: a foreign route that
	// took the key must not be overwritten.
	c, err := k.foreignOwner(r.key())
	if err != nil {
		return err
	}
	if c != nil {
		return c
	}
	nr, err := k.toNetlink(r)
	if err != nil {
		return err
	}
	return netlink.RouteReplace(nr)
}

func (k *netlinkKernel) deleteRoute(r kroute) error {
	// The protocol takes part in the kernel lookup, so a foreign route with
	// the same key is never deleted.
	err := netlink.RouteDel(&netlink.Route{
		Family:   netlink.FAMILY_V4,
		Dst:      prefixToIPNet(r.Prefix),
		Table:    int(r.Table),
		Priority: int(r.Metric),
		Protocol: netlink.RouteProtocol(k.protocol),
	})
	if errors.Is(err, unix.ESRCH) {
		return nil
	}
	return err
}

func (k *netlinkKernel) takeOverRoute(r kroute) (func() error, error) {
	routes, err := k.routesAt(r.key())
	if err != nil {
		return nil, err
	}
	var saved *netlink.Route
	for _, x := range routes {
		if int(x.Protocol) != k.protocol {
			saved = &x
			break
		}
	}
	if saved == nil {
		return nil, fmt.Errorf("no foreign route at %s", r.key())
	}
	nr, err := k.toNetlink(r)
	if err != nil {
		return nil, err
	}
	if err := netlink.RouteReplace(nr); err != nil {
		return nil, err
	}
	saved.Flags = 0
	return func() error { return netlink.RouteReplace(saved) }, nil
}

func (k *netlinkKernel) toNetlinkRule(r krule) *netlink.Rule {
	nr := netlink.NewRule()
	nr.Family = netlink.FAMILY_V4
	nr.Priority = int(r.Priority)
	nr.Table = int(r.Table)
	nr.Protocol = uint8(k.protocol)
	if r.From.IsValid() {
		nr.Src = prefixToIPNet(r.From)
	}
	if r.To.IsValid() {
		nr.Dst = prefixToIPNet(r.To)
	}
	nr.IifName = r.Iif
	if r.Mask != 0 {
		mask := r.Mask
		nr.Mark, nr.Mask = r.Mark, &mask
	}
	return nr
}

func (k *netlinkKernel) listRules() ([]krule, map[uint32]bool, error) {
	rules, err := netlink.RuleListFiltered(netlink.FAMILY_V4, nil, 0)
	if err != nil {
		return nil, nil, err
	}
	var owned []krule
	foreign := map[uint32]bool{}
	for _, r := range rules {
		if int(r.Protocol) != k.protocol {
			foreign[uint32(r.Priority)] = true
			continue
		}
		x := krule{Priority: uint32(r.Priority), Table: uint32(r.Table), Iif: r.IifName}
		if r.Src != nil {
			x.From = ipNetToPrefix(r.Src)
		}
		if r.Dst != nil {
			x.To = ipNetToPrefix(r.Dst)
		}
		if r.Mask != nil && *r.Mask != 0 {
			x.Mark, x.Mask = r.Mark, *r.Mask
		}
		owned = append(owned, x)
	}
	return owned, foreign, nil
}

func (k *netlinkKernel) addRule(r krule) error {
	return netlink.RuleAdd(k.toNetlinkRule(r))
}

func (k *netlinkKernel) deleteRule(r krule) error {
	err := netlink.RuleDel(k.toNetlinkRule(r))
	if errors.Is(err, unix.ENOENT) {
		return nil
	}
	return err
}

// kernelRequirements checks for the protocol attribute of rules (Linux 4.17),
// which identifies the owned rules.
func kernelRequirements(needRules bool) error {
	if !needRules {
		return nil
	}
	var u unix.Utsname
	if err := unix.Uname(&u); err != nil {
		return err
	}
	release := unix.ByteSliceToString(u.Release[:])
	var major, minor int
	if _, err := fmt.Sscanf(release, "%d.%d", &major, &minor); err != nil {
		return fmt.Errorf("cannot parse the kernel release %q: %w", release, err)
	}
	if major < 4 || major == 4 && minor < 17 {
		return fmt.Errorf("traffic rules need Linux 4.17 or later, running %s", release)
	}
	return nil
}
