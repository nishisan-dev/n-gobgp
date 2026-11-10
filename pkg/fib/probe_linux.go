//go:build linux

package fib

import (
	"encoding/binary"
	"fmt"
	"net"
	"net/netip"
	"time"

	"github.com/vishvananda/netlink"
	"golang.org/x/sys/unix"

	"github.com/osrg/gobgp/v4/pkg/config/oc"
)

type netlinkChecker struct{}

func newChecker() checker { return netlinkChecker{} }

func htons(v uint16) uint16 {
	var b [2]byte
	binary.BigEndian.PutUint16(b[:], v)
	return binary.NativeEndian.Uint16(b[:])
}

func linkUp(link netlink.Link) bool {
	f := link.Attrs().Flags
	return f&net.FlagUp != 0 && f&net.FlagRunning != 0
}

func (netlinkChecker) check(p oc.FibExitPath, timeout time.Duration) (bool, bool, error) {
	link, err := netlink.LinkByName(p.Interface)
	if err != nil {
		return false, false, err
	}
	if !linkUp(link) {
		return false, false, nil
	}
	if p.Probe == oc.FIB_PROBE_LINK {
		return true, true, nil
	}
	err = arpProbe(link, p.Gateway, timeout)
	return true, err == nil, err
}

func (netlinkChecker) watchLinks(done <-chan struct{}, wake func(string)) error {
	ch := make(chan netlink.LinkUpdate, 16)
	if err := netlink.LinkSubscribeWithOptions(ch, done, netlink.LinkSubscribeOptions{}); err != nil {
		return err
	}
	go func() {
		for u := range ch {
			wake(u.Attrs().Name)
		}
	}()
	return nil
}

// sourceAddress returns the interface address in the gateway's subnet.
func sourceAddress(link netlink.Link, gw netip.Addr) (netip.Addr, error) {
	addrs, err := netlink.AddrList(link, netlink.FAMILY_V4)
	if err != nil {
		return netip.Addr{}, err
	}
	for _, a := range addrs {
		if a.IPNet != nil && ipNetToPrefix(a.IPNet).Contains(gw) {
			addr, _ := netip.AddrFromSlice(a.IP)
			return addr.Unmap(), nil
		}
	}
	return netip.Addr{}, fmt.Errorf("%s has no address in the subnet of %s", link.Attrs().Name, gw)
}

// arpProbe sends an ARP request for the gateway and waits for its reply. It
// tests the layer 2 path to the gateway, independently of the routing table.
func arpProbe(link netlink.Link, gw netip.Addr, timeout time.Duration) error {
	attrs := link.Attrs()
	if len(attrs.HardwareAddr) != 6 {
		return fmt.Errorf("%s has no Ethernet address; use the link probe", attrs.Name)
	}
	src, err := sourceAddress(link, gw)
	if err != nil {
		return err
	}
	fd, err := unix.Socket(unix.AF_PACKET, unix.SOCK_DGRAM|unix.SOCK_CLOEXEC, int(htons(unix.ETH_P_ARP)))
	if err != nil {
		return fmt.Errorf("arp socket: %w", err)
	}
	defer unix.Close(fd)
	if err := unix.Bind(fd, &unix.SockaddrLinklayer{Protocol: htons(unix.ETH_P_ARP), Ifindex: attrs.Index}); err != nil {
		return fmt.Errorf("arp bind: %w", err)
	}
	req := make([]byte, 28)
	binary.BigEndian.PutUint16(req[0:], 1)      // Ethernet
	binary.BigEndian.PutUint16(req[2:], 0x0800) // IPv4
	req[4], req[5] = 6, 4
	binary.BigEndian.PutUint16(req[6:], 1) // request
	copy(req[8:], attrs.HardwareAddr)
	copy(req[14:], src.AsSlice())
	copy(req[24:], gw.AsSlice())
	dst := &unix.SockaddrLinklayer{Protocol: htons(unix.ETH_P_ARP), Ifindex: attrs.Index, Halen: 6}
	copy(dst.Addr[:], []byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff})
	if err := unix.Sendto(fd, req, 0, dst); err != nil {
		return fmt.Errorf("arp send: %w", err)
	}
	deadline := time.Now().Add(timeout)
	buf := make([]byte, 128)
	for {
		left := time.Until(deadline)
		if left <= 0 {
			return fmt.Errorf("no ARP reply from %s on %s", gw, attrs.Name)
		}
		tv := unix.NsecToTimeval(left.Nanoseconds())
		if err := unix.SetsockoptTimeval(fd, unix.SOL_SOCKET, unix.SO_RCVTIMEO, &tv); err != nil {
			return err
		}
		n, _, err := unix.Recvfrom(fd, buf, 0)
		if err == unix.EAGAIN || err == unix.EINTR {
			continue
		}
		if err != nil {
			return fmt.Errorf("arp receive: %w", err)
		}
		if n >= 28 && binary.BigEndian.Uint16(buf[6:]) == 2 && netip.AddrFrom4([4]byte(buf[14:18])) == gw {
			return nil
		}
	}
}
