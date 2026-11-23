//go:build linux

package fib

import (
	"os"
	"os/exec"
	"strings"
	"testing"
)

const netnsEnv = "FIB_TEST_NETNS"

// inNetns runs the calling test again inside new user, network and mount
// namespaces, where it may change links, routes and rules without
// privileges and without touching the host. It returns true in the child,
// which runs the test body, and false in the parent. The test is skipped
// when unprivileged namespaces are unavailable, unless FIB_REQUIRE_NETNS=1.
func inNetns(t *testing.T) bool {
	t.Helper()
	if os.Getenv(netnsEnv) == "1" {
		netnsSetup(t)
		return true
	}
	if err := exec.Command("unshare", "-rnm", "true").Run(); err != nil {
		if os.Getenv("FIB_REQUIRE_NETNS") == "1" {
			t.Fatalf("unprivileged network namespaces are required: %v", err)
		}
		t.Skipf("unprivileged network namespaces are unavailable: %v", err)
	}
	cmd := exec.Command("unshare", "-rnm", "--propagation", "private", os.Args[0],
		"-test.run", "^"+t.Name()+"$", "-test.count=1", "-test.v")
	cmd.Env = append(os.Environ(), netnsEnv+"=1")
	out, err := cmd.CombinedOutput()
	if err != nil || !strings.Contains(string(out), "--- PASS: "+t.Name()) {
		t.Fatalf("test in network namespace failed: %v\n%s", err, out)
	}
	return false
}

func sh(t *testing.T, args ...string) string {
	t.Helper()
	out, err := exec.Command(args[0], args[1:]...).CombinedOutput()
	if err != nil {
		t.Fatalf("%s: %v\n%s", strings.Join(args, " "), err, out)
	}
	return string(out)
}

// netnsSetup builds two links to a CPE namespace:
// wan1 10.0.1.2/24 -> 10.0.1.1 and wan2 10.0.2.2/24 -> 10.0.2.1.
func netnsSetup(t *testing.T) {
	t.Helper()
	sh(t, "mount", "-t", "tmpfs", "tmpfs", "/run")
	sh(t, "mkdir", "-p", "/run/netns")
	sh(t, "ip", "link", "set", "lo", "up")
	sh(t, "ip", "netns", "add", "cpe")
	sh(t, "ip", "-n", "cpe", "link", "set", "lo", "up")
	for _, i := range []string{"1", "2"} {
		sh(t, "ip", "link", "add", "wan"+i, "type", "veth", "peer", "name", "c"+i)
		sh(t, "ip", "link", "set", "c"+i, "netns", "cpe")
		sh(t, "ip", "addr", "add", "10.0."+i+".2/24", "dev", "wan"+i)
		sh(t, "ip", "link", "set", "wan"+i, "up")
		sh(t, "ip", "-n", "cpe", "addr", "add", "10.0."+i+".1/24", "dev", "c"+i)
		sh(t, "ip", "-n", "cpe", "link", "set", "c"+i, "up")
	}
}
