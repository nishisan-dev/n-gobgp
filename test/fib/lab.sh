#!/usr/bin/env bash
# End-to-end lab for the fib extension, with two gobgpd processes: the server
# under test and a CPE in its own network namespace, connected by two links
# and peering with eBGP multihop between loopbacks.
#
#   server lo 192.0.2.20/32                  cpe lo 192.0.2.10/32
#   wan1 10.0.1.2/24  <--- veth --->  c1 10.0.1.1/24
#   wan2 10.0.2.2/24  <--- veth --->  c2 10.0.2.1/24
#
# It runs without privileges inside new user, network and mount namespaces.
# Usage: test/fib/lab.sh   (builds gobgpd and gobgp from this checkout)
set -euo pipefail

ROOT=$(cd "$(dirname "$0")/../.." && pwd)
if [ "${FIB_LAB_INNER:-}" != 1 ]; then
	WORK=$(mktemp -d "${TMPDIR:-/tmp}/fib-lab.XXXXXX")
	go build -C "$ROOT" -o "$WORK/gobgpd" ./cmd/gobgpd
	go build -C "$ROOT" -o "$WORK/gobgp" ./cmd/gobgp
	echo "lab directory: $WORK"
	exec unshare -rnm --propagation private env FIB_LAB_INNER=1 WORK="$WORK" "$0" "$@"
fi

cd "$WORK"
FAILED=0
pass() { echo "PASS $*"; }
fail() {
	echo "FAIL $*"
	FAILED=1
}
# wait_for <seconds> <description> <command...>: retry until the command succeeds.
wait_for() {
	local limit=$1 what=$2
	shift 2
	local end=$((SECONDS + limit))
	until "$@" >/dev/null 2>&1; do
		if [ $SECONDS -ge $end ]; then
			fail "$what"
			return 0
		fi
		sleep 0.1
	done
	pass "$what"
}
has_route() { ip route show "$@" | grep -q .; }
route_via() { # route_via <table> <prefix> <gateway> <dev>
	ip route show table "$1" "$2" | grep -q "via $3 dev $4 proto 200 metric 20"
}
no_route() { ! ip route show "$@" | grep -q .; }
cpe() { ip netns exec cpe "$@"; }
cpe_cli() { cpe ./gobgp -p 50062 -u 127.0.0.1 "$@"; }
srv_cli() { ./gobgp -p 50061 -u 127.0.0.1 "$@"; }
established() { srv_cli neighbor 192.0.2.10 | grep -q "BGP state = .*ESTABLISHED"; }
owned_objects() { ip route show table all proto 200 | grep -q . || ip rule | grep -q "proto 200"; }

# --- topology
mount -t tmpfs tmpfs /run
mkdir -p /run/netns
ip netns add cpe
ip link set lo up
cpe ip link set lo up
ip addr add 192.0.2.20/32 dev lo
cpe ip addr add 192.0.2.10/32 dev lo
for i in 1 2; do
	ip link add wan$i type veth peer name c$i
	ip link set c$i netns cpe
	ip addr add 10.0.$i.2/24 dev wan$i
	ip link set wan$i up
	cpe ip addr add 10.0.$i.1/24 dev c$i
	cpe ip link set c$i up
done
# The CPE returns to the server loopback through c1, or c2 when c1 is down.
# Taking a link down flushes its routes, so they are restored after each test,
# as the CPE's own failover would do.
cpe_return_routes() {
	cpe ip route replace 192.0.2.20/32 via 10.0.1.2 metric 10 2>/dev/null || true
	cpe ip route replace 192.0.2.20/32 via 10.0.2.2 metric 20 2>/dev/null || true
}
cpe sysctl -qw net.ipv4.conf.all.ignore_routes_with_linkdown=1
cpe_return_routes
sysctl -qw net.ipv4.ip_forward=1
# A LAN whose traffic is steered by the traffic rule.
ip link add lan0 type dummy
ip link set lan0 up
ip addr add 172.16.0.254/16 dev lan0
# Objects of other owners that the fib must leave alone.
ip route add 10.10.0.0/16 via 10.0.1.1 proto static
ip route add 192.0.2.99/32 via 10.0.2.1 table 100 metric 20 proto static
ip rule add priority 1500 from 10.20.0.0/16 lookup 100

cat >cpe.toml <<'EOF'
[global.config]
as = 65002
router-id = "192.0.2.10"
[[neighbors]]
[neighbors.config]
neighbor-address = "192.0.2.20"
peer-as = 65001
[neighbors.transport.config]
local-address = "192.0.2.10"
passive-mode = true
[neighbors.ebgp-multihop.config]
enabled = true
multihop-ttl = 3
[neighbors.timers.config]
hold-time = 9
keepalive-interval = 3
[[neighbors.afi-safis]]
[neighbors.afi-safis.config]
afi-safi-name = "ipv4-unicast"
EOF

cat >server.toml <<'EOF'
[global.config]
as = 65001
router-id = "192.0.2.20"
port = -1

[[neighbors]]
[neighbors.config]
neighbor-address = "192.0.2.10"
peer-as = 65002
[neighbors.transport.config]
local-address = "192.0.2.20"
[neighbors.ebgp-multihop.config]
enabled = true
multihop-ttl = 3
[neighbors.timers.config]
hold-time = 9
keepalive-interval = 3
connect-retry = 1
[[neighbors.afi-safis]]
[neighbors.afi-safis.config]
afi-safi-name = "ipv4-unicast"

[[defined-sets.prefix-sets]]
prefix-set-name = "customers"
[[defined-sets.prefix-sets.prefix-list]]
ip-prefix = "198.51.100.0/24"
masklength-range = "24..32"
[[defined-sets.bgp-defined-sets.community-sets]]
community-set-name = "via-wan2"
community-list = ["65002:200"]

[fib]
enabled = true
mode = "enforce"
resync-interval = "2s"
protected-prefixes = ["10.10.0.0/16"]
[fib.health]
interval = "200ms"
failures = 3
successes = 3
hold-down = "2s"
max-hold-down = "10s"
[fib.startup]
eor-timeout = "3s"
max-retention = "30s"

[[fib.exit-paths]]
name = "wan1"
interface = "wan1"
gateway = "10.0.1.1"
[[fib.exit-paths]]
name = "wan2"
interface = "wan2"
gateway = "10.0.2.1"

[[fib.anchors]]
name = "cpe-loopback"
prefix = "192.0.2.10/32"
exit-paths = ["wan1", "wan2"]

[[fib.rules]]
name = "community-wan2"
priority = 10
table = 100
exit-paths = ["wan2", "wan1"]
[fib.rules.conditions.bgp-conditions.match-community-set]
community-set = "via-wan2"
[[fib.rules]]
name = "customers"
priority = 20
table = 100
exit-paths = ["wan1", "wan2"]
[fib.rules.conditions.match-prefix-set]
prefix-set = "customers"
[[fib.rules]]
name = "rest"
priority = 100
table = "main"
exit-paths = ["wan1", "wan2"]

[[fib.traffic-rules]]
name = "lan"
priority = 1010
table = 100
from = "172.16.0.0/16"

[fib.default-route]
mode = "table"
table = 300
exit-paths = ["wan1", "wan2"]
EOF

SERVER_PID=
start_server() {
	./gobgpd -f server.toml -p --api-hosts 127.0.0.1:50061 --pprof-disable --metrics-path "" >>server.log 2>&1 &
	SERVER_PID=$!
}
cpe ./gobgpd -f cpe.toml -p --api-hosts 127.0.0.1:50062 --pprof-disable --metrics-path "" >cpe.log 2>&1 &
CPE_PID=$!
trap 'kill $SERVER_PID $CPE_PID 2>/dev/null || true' EXIT
sleep 1
cpe_cli global rib add 198.51.100.0/24 -a ipv4
cpe_cli global rib add 203.0.113.0/24 community 65002:200 -a ipv4
cpe_cli global rib add 10.10.1.0/24 -a ipv4
cpe_cli global rib add 0.0.0.0/0 -a ipv4
cpe_cli global rib add 192.0.2.10/32 -a ipv4

echo "== startup"
start_server
wait_for 15 "anchor installed through wan1" route_via main 192.0.2.10/32 10.0.1.1 wan1
wait_for 20 "multihop session established through the anchor" established
wait_for 5 "customers route in table 100 through wan1" route_via 100 198.51.100.0/24 10.0.1.1 wan1
wait_for 5 "community route in table 100 through wan2" route_via 100 203.0.113.0/24 10.0.2.1 wan2
wait_for 5 "default route in table 300" route_via 300 0.0.0.0/0 10.0.1.1 wan1
no_route 10.10.1.0/24 proto 200 && pass "protected prefix not installed in main" || fail "protected prefix installed"
grep -q 'msg="fib route" Topic=Fib Prefix=192.0.2.10/32 Route=none Status="protected prefix"' server.log && pass "BGP route for the anchor refused" || fail "anchor not protected"
ip rule | grep -q "1000:.*to 10.10.0.0/16 lookup main proto 200" && pass "exemption for the protected prefix" || fail "missing exemption"
ip rule | grep -q "1001:.*to 192.0.2.10 lookup main proto 200" && pass "exemption for the anchor" || fail "missing anchor exemption"
ip rule | grep -q "1010:.*from 172.16.0.0/16 lookup 100 proto 200" && pass "traffic rule" || fail "missing traffic rule"
ip route get 198.51.100.1 from 172.16.0.1 iif lan0 2>/dev/null | grep -q "via 10.0.1.1 dev wan1 table 100" && pass "traffic from the LAN uses table 100" || fail "traffic rule lookup"
grep -q "Status=conflict" server.log && fail "unexpected conflict" || pass "no conflict with the foreign route in table 100"

echo "== BGP withdrawal"
cpe_cli global rib del 198.51.100.0/24 -a ipv4
wait_for 2 "withdrawn route removed at once" no_route table 100 198.51.100.0/24
cpe_cli global rib add 198.51.100.0/24 -a ipv4
wait_for 5 "announced again" route_via 100 198.51.100.0/24 10.0.1.1 wan1

echo "== silent gateway failure on wan1 (link stays up)"
cpe ip addr del 10.0.1.1/24 dev c1
wait_for 5 "anchor moves to wan2" route_via main 192.0.2.10/32 10.0.2.1 wan2
wait_for 5 "customers route moves to wan2" route_via 100 198.51.100.0/24 10.0.2.1 wan2
wait_for 5 "default route moves to wan2" route_via 300 0.0.0.0/0 10.0.2.1 wan2
sleep 10
established && pass "BGP session survives the failure (beyond the hold time)" || fail "session lost"
grep -q 'msg="fib exit path is not usable" Topic=Fib ExitPath=wan1 Usable=false Reason="gateway unreachable"' server.log && pass "reason logged" || fail "reason not logged"

echo "== recovery with hold-down"
cpe ip addr add 10.0.1.1/24 dev c1
cpe_return_routes
sleep 1
route_via main 192.0.2.10/32 10.0.2.1 wan2 && pass "still on wan2 during the hold-down" || fail "returned before the hold-down"
wait_for 6 "anchor back on wan1" route_via main 192.0.2.10/32 10.0.1.1 wan1
wait_for 2 "customers route back on wan1" route_via 100 198.51.100.0/24 10.0.1.1 wan1

echo "== link failure on wan1"
cpe ip link set c1 down
wait_for 1 "link down moves the anchor at once" route_via main 192.0.2.10/32 10.0.2.1 wan2
cpe ip link set c1 up
cpe_return_routes
wait_for 15 "back on wan1 after recovery" route_via main 192.0.2.10/32 10.0.1.1 wan1

echo "== both exit paths down"
cpe ip link set c1 down
cpe ip link set c2 down
wait_for 2 "BGP routes removed without a usable path" no_route table 100 198.51.100.0/24
route_via main 192.0.2.10/32 10.0.2.1 wan2 && pass "anchor kept on its last active path" || fail "anchor removed"
cpe ip link set c1 up
cpe ip link set c2 up
cpe_return_routes
wait_for 30 "session back" established
wait_for 15 "routes back" has_route table 100 198.51.100.0/24 proto 200
wait_for 30 "routes back on the preferred path" route_via 100 198.51.100.0/24 10.0.1.1 wan1

echo "== external removal is repaired"
ip route del 198.51.100.0/24 table 100 proto 200 || true
wait_for 5 "route installed again by the resync" route_via 100 198.51.100.0/24 10.0.1.1 wan1

echo "== crash and restart"
wait_for 10 "anchor on wan1 before the crash" route_via main 192.0.2.10/32 10.0.1.1 wan1
kill -9 $SERVER_PID
wait $SERVER_PID 2>/dev/null || true
has_route table 100 198.51.100.0/24 proto 200 && pass "routes survive the crash" || fail "routes lost on crash"
cpe_cli global rib del 203.0.113.0/24 -a ipv4
(
	end=$((SECONDS + 25))
	while [ $SECONDS -lt $end ]; do
		ip route show table 100 198.51.100.0/24 | grep -q . || echo gap
		ip route show 192.0.2.10/32 | grep -q . || echo gap
		sleep 0.05
	done
) >continuity.log &
WATCH=$!
start_server
wait_for 20 "session after restart" established
wait_for 15 "route withdrawn while down is reconciled away" no_route table 100 203.0.113.0/24
wait $WATCH
[ ! -s continuity.log ] && pass "no gap for confirmed routes and the anchor during the restart" || fail "forwarding gap during restart"
grep -q "fib reconciled the routes of the previous run" server.log && pass "reconciliation logged" || fail "no reconciliation"

echo "== clean stop"
kill -TERM $SERVER_PID
wait $SERVER_PID 2>/dev/null || true
owned_objects && fail "owned objects left after stop" || pass "no owned routes or rules after stop"
ip route show 10.10.0.0/16 | grep -q "proto static" && pass "foreign route in main intact" || fail "foreign route touched"
ip route show table 100 192.0.2.99 | grep -q "proto static" && pass "foreign route in table 100 intact" || fail "foreign table route touched"
ip rule | grep -q "1500:.*from 10.20.0.0/16 lookup 100" && pass "foreign rule intact" || fail "foreign rule touched"

echo "== default route in main, replacing a foreign default"
ip route add default via 10.0.2.1 metric 20 proto dhcp
sed -i 's/^mode = "table"$/mode = "main"\nreplace-external = true/; /^table = 300$/d' server.toml
start_server
wait_for 20 "session with the new configuration" established
wait_for 10 "BGP default replaces the foreign default" route_via main 0.0.0.0/0 10.0.1.1 wan1
ip route show 10.10.0.0/16 | grep -q "proto static" && pass "management route unaffected" || fail "management route touched"
cpe_cli global rib del 0.0.0.0/0 -a ipv4
wait_for 3 "withdrawal restores the foreign default" sh -c 'ip route show default | grep -q "via 10.0.2.1 dev wan2 proto dhcp metric 20"'
cpe_cli global rib add 0.0.0.0/0 -a ipv4
wait_for 5 "BGP default installed again" route_via main 0.0.0.0/0 10.0.1.1 wan1
kill -TERM $SERVER_PID
wait $SERVER_PID 2>/dev/null || true
ip route show default | grep -q "via 10.0.2.1 dev wan2 proto dhcp metric 20" && pass "clean stop restores the foreign default" || fail "foreign default lost"
owned_objects && fail "owned objects left after stop" || pass "no owned routes or rules after the second stop"

echo
if [ $FAILED = 0 ]; then echo "LAB PASSED"; else
	echo "LAB FAILED; logs in $WORK"
	exit 1
fi
