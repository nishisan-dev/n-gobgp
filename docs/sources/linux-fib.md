# Policy-based Linux forwarding

The `fib` extension lets `gobgpd` use the routes learned from BGP for the
forwarding of the Linux server it runs on. Rules that match BGP attributes
choose, for each destination, the routing table and an ordered list of exit
paths. Each exit path is an interface with its own gateway, which does not
need to be the next hop announced in BGP.

It is designed for a server connected to one CPE through several links, each
with its own subnet and gateway, with a single BGP session to the CPE.

It is independent of the [Zebra integration](zebra.md): no other routing
daemon is needed. It is disabled by default and supports IPv4 unicast.

## Contents

- [Concepts](#concepts)
- [Modes](#modes)
- [Network design](#network-design)
- [Configuration](#configuration)
- [Behaviour](#behaviour)
- [Default route and management access](#default-route-and-management-access)
- [Coexistence with other route owners](#coexistence-with-other-route-owners)
- [Operation](#operation)
- [Limitations](#limitations)

## Concepts

The configuration has three independent layers.

| Layer | Decides | Depends on |
| --- | --- | --- |
| Exit paths | How to leave the server: interface and gateway, and whether that path works | Link state and ARP probes |
| Rules | For each best path: ignore it, or install it in a table through an ordered list of exit paths | BGP attributes |
| Traffic rules | Which traffic consults which table (`ip rule`) | Source, destination, input interface, firewall mark |

Rules see only BGP; traffic rules see only packets. A rule that sends
destinations to table 100 has no effect on traffic until a traffic rule, or
another component, makes that traffic consult table 100.

**Anchors** are routes that come from the configuration instead of BGP, such
as the route to the BGP neighbor's loopback. They follow the health of their
exit paths like BGP routes, and are installed before the BGP neighbors start.

Every route and rule that `gobgpd` installs carries its protocol number
(`protocol`, 200 by default). Objects with another protocol number belong to
someone else and are never changed, with one explicit exception described in
[Default route and management access](#default-route-and-management-access).

## Modes

- `observe` (default): the decisions are evaluated and logged, and the exit
  paths are monitored, but the kernel is not changed. Use it to validate the
  rules on a production server.
- `enforce`: the kernel is programmed through netlink.

## Network design

The goal is that losing one link does not lose the BGP session, because a
lost session withdraws every route, including those that would leave through
the other links.

### Preferred: eBGP multihop between loopbacks

The server and the CPE each peer from a loopback address. An anchor sends the
traffic to the CPE loopback through the first healthy link; the CPE must
reach the server loopback through every link.

Requirements on the CPE:

- BGP peering with a loopback address, with eBGP multihop (TTL 2 or more);
- routes back to the server loopback through each link, with its own
  failure detection (static routes with link tracking, BFD, or similar);
- no strict reverse path filtering towards the server loopback.

Requirements on the server:

- a loopback address used as `local-address` of the neighbor;
- an anchor for the CPE loopback over all exit paths;
- `rp_filter` set to loose mode (`2`) or disabled on the exit interfaces,
  because replies may arrive on a link other than the one used to send;
- a BGP hold time longer than the failure detection time of the exit paths
  (`failures` × `interval`, about 3 s by default).

### Without a CPE loopback

When the CPE accepts the session on the address of one link but can receive
it through another, use that address as the anchor prefix (`/32`) and list
every exit path in the anchor. This depends on the CPE accepting packets for
that address on any link and having a return route for the server address
through each link.

### Single-hop session

When neither is possible, peer over one link as usual. The extension still
works, but losing that link loses the session and every BGP route.

## Configuration

The section `[fib]` is read at startup; changes need a restart (a `SIGHUP`
reload logs a warning and keeps the active configuration). `gobgpd --dry-run`
validates it, including the references to defined sets. A complete example is
in [`docs/examples/fib.toml`](../examples/fib.toml).

### Global settings

| Key | Default | Meaning |
| --- | --- | --- |
| `enabled` | `false` | Enables the extension |
| `mode` | `observe` | `observe` or `enforce` |
| `protocol` | `200` | Route and rule protocol number identifying owned objects. Numbers of the kernel and known routing daemons are rejected |
| `metric` | `20` | Metric of every installed route. Routes of other owners with a different metric coexist |
| `anchor-wait` | `10s` | How long the start waits for the anchors before the BGP neighbors start |
| `resync-interval` | `30s` | Period of the comparison between the kernel and the owned state |
| `protected-prefixes` | none | Prefixes that BGP routes, equal or more specific, cannot take in the main table |
| `rule-priority-min`, `rule-priority-max` | `1000`, `1999` | Priority range of the managed `ip rule` entries |

### Health of the exit paths: `[fib.health]`

| Key | Default | Meaning |
| --- | --- | --- |
| `interval` | `1s` | Time between checks of each exit path |
| `failures` | `3` | Consecutive failed probes that make a path unusable. A link down is immediate |
| `successes` | `3` | Consecutive successful probes needed to use a path again |
| `hold-down` | `10s` | After a failure, how long a path must stay healthy before it is used again |
| `max-hold-down` | `5m` | The hold-down doubles each time a path fails again less than this time after recovering, up to this value; a path stable for this time returns to `hold-down` |

### Exit paths: `[[fib.exit-paths]]`

| Key | Meaning |
| --- | --- |
| `name` | Name referenced by rules and anchors |
| `interface` | Linux interface |
| `gateway` | IPv4 gateway on that interface |
| `probe` | `arp` (default): the gateway must answer ARP requests; `link`: only the link state counts, for interfaces without ARP |

The ARP probe tests the path to the gateway only. A CPE that answers ARP but
cannot forward is not detected.

### Anchors: `[[fib.anchors]]`

| Key | Meaning |
| --- | --- |
| `name` | Name |
| `prefix` | IPv4 prefix, for example the CPE loopback `/32` |
| `table` | `main` (default) or a table number |
| `exit-paths` | Exit paths, in order of preference |

### Rules: `[[fib.rules]]`

| Key | Meaning |
| --- | --- |
| `name` | Name, shown in the logs |
| `priority` | Required and unique. The matching rule with the lowest priority decides |
| `action` | `install` (default) or `ignore` |
| `table` | `main` or a table number, for `install` |
| `exit-paths` | Exit paths in order of preference, for `install` |
| `conditions` | The same conditions as [policy statements](policy.md), using the same `defined-sets` |

A destination that no rule matches is not installed. Conditions without
meaning outside the policy assignment points are rejected: `call-policy`,
`match-tag-set`, `install-protocol-eq`, `rpki-validation-result` and
`afi-safi-in-list`. The rules never match the default route; see
[Default route and management access](#default-route-and-management-access).

The rules apply to the best path of the global RIB, after the import
policies. Use the import policies to decide what is accepted, and the rules
to decide how it is forwarded.

### Traffic rules: `[[fib.traffic-rules]]`

| Key | Meaning |
| --- | --- |
| `name` | Name |
| `priority` | `ip rule` priority, unique and inside the priority range |
| `table` | `main` or a table number |
| `from`, `to` | Optional source and destination prefixes |
| `iif` | Optional input interface |
| `fwmark` | Optional firewall mark, `mark` or `mark/mask`, decimal or hexadecimal |

When there are traffic rules, exemption rules come first in the range: one
per protected prefix (`to <prefix> lookup main`) and one per anchor
(`to <prefix> lookup <anchor table>`). Traffic rule priorities therefore start
at `rule-priority-min` plus the number of protected prefixes and anchors.

### Default route: `[fib.default-route]`

| Key | Meaning |
| --- | --- |
| `mode` | `ignore` (default), `table` or `main` |
| `table` | Table number for mode `table`; it cannot be `main` |
| `exit-paths` | Exit paths, in order of preference |
| `replace-external` | Mode `main` only: allows replacing a foreign default route with the same metric |
| `conditions` | Optional conditions the default route must match |

### Restart: `[fib.startup]`

| Key | Default | Meaning |
| --- | --- | --- |
| `eor-timeout` | `30s` | After the first session is established, how long to wait for End-of-RIB |
| `max-retention` | `2m` | How long routes of a previous run are kept without reconciliation |

## Behaviour

### Three different events

| Event | Effect |
| --- | --- |
| BGP withdraws a destination, or its best path changes | Only that destination is updated, at once, without retention |
| The BGP session goes down | Every BGP route is removed; anchors and traffic rules stay |
| An exit path stops working | Routes move to the next usable exit path of their rule; with none left they are removed. Anchors stay on their last active path |

When an attribute change moves a destination to another table, the new route
is installed before the old one is removed.

Graceful restart and long-lived graceful restart should stay disabled on the
neighbors that feed the extension. Paths kept as stale by graceful restart
are treated as withdrawn, and `--dry-run` warns when a neighbor enables it.

### Start, stop and crash

At start, `gobgpd`:

1. when there are anchors, waits until every exit path has a first verdict
   (bounded by `anchor-wait`), so that anchors start on their preferred path;
2. takes over the owned routes left by a previous run;
3. installs the anchors and the rules;
4. waits for the anchors, then starts the BGP neighbors;
5. keeps the routes of the previous run that BGP has not confirmed yet, and
   removes them when every established session sent End-of-RIB for IPv4
   unicast, `eor-timeout` after the first session, or `max-retention` after
   the start, whichever comes first.

Some BGP speakers, including GoBGP, send End-of-RIB only with graceful
restart; `eor-timeout` covers them.

A clean stop (`SIGTERM`, `SIGINT`) removes every owned object: BGP routes,
then rules, then anchors. After a crash the routes stay in the kernel without
failover until `gobgpd` runs again; run it with an automatic restart, for
example `Restart=always` in systemd.

### Resync

Every `resync-interval`, routes deleted or changed by others are installed
again, owned routes nobody wants are removed (once reconciled), and conflicts
and failed operations are retried.

## Default route and management access

A default route learned from BGP is ignored unless `[fib.default-route]` says
otherwise:

- mode `table` installs it in another table, used only by the traffic that
  traffic rules send there. This is the safest choice.
- mode `main` changes the default route of the server itself. A foreign
  default route with a different metric stays; the lowest metric wins. A
  foreign default with the same metric is a conflict, unless
  `replace-external` is set: it is then replaced, and restored when BGP
  withdraws the default or `gobgpd` stops. The original route is kept in
  memory only: after a crash it is not restored.

To keep management access, make sure the management network has a more
specific route than the default (normally the connected route), list it in
`protected-prefixes`, and prefer mode `table` until the rules are proven in
`observe` mode.

## Coexistence with other route owners

Ownership is the protocol number. `gobgpd` never deletes, replaces or
overwrites a route or rule with another protocol number:

- installing a route whose table, prefix and metric are held by another owner
  is reported as a conflict (log and metric) and retried by the resync;
- if another owner replaces an owned route, `gobgpd` stops managing it;
- a traffic rule whose priority is used by another rule is not installed.

Zebra (FRR or Quagga) can run on the same server. Prefer separate tables:
`--dry-run` warns when Zebra is enabled and rules use the main table. Network
managers that remove unknown routes, such as `systemd-networkd` with
`ManageForeignRoutes=yes`, must be configured to leave the protocol number
alone.

## Operation

Required privileges: `CAP_NET_ADMIN` (routes and rules) and `CAP_NET_RAW`
(ARP probes). Traffic rules need Linux 4.17 or later.

Logs use `Topic=Fib`: decisions (`fib decision`), route and anchor changes
with their status (`fib route`, `fib anchor`), exit path changes, conflicts,
restart reconciliation and kernel errors.

Metrics, under `gobgp_fib_`:

| Metric | Meaning |
| --- | --- |
| `exit_path_usable{exit_path}` | 1 when usable |
| `exit_path_changes_total{exit_path}` | Changes of the usable state |
| `destinations{status}` | Destinations by status: `installed`, `no usable exit path`, `conflict`, `protected prefix`, `ignored`, `observed`, `error` |
| `anchors{status}` | Anchors by status |
| `traffic_rules{status}` | Traffic and exemption rules by status |
| `reconciled` | 1 once the routes of a previous run were reconciled |
| `kernel_errors_total` | Failed kernel operations, including conflicts |
| `routes_repaired_total` | Owned routes installed again after a change by others |

The laboratory in `test/fib/lab.sh` runs two `gobgpd` processes in
unprivileged network namespaces and exercises the behaviour described here.

## Limitations

- IPv4 unicast only.
- One route per destination through the first usable exit path: no ECMP, and
  no backup route in the kernel while `gobgpd` is not running.
- The ARP probe does not detect failures beyond the gateway.
- The configuration is read at start only; there is no API or CLI.
- The original of a replaced default route is lost if `gobgpd` crashes.
- Changing `protocol` leaves the routes and rules of the previous number in
  place; remove them before the change.
