# Graceful Restart

This page explains how to configure [Graceful Restart](https://tools.ietf.org/html/rfc4724),
[Graceful Restart Notification Support](https://tools.ietf.org/html/rfc8538) and
[Long Lived Graceful Restart](https://tools.ietf.org/html/draft-uttaro-idr-bgp-persistence-02).
Graceful Restart has two sides. One is restarting speaker which does restart,
the other is receiving speaker (helper speaker) which helps a restarting speaker
to do graceful restart. GoBGP supports both roles.

## Contents

- [Helper speaker](#helper-speaker)
- [Restarting speaker](#restarting-speaker)
- [Graceful Restart Notification Support](#graceful-restart-notification-support)
- [Long Lived Graceful Restart](#long-lived-graceful-restart)
  - [Long Lived Graceful Restart Helper Speaker Configuration](#long-lived-graceful-restart-helper-speaker-configuration)
  - [Long Lived Graceful Restart Restarting Speaker Configuration](#long-lived-graceful-restart-restarting-speaker-configuration)
  - [Combination with normal Graceful Restart](#combination-with-normal-graceful-restart)

## Helper speaker

Below is the configuration to enable helper speaker behavior.

```toml
[global.config]
  as = 64512
  router-id = "192.168.255.1"

[[neighbors]]
  [neighbors.config]
    neighbor-address = "10.0.255.1"
    peer-as = 65001
  [neighbors.graceful-restart.config]
    enabled = true
```

Check graceful restart capability is negotiated.

```shell
$ gobgp n 10.0.255.1
BGP neighbor is 10.0.255.1, remote AS 65001
  BGP version 4, remote router ID 192.168.0.2
  BGP state = BGP_FSM_ESTABLISHED, up for 00:00:36
  BGP OutQ = 0, Flops = 0
  Hold time is 0, keepalive interval is 30 seconds
  Configured hold time is 90, keepalive interval is 30 seconds
  Neighbor capabilities:
    BGP_CAP_MULTIPROTOCOL:
        RF_IPv4_UC:     advertised and received
    BGP_CAP_ROUTE_REFRESH:      advertised and received
    BGP_CAP_GRACEFUL_RESTART:   advertised and received
        Remote: restart time 90 sec
            RF_IPv4_UC
    BGP_CAP_FOUR_OCTET_AS_NUMBER:       advertised and received
  Message statistics:
                         Sent       Rcvd
    Opens:                  1          1
    Notifications:          0          0
    Updates:                2          1
    Keepalives:             2          2
    Route Refresh:          0          0
    Discarded:              0          0
    Total:                  5          4
  Route statistics:
    Advertised:             1
    Received:               0
    Accepted:               0
```

## Restarting speaker

To support restarting speaker behavior, try the configuration below.

```toml
[global.config]
  as = 64512
  router-id = "192.168.255.1"

[[neighbors]]
  [neighbors.config]
    neighbor-address = "10.0.255.1"
    peer-as = 65001
  [neighbors.graceful-restart.config]
    enabled = true
    restart-time = 120
  [[neighbors.afi-safis]]
    [neighbors.afi-safis.config]
    afi-safi-name = "ipv4-unicast"
    [neighbors.afi-safis.mp-graceful-restart.config]
        enabled = true
```

By this configuration, if graceful restart capability is negotiated with the peer,
the peer starts graceful restart helper procedure, when `gobgpd` dies involuntarily or
`SIGINT`, `SIGKILL` signal is sent to `gobgpd`.
Note when `SIGTERM` signal is sent to `gobgpd`, graceful restart negotiated peers
don't start graceful restart helper procedure, since `gobgpd` sends notification
messages to these peers before it die.

When you restart `gobgpd`, add `-r` option to let peers know `gobgpd` is
recovered from graceful restart.

```shell
$ gobgpd -f gobgpd.conf -r
```

Let's see how capability negotiation changes.

```shell
$ gobgp n 10.0.255.1
BGP neighbor is 10.0.255.1, remote AS 65001
  BGP version 4, remote router ID 192.168.0.2
  BGP state = BGP_FSM_ESTABLISHED, up for 00:00:03
  BGP OutQ = 0, Flops = 0
  Hold time is 0, keepalive interval is 30 seconds
  Configured hold time is 90, keepalive interval is 30 seconds
  Neighbor capabilities:
    BGP_CAP_MULTIPROTOCOL:
        RF_IPv4_UC:     advertised and received
    BGP_CAP_ROUTE_REFRESH:      advertised and received
    BGP_CAP_GRACEFUL_RESTART:   advertised and received
        Local: restart time 90 sec, restart flag set
            RF_IPv4_UC, forward flag set
        Remote: restart time 90 sec
            RF_IPv4_UC
    BGP_CAP_FOUR_OCTET_AS_NUMBER:       advertised and received
  Message statistics:
                         Sent       Rcvd
    Opens:                  1          1
    Notifications:          0          0
    Updates:                2          1
    Keepalives:             1          1
    Route Refresh:          0          0
    Discarded:              0          0
    Total:                  4          3
  Route statistics:
    Advertised:             1
    Received:               0
    Accepted:               0
```

You can see `restart flag` and `forward flag` is set.

Without `-r` option, the peers which are under helper procedure will
immediately withdraw all routes which were advertised from `gobgpd`.

Also, when `gobgpd` doesn't recovered within `restart-time`, the peers will
withdraw all routes.
Default value of `restart-time` is equal to `hold-time`.

## Graceful Restart Notification Support

[RFC4724](https://tools.ietf.org/html/rfc4724) specifies gracful restart procedures are triggered only when
the BGP session between graceful restart capable peers turns down without
a notification message for backward compatibility.
[Graceful Restart Notification Support](https://tools.ietf.org/html/rfc8538)
expands this to trigger graceful restart procedures also with a notification message.
To turn on this feature, add `notification-enabled = true` to configuration like below.

```toml
[global.config]
  as = 64512
  router-id = "192.168.255.1"

[[neighbors]]
  [neighbors.config]
    neighbor-address = "10.0.255.1"
    peer-as = 65001
  [neighbors.graceful-restart.config]
    enabled = true
    notification-enabled = true
```

## Long Lived Graceful Restart

### Long Lived Graceful Restart Helper Speaker Configuration

```toml
[global.config]
  as = 64512
  router-id = "192.168.255.1"

[[neighbors]]
  [neighbors.config]
    neighbor-address = "10.0.255.1"
    peer-as = 65001
  [neighbors.graceful-restart.config]
    enabled = true
    long-lived-enabled = true
```

### Long Lived Graceful Restart Restarting Speaker Configuration

Unlike normal graceful restart, long-lived graceful restart supports
restart-time as per address family.

```toml
[global.config]
  as = 64512
  router-id = "192.168.255.1"

[[neighbors]]
  [neighbors.config]
    neighbor-address = "10.0.255.1"
    peer-as = 65001
  [neighbors.graceful-restart.config]
    enabled = true
    long-lived-enabled = true
  [[neighbors.afi-safis]]
    [neighbors.afi-safis.config]
    afi-safi-name = "ipv4-unicast"
    [neighbors.afi-safis.long-lived-graceful-restart.config]
        enabled = true
        restart-time = 100000
```

### Combination with normal Graceful Restart

You can also use long lived graceful restart with normal graceful restart.

```toml
[global.config]
  as = 64512
  router-id = "192.168.255.1"

[[neighbors]]
  [neighbors.config]
    neighbor-address = "10.0.255.1"
    peer-as = 65001
  [neighbors.graceful-restart.config]
    enabled = true
    long-lived-enabled = true
    restart-time = 120
  [[neighbors.afi-safis]]
    [neighbors.afi-safis.config]
    afi-safi-name = "ipv4-unicast"
    [neighbors.afi-safis.mp-graceful-restart.config]
        enabled = true
    [neighbors.afi-safis.long-lived-graceful-restart.config]
        enabled = true
        restart-time = 100000
```

## EOR and Enhanced Route Refresh in n-gobgp

During GR recovery, an EOR cleans stale input routes for its own address
family immediately. Other families retain their recovery state until their
EOR arrives. The peer remains in GR recovery while negotiated families are
still incomplete. Collection observers receive generated withdrawals before
the corresponding EOR completion event, including removals of rejected input
entries present in an initial snapshot. See
[issue #10](https://github.com/nishisan-dev/n-gobgp/issues/10).

EOR and EoRR are separate boundaries. Enhanced Refresh tracks omitted routes
independently of GR/LLGR stale flags. When the peer advertises GR capability,
BoRR for a family is ignored until its initial EOR has arrived. Outbound BoRR
also follows the initial EOR when GR is enabled. These rules follow
[RFC 7313](https://www.rfc-editor.org/rfc/rfc7313.html).

The R03 regression covers a real BGP TCP reconnect with IPv4 and IPv6, followed
by independent EORs. It does not complete the broader roadmap review of all
GR/LLGR timers, peer combinations and lifecycle transitions.

## Local restart with multiple peers in n-gobgp

For local restart (`local-restarting = true`), route selection and initial
advertisements are deferred per address family until EOR has arrived from all
relevant GR peers, or its shared selection deadline expires. A configured,
disconnected peer remains part of that wait. Administratively disabled peers,
peers that negotiate no GR capability and peers advertising the Restart State
(R) bit do not require EOR. The R bit never sets the received-EOR API flags.
A GR helper that advertises no preservation tuples still participates in EOR
synchronization. See [RFC 4724 section 4.1](https://www.rfc-editor.org/rfc/rfc4724.html#section-4.1).

The earliest configured GR deferral time in the startup cohort bounds each
family, starting with its first established session. Reconnection does not
restart that deadline or reopen families already synchronized. IPv4 may finish
while IPv6 remains pending. A disconnected peer also leaves local restart when
its families finish. RTC membership synchronization continues to gate the
initial export of other families, with its own existing deferral timeout.

Input observation continues during deferral. The server retains the latest
input action per source, family, prefix and ADD-PATH identifier, cancels
uninstalled announcements on withdrawal, and applies current import policies
when the family is released. It installs the complete candidate set before
publishing best-path changes. Initial EOR follows that family snapshot,
including empty secondary-route views. Route refresh requests for a pending
family are answered after the initial synchronization.

This is a selection barrier, not an atomic snapshot across all families or an
unbounded history of input changes. Configuration and observer APIs can still
show incomplete input while synchronization is underway. Capacity with large
tables and interoperability with independent implementations remain roadmap
items. See [fork issue #17](https://github.com/nishisan-dev/n-gobgp/issues/17).

## Optional initial configuration barrier in the API

A controller that restores configuration through sequential API calls can
request deferred BGP session activation:

```go
started, err := client.StartBgp(ctx, &api.StartBgpRequest{
    Global: global,
    DeferInitialConfiguration: true,
})
if err != nil {
    return err
}
state := started.GetInitialConfiguration()
if state == nil || !state.GetPending() || state.GetGeneration() == "" {
    return errors.New("server did not acknowledge the configuration barrier")
}
// Add peers, policies and restored paths here; check every operation's error.
_, err = client.CompleteInitialConfiguration(ctx,
    &api.CompleteInitialConfigurationRequest{Generation: state.GetGeneration()})
return err
```

Regenerate the client API from this fork before using the new RPC. Older
servers can ignore unknown request fields, so the controller must check the
acknowledgement before restoring peers. `GetBgp` also returns the generation
and `pending` state, allowing a controller to recover after a lost response.
The generation is opaque, belongs to one successful StartBgp lifecycle, and
must match exactly. Repeating completion for the same active generation is
idempotent. Empty or obsolete generations are rejected.

While pending, outgoing sessions stay idle and incoming BGP connections are
closed. Administrative controls, peer deletion and shutdown remain available.
The controller must explicitly complete configuration; no timer opens the
barrier automatically. StopBgp cancels the pending lifecycle. A canceled queued
completion does not activate sessions, but cancellation cannot undo a
completion already executed; inspect GetBgp after an ambiguous result.

The barrier does not roll back individual configuration operations, restore
RIB state automatically, or mark data-plane convergence. Set the usual GR
local-restarting options separately when restoring a restarted speaker.
Completion releases session activation; EOR and the GR selection deadline
still control routing synchronization afterwards. Kafka event schemas and
capture generations are unchanged. The default request keeps immediate
activation for existing clients. See
[fork issue #16](https://github.com/nishisan-dev/n-gobgp/issues/16) and
[upstream issue #3433](https://github.com/osrg/gobgp/issues/3433).

## Incomplete Enhanced Route Refresh in n-gobgp

An accepted BoRR starts an independent retention deadline for that peer and
address family. The default is **300 seconds**. If EoRR never arrives, the
server removes only input paths still omitted from the refresh, including
rejected input. Ordinary UPDATEs clear pending membership by remote Path-ID;
an import-policy replay does not. Other peers and families retain their input.
The implementation uses the optional retention bound in
[RFC 7313 section 4](https://www.rfc-editor.org/rfc/rfc7313.html#section-4).

Configure the server-wide limit at daemon startup, in seconds:

```shell
gobgpd -f gobgpd.toml --route-refresh-stale-time=600
```

Embedded applications can set the duration when constructing the server:

```go
s := server.NewBgpServer(server.RouteRefreshStaleTime(10 * time.Minute))
```

The daemon accepts an unsigned 32-bit number of seconds. Zero disables this
limit and restores retention until EoRR; non-positive durations disable it in
the embedded option. Select a bound above the observed full-table refresh
replay time. This setting applies to all peers, including dynamically created
neighbors; each peer and family has its own deadline. It is fixed at server
construction and is not a per-neighbor API configuration field.

Every repeated BoRR marks the current input stale again, but does not extend
the deadline of an unfinished cycle. Consequently, a path reannounced before
the latest BoRR needs another UPDATE to remain fresh in that cycle. A later
cycle after completion or expiration receives a new deadline. A timer callback
already queued for an older cycle cannot expire the new one.

EoRR cancels the corresponding timer. Session loss, peer deletion and shutdown
cancel all timers belonging to that peer. In particular, the refresh deadline
from a disconnected session cannot shorten subsequent GR or LLGR retention.
An unpaired late EoRR has no effect. The wire protocol carries no cycle ID, so
it cannot distinguish an old EoRR arriving after a new BoRR from completion of
that new cycle.

Expiration publishes the same real withdrawals to input, post-policy and best
observers as EoRR cleanup, and logs the peer, family, limit and removal count.
It keeps the session established and does not invent EOR, EoRR or a new Kafka
completion event. This limit starts only after an accepted BoRR; it does not
bound waiting for the first BoRR, legacy Route Refresh without demarcation,
or initial GR EOR. Those operations retain their separate semantics. See
[fork issue #19](https://github.com/nishisan-dev/n-gobgp/issues/19).
