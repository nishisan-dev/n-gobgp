# n-gobgp: BGP routing and event streaming in Go

[![Upstream Go Report Card](https://goreportcard.com/badge/github.com/osrg/gobgp)](https://goreportcard.com/report/github.com/osrg/gobgp)
[![Tests](https://github.com/nishisan-dev/n-gobgp/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/nishisan-dev/n-gobgp/actions/workflows/ci.yml)
[![Upstream Go Reference](https://pkg.go.dev/badge/github.com/osrg/gobgp/v4.svg)](https://pkg.go.dev/github.com/osrg/gobgp/v4)
[![LICENSE](https://img.shields.io/github/license/nishisan-dev/n-gobgp.svg?style=flat-square)](LICENSE)

`n-gobgp` is a Nishisan project for Border Gateway Protocol (BGP) routing and
event streaming, based on [GoBGP](https://github.com/osrg/gobgp) and written in
[Go](https://go.dev/). It combines BGP routing with optional Kafka sinks for
native IPv4, IPv6, BGP-LS and peer observations, with filtering, startup RIB
snapshots and durable replay. The primary branch is `main`.

----

## Build and run

Build from this repository to include its extensions. Go 1.25 or later is
required. The Go module retains the upstream name `github.com/osrg/gobgp/v4`.

```sh
git clone https://github.com/nishisan-dev/n-gobgp.git
cd n-gobgp
mkdir -p bin
go build -o bin/gobgpd ./cmd/gobgpd
go build -o bin/gobgp ./cmd/gobgp
```

Follow [Getting Started](docs/sources/getting-started.md) to create a BGP
configuration, then start the daemon and inspect its peers:

```sh
./bin/gobgpd -f gobgpd.conf
# In another terminal:
./bin/gobgp neighbor
```

## Documentation

### Project extensions: Kafka event sinks

Kafka is disabled by default. Enable it in the daemon configuration to publish
native `api.WatchEventResponse` ProtoJSON records to selected topics and
clusters. Sinks use bounded queues and a durable spool; Kafka publication and
disk writes run outside the BGP notification path. Configuration changes require
a daemon restart.

| Guide | What it covers |
| --- | --- |
| [Kafka event sinks](docs/sources/kafka-event-sinks.md) | Event sources, filters, ordering, TLS/SASL, failure handling and metrics |
| [Initial dump and consumer bootstrap](docs/sources/kafka-event-sinks.md#initial-dump-and-consumer-bootstrap) | `initialDump`, snapshot markers and deterministic state reconstruction |
| [Buffering and durable replay](docs/sources/kafka-event-sinks.md#buffering-and-durable-replay) | Spool limits, `failOnError`, delivery guarantees and `replayDestination` |
| [Complete YAML configuration](docs/examples/kafka-event-sinks.yaml) | Multiple clusters and sinks, including BGP-LS |
| [Kafka record JSON examples](docs/sources/kafka-event-sinks.md#kafka-record-json-examples) | IPv4/IPv6, BGP-LS node/link/prefix, withdraw, EOR, peer state and snapshot controls; [reusable JSON file](docs/examples/kafka-records.json) |
| [Kafka integration tests and benchmark](test/kafka/README.md) | Two-cluster delivery tests and publication throughput with a single peer key |

### Using GoBGP

The following guides cover the BGP features available in this checkout. For
BGP-LS route injection and library usage, start with the
[BGP-LS guide](docs/sources/bgp-ls.md), including its SRv6 SID and SR Policy
Candidate Path examples. Use the Kafka guides above to export those observations.

- [Getting Started](docs/sources/getting-started.md)
- [Configuration reference and examples](docs/sources/configuration.md)
- CLI
  - [Typical operation examples](docs/sources/cli-operations.md)
  - [Complete syntax](docs/sources/cli-command-syntax.md)
- [Route Server](docs/sources/route-server.md)
- [Route Reflector](docs/sources/route-reflector.md)
- [Policy](docs/sources/policy.md)
- Zebra Integration
  - [FIB manipulation](docs/sources/zebra.md)
  - [Equal Cost Multipath Routing](docs/sources/zebra-multipath.md)
- [MRT](docs/sources/mrt.md)
- [BMP](docs/sources/bmp.md)
- [EVPN](docs/sources/evpn.md)
- [Flowspec](docs/sources/flowspec.md)
- [RPKI](docs/sources/rpki.md)
- [Metrics](docs/sources/metrics.md)
- [Managing GoBGP with your favorite language with gRPC](docs/sources/grpc-client.md)
- Go Native BGP Library
  - [Basics](docs/sources/lib.md)
  - [BGP-LS](docs/sources/bgp-ls.md)
  - [SR Policy](docs/sources/lib-srpolicy.md)
- [Graceful Restart](docs/sources/graceful-restart.md)
- [Additional Paths](docs/sources/add-paths.md)
- [Peer Group](docs/sources/peer-group.md)
- [Dynamic Neighbor](docs/sources/dynamic-neighbor.md)
- [eBGP Multihop](docs/sources/ebgp-multihop.md)
- [TTL Security](docs/sources/ttl-security.md)
- [BFD](docs/sources/bfd.md)
- [Confederation](docs/sources/bgp-confederation.md)
- Data Center Networking
  - [Unnumbered BGP](docs/sources/unnumbered-bgp.md)
- [Sentry](docs/sources/sentry.md)

### Externals

- [Tutorial: Using GoBGP as an IXP connecting router](http://www.slideshare.net/shusugimoto1986/tutorial-using-gobgp-as-an-ixp-connecting-router)
- [GoBGP.nix: A NixOS module for GoBGP. Containing a working FRR implementation and a rich set of Options](https://github.com/wavelens/gobgp.nix)

## Community, discussion and support

For this project and its extensions, use
[issues](https://github.com/nishisan-dev/n-gobgp/issues) and
[pull requests](https://github.com/nishisan-dev/n-gobgp/pulls) in this repository.
The upstream GoBGP community also has
[Slack](https://join.slack.com/t/gobgp/shared_invite/zt-g9il5j8i-3gZwnXArK0O9Mnn4Yu~IrQ)
for questions and discussion.

You have code or documentation for GoBGP? Awesome! Send a pull
request. No CLA, board members, governance, or other mess. See [`CONTRIBUTING.md`](CONTRIBUTING.md) for info on
code contributing.

## Licensing

GoBGP is licensed under the Apache License, Version 2.0. See
[LICENSE](LICENSE) for the full
license text.
