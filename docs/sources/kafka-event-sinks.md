# Kafka event sinks

Kafka event sinks publish observations from GoBGP's existing native watcher
pipeline. IPv4, IPv6 and BGP-LS use the existing protocol parser, path model and
protobuf conversion. Kafka integration performs no topology interpretation,
enrichment, route normalization or additional BGP parsing.

The feature is disabled unless `kafka.enabled: true` is configured. Disabled
Kafka creates no subscriptions, workers, producers or spool files. Each named
sink selects a named cluster and topic; sinks sharing a cluster share one
producer. A matching observation can be published to several sinks.

See [the complete YAML example](../examples/kafka-event-sinks.yaml). The same
fields are supported in TOML and JSON. Check a configuration without creating
spool files or contacting brokers:

```sh
gobgpd --dry-run -f gobgp.yaml -t yaml
```

Create the Kafka topics before enabling sinks. Kafka integration does not
create topics. Changes to Kafka configuration require daemon restart in this
version. SIGHUP and auto-reload keep the running Kafka configuration and log a
warning while applying other supported BGP configuration changes.

## Payload and classification

The payload is standard ProtoJSON for `api.WatchEventResponse`, with one path
per table event, or one peer event. This allows filters to select individual
paths from native notifications containing multiple families or peers. Binary
wire messages are not preserved. The format currently supports only
`protojson`; serializers are separate from transport and delivery.

| Event type | Meaning |
| --- | --- |
| `route` | Announcement observed in a supported view |
| `withdraw` | A path with the existing withdrawal flag |
| `peer-state` | Peer state transition; excludes initialization markers |
| `eor` | End-of-RIB observation; a path with family and no NLRI, matching the existing WatchEvent representation |

| Source | Native view |
| --- | --- |
| `adj-in` | Pre-policy updates, internally generated Adj-RIB-In withdrawals and EOR |
| `post-policy` | Import-policy update/withdraw observations |
| `best` | Best-path changes, including multipath additions and removals |
| `peer` | Peer state transitions |

The current watcher has no independent Adj-RIB-Out or complete Local RIB event
stream. `adj-out` and `local-rib` are rejected rather than silently mapped to a
different view. Best-path events are unavailable when best-path selection is
disabled. EOR is available in `adj-in`, not independently in the other views.
Sinks receive deltas and recovered pending records by default. Set
`initialDump: true` on a sink to request a startup snapshot of its selected
views. `adj-in` dumps established peers' Adj-RIB-In, `post-policy` dumps the
imported paths in the global RIB, `best` dumps selected best/multipath routes,
and `peer` includes current peer initialization events.

Use GoBGP family names such as `ipv4-unicast`, `ipv6-unicast`,
`l3vpn-ipv4-unicast`, `l3vpn-ipv6-unicast`, `l2vpn-evpn` and `ls`. `bgp-ls` is
accepted as an alias for `ls`. BGP-LS is a family, not an event type.

## Filters and ordering

Each sink supports `match.eventTypes`, `match.families`, `match.sources`,
`match.peers.addresses` and `match.peers.asns`, with `include` and `exclude`.
An empty include initially allows everything; excludes always take precedence.
Dimensions are combined with AND and entries in one list with OR. Address and
ASN restrictions both apply when both are present. Peer addresses are exact IP
addresses, not CIDR ranges. IPv4-mapped addresses are normalized for matching.

Events without a family or peer cannot match an include requiring that
attribute. For example, a family include does not match peer-state events.
Without source filters, a sink sees every supported source: one route may have
separate pre-policy, post-policy and best-path observations. The source header
makes those observations distinguishable.

`key.strategy` accepts:

- `peer` (default): normalized peer address; `local` for paths without a peer.
- `peer+family`: peer plus canonical family, separated by `|`; peer-state uses
  `none` for family.
- `none`: no key and no ordering promise.

Publication uses sequence-ordered windows of up to 256 records per sink,
including multiple records for the same peer. The shared idempotent franz-go
producer batches these records and preserves ordering within a partition.
A window waits for all outcomes before retrying its failed records or sending
newer records. ACK deletions share one synchronized spool transaction. A slow partition can
hold its sink's current window; other sinks have independent workers. If an
earlier failure has a later ACK for the same destination/key, publication is
suspended instead of automatically replaying that earlier record behind it.
See the [franz-go producer guarantees](https://pkg.go.dev/github.com/twmb/franz-go/pkg/kgo#RecordDeliveryTimeout).

Committed records have at-least-once delivery: restart can duplicate records
from the last window, including a previously acknowledged prefix. Consumers
should deduplicate event IDs and use the stable `spool-sequence` to reject
stale updates during recovery. Discarded events create gaps. There is no
ordering guarantee between sinks, across changed partition counts, or across
a changed destination. With `peer+family`, peer-state and route events have
different keys and therefore are not ordered relative to one another.

Headers include `content-type`, `serializer`, `schema`, `sink`, `gobgp-version`,
`source`, `event-type`, `observed-at`, `event-id`, `stream-id` and
`spool-sequence`. A stream ID identifies one sink capture invocation; the
sequence is monotonic in that sink's spool and survives replay. Recreating the
spool resets this sequence and requires a fresh bootstrap/reset of consumer
tracking. Optional `collectorId`
becomes `collector-id`. The schema is `api.WatchEventResponse`. An event ID is
shared across fan-out copies and remains stable in durable replay; consumers
can deduplicate per sink using this identifier. Source and event type headers
also distinguish EOR and the observed RIB view without changing protobuf.

## Kafka record JSON examples

The following examples show a **decoded inspection view of a Kafka record**.
Only the object inside `value` is serialized as Kafka's value bytes. `topic`,
`key` and `headers` are separate Kafka record fields; the sink does **not** wrap
BGP messages in this inspection object. Header values and keys are shown as
UTF-8 strings. `key.strategy: none` would instead produce a null key.

The addresses, ASNs, timestamps, IDs and topics below are illustrative. Payloads
were generated using the native GoBGP watcher conversion and the actual
ProtoJSON serializer, rather than a separate BGP JSON model. The examples use
`key.strategy: peer` and `collectorId: edge-router-01`. Whitespace is expanded
for readability; actual producer values use compact ProtoJSON. Build metadata
may change `gobgp-version` in real deployments.

The [complete example record views](../examples/kafka-records.json) are also
available as a JSON file. Jump to:

- [IPv4 announcement](#ipv4-announcement-with-path-attributes).
- [IPv6 best-path announcement](#ipv6-best-path-announcement).
- [BGP-LS node](#bgp-ls-node-with-segment-routing-capabilities),
  [BGP-LS link](#bgp-ls-link-with-te-attributes) and
  [BGP-LS prefix](#bgp-ls-ipv4-prefix-with-prefix-sid).
- [BGP-LS withdrawal](#bgp-ls-link-withdrawal), [EOR](#bgp-ls-end-of-rib) and
  [peer state](#peer-established-with-negotiated-capabilities).
- [Snapshot controls](#snapshot-begin-end-and-failure) and
  [replay/fan-out](#replay-and-fan-out-record-identity).

ProtoJSON details visible in these examples:

- Protobuf field names use lower camel case, and enums use their protobuf names.
- Default scalar values are omitted. `"origin": {}` still identifies an ORIGIN
  attribute with value 0 (IGP); absent `isWithdraw` means false.
- `uint64` values such as the BGP-LS identifier `"42"` are JSON strings; the
  path's `identifier: 7` is a separate, 32-bit Add-Path identifier.
- Protobuf `bytes` are base64 strings. For example, `isisArea: "SQAB"` encodes
  bytes `49 00 01`; `srAlgorithms: "AAE="` encodes bytes `00 01`.
- `age` is the path's timestamp, not elapsed seconds. `observed-at` is the
  observation timestamp. The sink does not normalize TE metrics, bandwidth or
  SID values, and emits the existing converter's empty attribute submessages.
- NLRI may appear both in `path.nlri` and in an MP_REACH attribute. Both are
  preserved from the existing GoBGP representation.

### IPv4 announcement with path attributes

An Adj-RIB-In announcement for `198.51.100.0/24`, with AS sequence
`65001 64496`, next hop `192.0.2.1`, local preference 100, MED 20 and communities
`65001:100` / `65001:200`. Communities appear as their native unsigned 32-bit
values, not colon-separated strings. One Kafka record contains one path.

```json
{
  "topic": "network.bgp.raw",
  "key": "192.0.2.1",
  "headers": {
    "content-type": "application/json",
    "serializer": "protojson",
    "schema": "api.WatchEventResponse",
    "sink": "bgp-raw",
    "gobgp-version": "4.9.0",
    "source": "adj-in",
    "event-type": "route",
    "observed-at": "2026-10-04T12:00:00Z",
    "event-id": "abea3a4d-e6d5-4d39-b52f-c97c40993e6c",
    "stream-id": "11111111-1111-4111-8111-111111111111",
    "collector-id": "edge-router-01",
    "spool-sequence": "4001"
  },
  "value": {
    "table": {
      "paths": [
        {
          "nlri": {
            "prefix": {
              "prefixLen": 24,
              "prefix": "198.51.100.0"
            }
          },
          "pattrs": [
            {
              "origin": {}
            },
            {
              "asPath": {
                "segments": [
                  {
                    "type": "TYPE_AS_SEQUENCE",
                    "numbers": [
                      65001,
                      64496
                    ]
                  }
                ]
              }
            },
            {
              "nextHop": {
                "nextHop": "192.0.2.1"
              }
            },
            {
              "localPref": {
                "localPref": 100
              }
            },
            {
              "multiExitDisc": {
                "med": 20
              }
            },
            {
              "communities": {
                "communities": [
                  4259905636,
                  4259905736
                ]
              }
            }
          ],
          "age": "2026-10-04T12:00:00Z",
          "family": {
            "afi": "AFI_IP",
            "safi": "SAFI_UNICAST"
          },
          "sourceAsn": 65001,
          "sourceId": "192.0.2.1",
          "neighborIp": "192.0.2.1",
          "identifier": 7
        }
      ]
    }
  }
}
```

### IPv6 best-path announcement

A `best` observation for `2001:db8:100::/48`. The IPv6 next hop is carried
by `mpReach.nextHops`. This is a distinct observation from an `adj-in` event;
consumers should keep the source views separate.

```json
{
  "topic": "network.bgp.best",
  "key": "192.0.2.1",
  "headers": {
    "content-type": "application/json",
    "serializer": "protojson",
    "schema": "api.WatchEventResponse",
    "sink": "best-routes",
    "gobgp-version": "4.9.0",
    "source": "best",
    "event-type": "route",
    "observed-at": "2026-10-04T12:00:00Z",
    "event-id": "cce8e35d-9fed-4c9a-b4c9-e08d1850f9ce",
    "stream-id": "22222222-2222-4222-8222-222222222222",
    "collector-id": "edge-router-01",
    "spool-sequence": "8001"
  },
  "value": {
    "table": {
      "paths": [
        {
          "nlri": {
            "prefix": {
              "prefixLen": 48,
              "prefix": "2001:db8:100::"
            }
          },
          "pattrs": [
            {
              "origin": {}
            },
            {
              "asPath": {
                "segments": [
                  {
                    "type": "TYPE_AS_SEQUENCE",
                    "numbers": [
                      65001,
                      64496
                    ]
                  }
                ]
              }
            },
            {
              "mpReach": {
                "family": {
                  "afi": "AFI_IP6",
                  "safi": "SAFI_UNICAST"
                },
                "nextHops": [
                  "2001:db8::1"
                ],
                "nlris": [
                  {
                    "prefix": {
                      "prefixLen": 48,
                      "prefix": "2001:db8:100::"
                    }
                  }
                ]
              }
            },
            {
              "localPref": {
                "localPref": 100
              }
            }
          ],
          "age": "2026-10-04T12:00:00Z",
          "family": {
            "afi": "AFI_IP6",
            "safi": "SAFI_UNICAST"
          },
          "sourceAsn": 65001,
          "sourceId": "192.0.2.1",
          "neighborIp": "192.0.2.1",
          "identifier": 7
        }
      ]
    }
  }
}
```

### BGP-LS node with segment routing capabilities

A snapshot data record describing IS-IS level-2 node `0000.0000.0001` in
ASN 65000, exported by BGP peer `192.0.2.1` (ASN 65001). Node identity lives
inside the NLRI; the Kafka key identifies the exporting peer. Attributes
include node name, router ID, IS-IS area, opaque bytes, SR range and algorithms.
This is the first of three data records in the bootstrap example below.

```json
{
  "topic": "network.bgpls.raw",
  "key": "192.0.2.1",
  "headers": {
    "content-type": "application/json",
    "serializer": "protojson",
    "schema": "api.WatchEventResponse",
    "sink": "bgpls",
    "gobgp-version": "4.9.0",
    "source": "adj-in",
    "event-type": "route",
    "observed-at": "2026-10-04T12:00:00Z",
    "event-id": "929f377b-3917-4ee9-9a64-4afb3ee2e65e",
    "stream-id": "33333333-3333-4333-8333-333333333333",
    "snapshot-id": "44444444-4444-4444-8444-444444444444",
    "snapshot-phase": "data",
    "collector-id": "edge-router-01",
    "spool-sequence": "10001"
  },
  "value": {
    "table": {
      "paths": [
        {
          "nlri": {
            "lsAddrPrefix": {
              "type": "LS_NLRI_TYPE_NODE",
              "nlri": {
                "node": {
                  "localNode": {
                    "asn": 65000,
                    "bgpLsId": 1,
                    "igpRouterId": "0000.0000.0001"
                  }
                }
              },
              "length": 39,
              "protocolId": "LS_PROTOCOL_ID_ISIS_L2",
              "identifier": "42"
            }
          },
          "pattrs": [
            {
              "origin": {}
            },
            {
              "asPath": {
                "segments": [
                  {
                    "type": "TYPE_AS_SEQUENCE",
                    "numbers": [
                      65001,
                      64496
                    ]
                  }
                ]
              }
            },
            {
              "mpReach": {
                "family": {
                  "afi": "AFI_LS",
                  "safi": "SAFI_LS"
                },
                "nextHops": [
                  "192.0.2.1"
                ],
                "nlris": [
                  {
                    "lsAddrPrefix": {
                      "type": "LS_NLRI_TYPE_NODE",
                      "nlri": {
                        "node": {
                          "localNode": {
                            "asn": 65000,
                            "bgpLsId": 1,
                            "igpRouterId": "0000.0000.0001"
                          }
                        }
                      },
                      "length": 39,
                      "protocolId": "LS_PROTOCOL_ID_ISIS_L2",
                      "identifier": "42"
                    }
                  }
                ]
              }
            },
            {
              "ls": {
                "node": {
                  "name": "edge-router-01",
                  "localRouterId": "192.0.2.11",
                  "isisArea": "SQAB",
                  "opaque": "aW52ZW50b3J5OmVkZ2UtMDE=",
                  "srCapabilities": {
                    "ipv4Supported": true,
                    "ipv6Supported": true,
                    "ranges": [
                      {
                        "begin": 16000,
                        "end": 23999
                      }
                    ]
                  },
                  "srAlgorithms": "AAE="
                },
                "link": {
                  "localRouterId": "192.0.2.11"
                },
                "prefix": {},
                "bgpPeerSegment": {},
                "srv6Sid": {},
                "srPolicy": {}
              }
            }
          ],
          "age": "2026-10-04T12:00:00Z",
          "family": {
            "afi": "AFI_LS",
            "safi": "SAFI_LS"
          },
          "sourceAsn": 65001,
          "sourceId": "192.0.2.1",
          "neighborIp": "192.0.2.1",
          "identifier": 7
        }
      ]
    }
  }
}
```

### BGP-LS link with TE attributes

The second snapshot data record describes the directed link from node
`0000.0000.0001` to `0000.0000.0002`. The local/remote link identifiers and
interface/neighbor addresses belong to the NLRI. Link name, administrative
group, IGP/TE metrics, bandwidth, eight unreserved-bandwidth values, adjacency
SID and SRLGs belong to the BGP-LS path attribute. The exporting peer remains
the Kafka key; different topology objects from that peer share this key.

```json
{
  "topic": "network.bgpls.raw",
  "key": "192.0.2.1",
  "headers": {
    "content-type": "application/json",
    "serializer": "protojson",
    "schema": "api.WatchEventResponse",
    "sink": "bgpls",
    "gobgp-version": "4.9.0",
    "source": "adj-in",
    "event-type": "route",
    "observed-at": "2026-10-04T12:00:00Z",
    "event-id": "79f52a8b-54ea-4674-8731-7c71435b2279",
    "stream-id": "33333333-3333-4333-8333-333333333333",
    "snapshot-id": "44444444-4444-4444-8444-444444444444",
    "snapshot-phase": "data",
    "collector-id": "edge-router-01",
    "spool-sequence": "10002"
  },
  "value": {
    "table": {
      "paths": [
        {
          "nlri": {
            "lsAddrPrefix": {
              "type": "LS_NLRI_TYPE_LINK",
              "nlri": {
                "link": {
                  "localNode": {
                    "asn": 65000,
                    "bgpLsId": 1,
                    "igpRouterId": "0000.0000.0001"
                  },
                  "remoteNode": {
                    "asn": 65000,
                    "bgpLsId": 2,
                    "igpRouterId": "0000.0000.0002"
                  },
                  "linkDescriptor": {
                    "linkLocalId": 10,
                    "linkRemoteId": 20,
                    "interfaceAddrIpv4": "192.0.2.11",
                    "neighborAddrIpv4": "192.0.2.12"
                  }
                }
              },
              "length": 97,
              "protocolId": "LS_PROTOCOL_ID_ISIS_L2",
              "identifier": "42"
            }
          },
          "pattrs": [
            {
              "origin": {}
            },
            {
              "asPath": {
                "segments": [
                  {
                    "type": "TYPE_AS_SEQUENCE",
                    "numbers": [
                      65001,
                      64496
                    ]
                  }
                ]
              }
            },
            {
              "mpReach": {
                "family": {
                  "afi": "AFI_LS",
                  "safi": "SAFI_LS"
                },
                "nextHops": [
                  "192.0.2.1"
                ],
                "nlris": [
                  {
                    "lsAddrPrefix": {
                      "type": "LS_NLRI_TYPE_LINK",
                      "nlri": {
                        "link": {
                          "localNode": {
                            "asn": 65000,
                            "bgpLsId": 1,
                            "igpRouterId": "0000.0000.0001"
                          },
                          "remoteNode": {
                            "asn": 65000,
                            "bgpLsId": 2,
                            "igpRouterId": "0000.0000.0002"
                          },
                          "linkDescriptor": {
                            "linkLocalId": 10,
                            "linkRemoteId": 20,
                            "interfaceAddrIpv4": "192.0.2.11",
                            "neighborAddrIpv4": "192.0.2.12"
                          }
                        }
                      },
                      "length": 97,
                      "protocolId": "LS_PROTOCOL_ID_ISIS_L2",
                      "identifier": "42"
                    }
                  }
                ]
              }
            },
            {
              "ls": {
                "node": {
                  "localRouterId": "192.0.2.11"
                },
                "link": {
                  "name": "edge-01-to-edge-02",
                  "localRouterId": "192.0.2.11",
                  "remoteRouterId": "192.0.2.12",
                  "adminGroup": 3,
                  "defaultTeMetric": 20,
                  "igpMetric": 10,
                  "bandwidth": 1250000000,
                  "reservableBandwidth": 1000000000,
                  "unreservedBandwidth": [
                    1000000000,
                    1000000000,
                    900000000,
                    800000000,
                    700000000,
                    600000000,
                    500000000,
                    400000000
                  ],
                  "srAdjacencySid": 24010,
                  "srlgs": [
                    100,
                    200
                  ],
                  "srAdjacencySids": [
                    {
                      "sid": 24010
                    }
                  ]
                },
                "prefix": {},
                "bgpPeerSegment": {},
                "srv6Sid": {},
                "srPolicy": {}
              }
            }
          ],
          "age": "2026-10-04T12:00:00Z",
          "family": {
            "afi": "AFI_LS",
            "safi": "SAFI_LS"
          },
          "sourceAsn": 65001,
          "sourceId": "192.0.2.1",
          "neighborIp": "192.0.2.1",
          "identifier": 7
        }
      ]
    }
  }
}
```

### BGP-LS IPv4 prefix with Prefix-SID

The third snapshot data record associates loopback `198.51.100.11/32` with
node `0000.0000.0001`. Its Prefix-SID value is 16011, with opaque prefix data
encoded as base64. This remains family `AFI_LS` / `SAFI_LS`: it is topology
reachability information, not an IPv4-unicast forwarding route.

```json
{
  "topic": "network.bgpls.raw",
  "key": "192.0.2.1",
  "headers": {
    "content-type": "application/json",
    "serializer": "protojson",
    "schema": "api.WatchEventResponse",
    "sink": "bgpls",
    "gobgp-version": "4.9.0",
    "source": "adj-in",
    "event-type": "route",
    "observed-at": "2026-10-04T12:00:00Z",
    "event-id": "05acc6ad-c7f0-431e-b64e-b564efd07b1f",
    "stream-id": "33333333-3333-4333-8333-333333333333",
    "snapshot-id": "44444444-4444-4444-8444-444444444444",
    "snapshot-phase": "data",
    "collector-id": "edge-router-01",
    "spool-sequence": "10003"
  },
  "value": {
    "table": {
      "paths": [
        {
          "nlri": {
            "lsAddrPrefix": {
              "type": "LS_NLRI_TYPE_PREFIX_V4",
              "nlri": {
                "prefixV4": {
                  "localNode": {
                    "asn": 65000,
                    "bgpLsId": 1,
                    "igpRouterId": "0000.0000.0001"
                  },
                  "prefixDescriptor": {
                    "ipReachability": [
                      "198.51.100.11/32"
                    ]
                  }
                }
              },
              "length": 48,
              "protocolId": "LS_PROTOCOL_ID_ISIS_L2",
              "identifier": "42"
            }
          },
          "pattrs": [
            {
              "origin": {}
            },
            {
              "asPath": {
                "segments": [
                  {
                    "type": "TYPE_AS_SEQUENCE",
                    "numbers": [
                      65001,
                      64496
                    ]
                  }
                ]
              }
            },
            {
              "mpReach": {
                "family": {
                  "afi": "AFI_LS",
                  "safi": "SAFI_LS"
                },
                "nextHops": [
                  "192.0.2.1"
                ],
                "nlris": [
                  {
                    "lsAddrPrefix": {
                      "type": "LS_NLRI_TYPE_PREFIX_V4",
                      "nlri": {
                        "prefixV4": {
                          "localNode": {
                            "asn": 65000,
                            "bgpLsId": 1,
                            "igpRouterId": "0000.0000.0001"
                          },
                          "prefixDescriptor": {
                            "ipReachability": [
                              "198.51.100.11/32"
                            ]
                          }
                        }
                      },
                      "length": 48,
                      "protocolId": "LS_PROTOCOL_ID_ISIS_L2",
                      "identifier": "42"
                    }
                  }
                ]
              }
            },
            {
              "ls": {
                "node": {},
                "link": {},
                "prefix": {
                  "opaque": "bG9vcGJhY2s6ZWRnZS0wMQ==",
                  "srPrefixSid": 16011,
                  "srPrefixSids": [
                    {
                      "sid": 16011
                    }
                  ]
                },
                "bgpPeerSegment": {},
                "srv6Sid": {},
                "srPolicy": {}
              }
            }
          ],
          "age": "2026-10-04T12:00:00Z",
          "family": {
            "afi": "AFI_LS",
            "safi": "SAFI_LS"
          },
          "sourceAsn": 65001,
          "sourceId": "192.0.2.1",
          "neighborIp": "192.0.2.1",
          "identifier": 7
        }
      ]
    }
  }
}
```

### BGP-LS link withdrawal

A later delta removes the link previously described. The event header is
`withdraw`, the path has `isWithdraw: true`, and no snapshot headers are present.
The unchanged stream ID connects it to the completed bootstrap. This native
withdrawal retains the prior attributes; attribute presence must not cause a
consumer to treat it as an announcement. Match the path identity before removal.

```json
{
  "topic": "network.bgpls.raw",
  "key": "192.0.2.1",
  "headers": {
    "content-type": "application/json",
    "serializer": "protojson",
    "schema": "api.WatchEventResponse",
    "sink": "bgpls",
    "gobgp-version": "4.9.0",
    "source": "adj-in",
    "event-type": "withdraw",
    "observed-at": "2026-10-04T12:01:00Z",
    "event-id": "a969a690-0ee8-4a57-bb6d-98ae10548a19",
    "stream-id": "33333333-3333-4333-8333-333333333333",
    "collector-id": "edge-router-01",
    "spool-sequence": "10005"
  },
  "value": {
    "table": {
      "paths": [
        {
          "nlri": {
            "lsAddrPrefix": {
              "type": "LS_NLRI_TYPE_LINK",
              "nlri": {
                "link": {
                  "localNode": {
                    "asn": 65000,
                    "bgpLsId": 1,
                    "igpRouterId": "0000.0000.0001"
                  },
                  "remoteNode": {
                    "asn": 65000,
                    "bgpLsId": 2,
                    "igpRouterId": "0000.0000.0002"
                  },
                  "linkDescriptor": {
                    "linkLocalId": 10,
                    "linkRemoteId": 20,
                    "interfaceAddrIpv4": "192.0.2.11",
                    "neighborAddrIpv4": "192.0.2.12"
                  }
                }
              },
              "length": 97,
              "protocolId": "LS_PROTOCOL_ID_ISIS_L2",
              "identifier": "42"
            }
          },
          "pattrs": [
            {
              "origin": {}
            },
            {
              "asPath": {
                "segments": [
                  {
                    "type": "TYPE_AS_SEQUENCE",
                    "numbers": [
                      65001,
                      64496
                    ]
                  }
                ]
              }
            },
            {
              "mpReach": {
                "family": {
                  "afi": "AFI_LS",
                  "safi": "SAFI_LS"
                },
                "nextHops": [
                  "192.0.2.1"
                ],
                "nlris": [
                  {
                    "lsAddrPrefix": {
                      "type": "LS_NLRI_TYPE_LINK",
                      "nlri": {
                        "link": {
                          "localNode": {
                            "asn": 65000,
                            "bgpLsId": 1,
                            "igpRouterId": "0000.0000.0001"
                          },
                          "remoteNode": {
                            "asn": 65000,
                            "bgpLsId": 2,
                            "igpRouterId": "0000.0000.0002"
                          },
                          "linkDescriptor": {
                            "linkLocalId": 10,
                            "linkRemoteId": 20,
                            "interfaceAddrIpv4": "192.0.2.11",
                            "neighborAddrIpv4": "192.0.2.12"
                          }
                        }
                      },
                      "length": 97,
                      "protocolId": "LS_PROTOCOL_ID_ISIS_L2",
                      "identifier": "42"
                    }
                  }
                ]
              }
            },
            {
              "ls": {
                "node": {
                  "localRouterId": "192.0.2.11"
                },
                "link": {
                  "name": "edge-01-to-edge-02",
                  "localRouterId": "192.0.2.11",
                  "remoteRouterId": "192.0.2.12",
                  "adminGroup": 3,
                  "defaultTeMetric": 20,
                  "igpMetric": 10,
                  "bandwidth": 1250000000,
                  "reservableBandwidth": 1000000000,
                  "unreservedBandwidth": [
                    1000000000,
                    1000000000,
                    900000000,
                    800000000,
                    700000000,
                    600000000,
                    500000000,
                    400000000
                  ],
                  "srAdjacencySid": 24010,
                  "srlgs": [
                    100,
                    200
                  ],
                  "srAdjacencySids": [
                    {
                      "sid": 24010
                    }
                  ]
                },
                "prefix": {},
                "bgpPeerSegment": {},
                "srv6Sid": {},
                "srPolicy": {}
              }
            }
          ],
          "age": "2026-10-04T12:00:00Z",
          "isWithdraw": true,
          "family": {
            "afi": "AFI_LS",
            "safi": "SAFI_LS"
          },
          "sourceAsn": 65001,
          "sourceId": "192.0.2.1",
          "neighborIp": "192.0.2.1",
          "identifier": 7
        }
      ]
    }
  }
}
```

### BGP-LS End-of-RIB

A real BGP EOR observation for `adj-in` / BGP-LS. Its path has a family and
peer identity but no NLRI or path attributes. It is not a withdrawal or a
snapshot completion marker. The existing EOR path model emits an epoch `age`;
use `observed-at` to locate the observation in time. A sink must allow `eor`
to receive this event; the sample `bgpls` sink only selects route/withdraw,
so this example uses the broader `bgp-raw` sink.

```json
{
  "topic": "network.bgp.raw",
  "key": "192.0.2.1",
  "headers": {
    "content-type": "application/json",
    "serializer": "protojson",
    "schema": "api.WatchEventResponse",
    "sink": "bgp-raw",
    "gobgp-version": "4.9.0",
    "source": "adj-in",
    "event-type": "eor",
    "observed-at": "2026-10-04T12:02:00Z",
    "event-id": "525d5950-b2de-4470-a016-6290e66667bd",
    "stream-id": "11111111-1111-4111-8111-111111111111",
    "collector-id": "edge-router-01",
    "spool-sequence": "4003"
  },
  "value": {
    "table": {
      "paths": [
        {
          "age": "1970-01-01T00:00:00Z",
          "family": {
            "afi": "AFI_LS",
            "safi": "SAFI_LS"
          },
          "sourceAsn": 65001,
          "sourceId": "192.0.2.1",
          "neighborIp": "192.0.2.1"
        }
      ]
    }
  }
}
```

### Peer established with negotiated capabilities

The peer transitions to `SESSION_STATE_ESTABLISHED` with IPv4, IPv6 and
BGP-LS capabilities, route refresh and four-octet ASN support. There is no
route family on this event. A sink restricted to `match.families: [bgp-ls]`
would not match it. Initial peer records requested by `initialDump` use
`TYPE_INIT` and snapshot data headers; this example is a normal state delta.

```json
{
  "topic": "network.bgp.raw",
  "key": "192.0.2.1",
  "headers": {
    "content-type": "application/json",
    "serializer": "protojson",
    "schema": "api.WatchEventResponse",
    "sink": "bgp-raw",
    "gobgp-version": "4.9.0",
    "source": "peer",
    "event-type": "peer-state",
    "observed-at": "2026-10-04T12:00:00Z",
    "event-id": "230302ac-2e56-4d59-b10d-8da30bb0de28",
    "stream-id": "11111111-1111-4111-8111-111111111111",
    "collector-id": "edge-router-01",
    "spool-sequence": "4002"
  },
  "value": {
    "peer": {
      "type": "TYPE_STATE",
      "peer": {
        "conf": {
          "localAsn": 65000,
          "neighborAddress": "192.0.2.1",
          "peerAsn": 65001,
          "peerGroup": "route-collectors"
        },
        "state": {
          "localAsn": 65000,
          "neighborAddress": "192.0.2.1",
          "peerAsn": 65001,
          "peerGroup": "route-collectors",
          "sessionState": "SESSION_STATE_ESTABLISHED",
          "adminState": "ADMIN_STATE_UP",
          "remoteCap": [
            {
              "multiProtocol": {
                "family": {
                  "afi": "AFI_IP",
                  "safi": "SAFI_UNICAST"
                }
              }
            },
            {
              "multiProtocol": {
                "family": {
                  "afi": "AFI_IP6",
                  "safi": "SAFI_UNICAST"
                }
              }
            },
            {
              "multiProtocol": {
                "family": {
                  "afi": "AFI_LS",
                  "safi": "SAFI_LS"
                }
              }
            },
            {
              "routeRefresh": {}
            },
            {
              "fourOctetAsn": {
                "asn": 65001
              }
            }
          ],
          "localCap": [
            {
              "multiProtocol": {
                "family": {
                  "afi": "AFI_IP",
                  "safi": "SAFI_UNICAST"
                }
              }
            },
            {
              "multiProtocol": {
                "family": {
                  "afi": "AFI_IP6",
                  "safi": "SAFI_UNICAST"
                }
              }
            },
            {
              "multiProtocol": {
                "family": {
                  "afi": "AFI_LS",
                  "safi": "SAFI_LS"
                }
              }
            },
            {
              "routeRefresh": {}
            },
            {
              "fourOctetAsn": {
                "asn": 65000
              }
            }
          ],
          "routerId": "192.0.2.1"
        },
        "transport": {
          "localAddress": "192.0.2.10",
          "localPort": 179,
          "remotePort": 50000
        }
      }
    }
  }
}
```

### Snapshot begin, end and failure

For the three BGP-LS data records above, the producer's logical sequence is:

| Spool sequence | Phase | Value | Key |
| --- | --- | --- | --- |
| 10000 | begin | `{}` | local |
| 10001 | data | node | 192.0.2.1 |
| 10002 | data | link | 192.0.2.1 |
| 10003 | data | prefix | 192.0.2.1 |
| 10004 | end | `{}`; snapshot-records = 3 | local |
| 10005 | delta | link withdrawal | 192.0.2.1 |

All three data records carry the same snapshot/stream IDs. Controls have an
empty `source`, `event-type: snapshot`, and key `local` under the peer key
strategy. Control/data keys can map to different partitions, so the consumer
must wait for the declared number of distinct data event IDs, not just the end
record. A withdrawal can arrive while another partition still has snapshot
data pending; buffer it until the snapshot is validated.

Begin record:

```json
{
  "topic": "network.bgpls.raw",
  "key": "local",
  "headers": {
    "content-type": "application/json",
    "serializer": "protojson",
    "schema": "api.WatchEventResponse",
    "sink": "bgpls",
    "gobgp-version": "4.9.0",
    "source": "",
    "event-type": "snapshot",
    "observed-at": "2026-10-04T12:00:00Z",
    "event-id": "0f4785d7-d0c8-4d7e-88c7-0a10f8e6d12a",
    "stream-id": "33333333-3333-4333-8333-333333333333",
    "snapshot-id": "44444444-4444-4444-8444-444444444444",
    "snapshot-phase": "begin",
    "collector-id": "edge-router-01",
    "spool-sequence": "10000"
  },
  "value": {}
}
```

Completion record, after all three data records committed to the spool:

```json
{
  "topic": "network.bgpls.raw",
  "key": "local",
  "headers": {
    "content-type": "application/json",
    "serializer": "protojson",
    "schema": "api.WatchEventResponse",
    "sink": "bgpls",
    "gobgp-version": "4.9.0",
    "source": "",
    "event-type": "snapshot",
    "observed-at": "2026-10-04T12:00:00Z",
    "event-id": "28a8fa36-2bd1-4702-b293-6a1fef02b2a4",
    "stream-id": "33333333-3333-4333-8333-333333333333",
    "snapshot-id": "44444444-4444-4444-8444-444444444444",
    "snapshot-phase": "end",
    "collector-id": "edge-router-01",
    "snapshot-records": "3",
    "spool-sequence": "10004"
  },
  "value": {}
}
```

An alternative failure outcome, after only two data records committed:

```json
{
  "topic": "network.bgpls.raw",
  "key": "local",
  "headers": {
    "content-type": "application/json",
    "serializer": "protojson",
    "schema": "api.WatchEventResponse",
    "sink": "bgpls",
    "gobgp-version": "4.9.0",
    "source": "",
    "event-type": "snapshot",
    "observed-at": "2026-10-04T12:00:00Z",
    "event-id": "641adad0-8698-4322-a2a2-4390252eb184",
    "stream-id": "33333333-3333-4333-8333-333333333333",
    "snapshot-id": "44444444-4444-4444-8444-444444444444",
    "snapshot-phase": "failed",
    "collector-id": "edge-router-01",
    "snapshot-records": "2",
    "spool-sequence": "10003"
  },
  "value": {}
}
```

The failed example replaces the successful end outcome; both are not emitted
for the same attempt. `snapshot-records: "2"` reports what was committed, but
`failed` never certifies a usable snapshot. If even the control cannot commit,
there is no end/failed marker and the consumer must treat the attempt as
incomplete. Operational failure details remain in logs/drop/storage metrics.

### Replay and fan-out record identity

Replay has no added `replay: true` field in the payload or headers. A pending
committed record retains its value, key, `event-id`, `stream-id`, snapshot
headers and `spool-sequence`; a broker assigns the actual partition and offset
when the producer sends it. Replay attempts are exposed through metrics.

For the link record above, if the same `bgpls` sink is later configured for
cluster `analytics` and topic `network.bgpls.v2`:

| Setting | Cluster used for that pending record | Topic used |
| --- | --- | --- |
| `replayDestination: original` | original primary cluster | network.bgpls.raw |
| `replayDestination: current` | current analytics cluster | network.bgpls.v2 |

This destination metadata is internal to the spool; it is not an envelope
around the Kafka value. Under `original`, an unavailable original cluster
configuration disables the sink and preserves backlog.

Fan-out copies share the observation's event ID and payload, but have their
own sink headers, stream IDs and spool sequences. For example:

| Record field | bgp-raw sink | analytics-copy sink |
| --- | --- | --- |
| Topic | network.bgp.raw | network.bgp.raw |
| Cluster | primary | analytics |
| Key | 192.0.2.1 | 192.0.2.1 |
| event-id | same observation ID | same observation ID |
| sink | bgp-raw | analytics-copy |
| stream-id | raw sink capture ID | analytics sink capture ID |
| spool-sequence | raw spool's sequence | analytics spool's sequence |
| value | identical ProtoJSON | identical ProtoJSON |

Deduplicate within the intended sink/view, preserve path identity, and do not
assume atomic publication or a shared sequence across these independent sinks.

## Initial dump and consumer bootstrap

Example for a BGP-LS consumer:

```yaml
initialDump: true
replayDestination: original
match:
  families:
    include: [bgp-ls]
  sources:
    include: [adj-in]
```

The native watcher captures RIB path lists and attaches delta observation in
one serialized management operation, restricted to the sources/families
requested by sinks with `initialDump`. This copies references proportional to
the RIB size; it does not serialize protobuf, wait for disk or contact Kafka
inside the BGP loop. Conversion runs outside the loop, emits snapshot data
first, then drains the bounded delta ingress queue. Sinks without `initialDump`
do not receive initial records or control markers. A daemon started before
sessions are configured normally dumps an empty RIB; subsequent session
updates and EOR populate it. Embedded applications can attach after the RIB
has populated. This option runs at sink startup, not on every Kafka consumer
join, and Kafka configuration still requires restart.

All snapshot records carry `snapshot-id` and `snapshot-phase` headers.
`data` records use the same single-path/peer ProtoJSON and the sink's normal
filters. `begin`, `end` and `failed` control records use an empty protobuf
response (`{}`), with `event-type: snapshot`, and bypass the sink's filters.
These controls are operational markers, not BGP EOR. `end` includes
`snapshot-records`, counting the matched, durably committed data records.
Snapshot fields, IDs and the count survive spool replay.

To reconstruct state deterministically:

1. Read every topic partition. Stage snapshot data by snapshot/stream ID and
   deduplicate `event-id`; buffer deltas belonging to that stream.
2. Accept a snapshot only after its `begin`, its `end`, and exactly the declared
   number of distinct data records have arrived. End may arrive before data
   from another partition; its arrival alone is not a completion barrier.
3. Replace the selected view with the staged state, then apply buffered deltas
   in `spool-sequence` order for each key. Keep sources separate and honor
   withdrawals, peer transitions and normal EOR independently. Preserve path
   identity (source, peer, family, NLRI and any path identifier), rather than
   collapsing different paths with the same NLRI.
4. Reject a `failed` or incomplete snapshot. An interrupted invocation must
   not be combined with a new stream's snapshot. Keep the last accepted state
   until a complete replacement is available.

Native ingress loss before snapshot capture completes, or loss of snapshot
records in per-sink admission, serialization or spool quota, invalidates
bootstrap. A failed marker is emitted when possible; if that
marker itself cannot be persisted, the snapshot remains incomplete. Watch
`snapshot_failures_total`, drop counters and `sink_up`. Queues remain bounded
and non-blocking, including during bootstrap: a very large RIB or a busy/slow
host can require larger queues/quota and another capture attempt. No partial
dump is certified complete. Deltas can also be discarded under overload;
a consumer must reconcile after reported loss. The spool protects committed
records, not observations discarded before commitment.

## Buffering and durable replay

The BGP notification path performs only bounded, non-blocking admission. Kafka
and filesystem work run outside the BGP loop. Defaults are:

| Setting | Default |
| --- | --- |
| `ingressQueueSize` | 4096 native notifications |
| `defaults.queue.size` | 4096 serialized records per sink |
| `defaults.queue.onFull` | `drop`; `block` is unsupported |
| `defaults.spool.maxBytes` | 1073741824 bytes per sink |
| `defaults.spool.failOnError` | false |
| `initialDump` | false per sink |
| `replayDestination` | original per sink |
| `defaults.producer.batchMaxBytes` | 1048576 |
| `defaults.producer.linger` | 10ms |
| `defaults.producer.maxBufferedRecords` | 10000 per cluster |
| `defaults.producer.maxBufferedBytes` | 67108864 per cluster |
| `defaults.producer.deliveryTimeout` | 30s |
| `shutdownTimeout` | 10s |

Queue/spool defaults can be overridden by `queue`/`spool` in each sink.
Producer defaults can be overridden by `producer` in each cluster. Producer
acknowledgements are fixed to all in-sync replicas with idempotence enabled.
A sink has at most 256 scheduled/in-flight records, and persistence batches
contain at most 1000 records, collected for up to 10ms.

`spoolDir` is required for active sinks. Use an operator-owned persistent local
directory, preferably on an SSD. Each sink writes `<sink-name>.db` using bbolt
synchronized transactions and per-record checksums. Files are process-exclusive and initially created
with mode 0600; new directories use mode 0700. Names use lowercase letters,
digits, dots, underscores and hyphens, starting with a letter or digit.

**Durability starts at successful local commit**, not at native capture.
Records awaiting persistence can be lost on abrupt process exit. Once
committed, records are published and removed only after Kafka acknowledgement
and a synchronized deletion. An exit between acknowledgement and deletion can
produce a duplicate after restart. This is at-least-once delivery of committed
records, assuming the persistent storage remains intact and the sink is
reactivated with a working destination.

When the backlog quota is reached, pending records are preserved and new
records are discarded with metrics and logs. Quota counts encoded records,
including delivery metadata; the physical bbolt file can be larger and retains
free pages for reuse. Monitor both backlog and physical file size. Separate
sinks have independent queues and storage, but share cluster producer limits
and the underlying disk; fan-out commits are not atomic across sinks.

`replayDestination: original` is the default. Every new record stores its
original cluster name, topic and sorted broker seed list; TLS/SASL secrets are
never stored in the spool. Changing a sink's configured cluster/topic affects
new observations only. Keep the original named clusters configured while
backlog exists. Missing original clusters or changed seed lists fail that sink
at startup and preserve its file. This check identifies configured destinations;
it cannot detect a physical cluster replacement behind unchanged DNS/seeds.

Set `replayDestination: current` explicitly to migrate all pending records to
the current cluster/topic, retaining payloads, keys, IDs and capture headers.
New filters are never applied to old records. Legacy pending records without
destination metadata cannot safely infer an original destination; they require
explicit `current` migration. Empty legacy spools can be reused.

Disabled or removed sinks retain their files and resume replay when
reactivated. Do not rename a sink expecting its backlog to follow automatically.

Shutdown detaches capture, attempts to drain persistence/publication within the
configured deadline, and leaves committed unacknowledged records for restart.
A storage operation stalled inside the operating system can outlive this
budget; the daemon does not wait indefinitely for Kafka or such background
cleanup, and only already committed records have the durability guarantee.

## TLS, authentication and failure handling

Each cluster can enable `tls.enabled`, with optional `caFile`, `serverName`,
and paired `certFile`/`keyFile` for mTLS. TLS certificate verification remains
enabled and the minimum TLS version is 1.2. Without a custom CA, system roots
are used.

SASL accepts `PLAIN`, `SCRAM-SHA-256`, or `SCRAM-SHA-512`. Configure
`usernameEnv` and `passwordEnv` with environment variable names; actual values
are read when initializing the producer and are not printed in configuration
logs. Use TLS when transporting SASL credentials over an untrusted network.

Invalid configuration is rejected. By default, failure opening/checking a
spool disables only that sink: `sink_up` is zero, `storage_errors_total` is
incremented and an error identifies the affected sink/file. Other sinks and
BGP sessions continue. The same isolation applies to local producer setup
errors and unavailable original replay configuration. Backlog/file gauges are
zero for an unopened spool; they do not imply that its file contains no backlog.
Set `spool.failOnError: true` on a sink, or in `defaults.spool`, to explicitly
require successful startup initialization and abort daemon startup on failure.
A per-sink false overrides a true default. Correct permissions or stop another
process holding the file. For corruption, remove the indicated file manually only after
accepting that its pending events will be lost. The daemon never deletes it
automatically. Unsupported spool versions also require operator intervention;
no automatic migration is attempted.

An unavailable broker, DNS/TLS failure or publication timeout retains committed
records for retry with capped backoff. A non-retryable Kafka error, such as a
topic authorization error, suspends publication for that sink until restart;
new records continue accumulating within quota. Other sinks and BGP continue.
A storage failure during operation disables that sink's admission/publication,
preserves its file, logs the error and requires restart after correction.

## Metrics and embedding

The existing metrics endpoint exposes the `gobgp_kafka_` namespace:

- `events_received_total`, `events_matched_total`, `events_published_total`,
  `events_dropped_total`, `serialization_errors_total`, `publish_errors_total`.
- `events_persisted_total`, `events_replayed_total`, `storage_errors_total`,
  `snapshot_failures_total`.
- `ingress_dropped_total`, `ingress_queue_size`, `queue_size`, `queue_capacity`.
- `backlog_records`, `backlog_bytes`, `spool_file_bytes`, `sink_up` and
  `publish_latency_seconds`.

Sink counters use cluster, sink, topic, canonical family and event-type labels;
storage/queue gauges omit family and event type. No prefix or peer IP labels
are used. Native ingress drops count notifications, which may contain multiple
paths; per-sink counters count individual records. Published counts include
acknowledged replay/duplicate attempts. `sink_up` reports observed failures,
not a proactive connectivity check. Repeated error logs are rate limited.

For embedded GoBGP, create `kafka.New(configuration, logger, registerer)`, call
`Start(bgpServer)` before starting configured sessions, and `Close(ctx)` before
stopping the server. The manager enforces `shutdownTimeout` even if `ctx` has no
deadline. `BgpServer.SubscribeEvents` exposes bounded subscriptions separately
from the existing callback API; callbacks run outside the core. Always call
`Stop` and wait for `Done` to finish queued observation processing.

See [integration test instructions](../../test/kafka/README.md).
