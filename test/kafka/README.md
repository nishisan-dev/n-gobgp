# Kafka integration tests

On a Linux Docker host, start the two disposable Kafka clusters, wait for their
protocol endpoints, and run the broker integration test:

```sh
docker compose -f test/kafka/docker-compose.yml up -d
bash test/kafka/wait-ready.sh
GOBGP_KAFKA_TEST_BROKERS=127.0.0.1:19092 \
GOBGP_KAFKA_TEST_ANALYTICS_BROKERS=127.0.0.1:19094 \
go test -race ./pkg/sink/kafka -run TestKafkaBrokerIntegration -count=1
docker compose -f test/kafka/docker-compose.yml down
```

The readiness check uses the Kafka AdminClient from the broker image, through
the same advertised loopback endpoints as the Go test. It waits at most 60
seconds per broker and removes its probe containers on exit or cancellation.

The test creates and deletes its own uniquely named topics. Broker integration
is skipped unless both environment variables are provided. The regular unit
suite covers bounded admission, fan-out, retry ordering, quota, corrupt/locked
storage, shutdown deadlines, destination migration, and recovery after abrupt
process exit without needing Kafka.

## Single-peer publication benchmark

With the primary test broker running:

```sh
GOBGP_KAFKA_TEST_BROKERS=127.0.0.1:19092 \
go test ./pkg/sink/kafka -run '^$' \
  -bench '^BenchmarkKafkaSinglePeerBacklog$' -benchtime=1x -count=1
```

The benchmark creates/deletes a topic and drains 2048 committed BGP-LS node
records with the same peer key. It compares a one-record window with the new
256-record window, using the same producer defaults (10ms linger, idempotence,
all-ISR ACKs). It measures spool reads, Kafka ACKs and synchronized ACK deletion;
it excludes initial capture/conversion and spool admission. It then consumes
the records and checks every durable sequence for ordering.

Example local run on Linux amd64, Intel Core 7 150U, Apache Kafka 3.9.1 in
Docker, one broker with replication factor one and a minimal 254-byte BGP-LS
node payload:

| Window | Time for 2048 records | Records/s |
| --- | --- | --- |
| 1 | 33.994s | 60.25 |
| 256 | 0.265s | 7730 |

These results isolate the per-record ACK/commit bottleneck; they are not a
production capacity estimate. Larger payloads, replicated clusters, latency,
disk performance and simultaneous sinks change throughput. Rerun against the
target environment before sizing queues or spool quotas.
