#!/usr/bin/env bash
# Wait for the Kafka protocol from a Linux Docker host, not just open TCP ports.
set -euo pipefail
config=$(mktemp "${TMPDIR:-/tmp}/gobgp-kafka-ready.XXXXXX")
prefix="gobgp-kafka-ready-$$"
cleanup() {
  docker rm -f "$prefix-19092" "$prefix-19094" >/dev/null 2>&1 || true
  rm -f "$config"
}
trap cleanup EXIT
trap 'exit 129' HUP
trap 'exit 130' INT
trap 'exit 143' TERM
printf 'default.api.timeout.ms=60000\nrequest.timeout.ms=5000\n' > "$config"
# The broker image runs as a non-root user; this file contains only timeouts.
chmod 0644 "$config"
for port in 19092 19094; do
  # Host networking lets metadata use the same advertised loopback endpoint
  # as the Go test. The Java AdminClient retries startup failures for 60 seconds.
  docker run --rm --init --name "$prefix-$port" --network host \
    -v "$config:/readiness.properties:ro" \
    --entrypoint /opt/kafka/bin/kafka-topics.sh apache/kafka:3.9.1 \
    --bootstrap-server "127.0.0.1:$port" --command-config /readiness.properties --list
  echo "Kafka broker at 127.0.0.1:$port is ready"
done
