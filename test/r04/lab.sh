#!/usr/bin/env bash
# Run only on the authorized cloud Docker host or a disposable CI runner.
set -euo pipefail
repo=$(cd "$(dirname "$0")/../.." && pwd)
state_dir=${R04_OUTPUT_DIR:-$(mktemp -d /tmp/ngobgp-r04-lab.XXXXXX)}
mkdir -p "$state_dir"
if find "$state_dir" -mindepth 1 -maxdepth 1 -print -quit | read -r _; then
  echo 'R04_OUTPUT_DIR must be empty; preserve earlier evidence and choose a fresh directory.' >&2
  exit 1
fi
state_dir=$(cd "$state_dir" && pwd)
prefix="ngobgp-r04-$(date +%s)-$$"
go_image=${R04_GO_IMAGE:-golang:1.27.3@sha256:f20d83a83e18dbd2a4714b8ffd4b30b287b6243962dd75608e86abcc63ba4efc}
frr_image=${R04_FRR_IMAGE:-quay.io/frrouting/frr:10.2.3@sha256:b48e58300fea8288a8ea0ce62c6f722945bb9dda532526fb9ef12f88278e4d66}
kafka_image=${R04_KAFKA_IMAGE:-apache/kafka:3.9.1@sha256:4ceccc577f03f51f6af8dbfda55194d0d892f4fa7913ffbded567ce3895622ed}
# The read-only checkout can belong to the CI runner rather than container root.
# Trust only this mounted path, preserving Go VCS stamping without global changes.
go_git_env=(-e GIT_CONFIG_COUNT=1 -e GIT_CONFIG_KEY_0=safe.directory -e GIT_CONFIG_VALUE_0=/work)
mod_cache=${R04_MODULE_CACHE:-$state_dir/mod}
build_cache=${R04_BUILD_CACHE:-$state_dir/build}
mkdir -p "$mod_cache" "$build_cache" "$state_dir/frr"
network_created=0
watch_pid=''
cleanup() {
  touch "$state_dir/finished"
  if [[ -n "$watch_pid" ]]; then kill "$watch_pid" 2>/dev/null || true; wait "$watch_pid" 2>/dev/null || true; fi
  docker rm -f "$prefix-collector" "$prefix-generator1" "$prefix-generator2" "$prefix-rr" "$prefix-kafka" >/dev/null 2>&1 || true
  if [[ "$network_created" == 1 ]]; then docker network rm "$prefix" >/dev/null 2>&1 || true; fi
}
trap cleanup EXIT
# Preparation can use external network; the actual laboratory network is internal.
for image in "$go_image" "$frr_image" "$kafka_image"; do docker image inspect "$image" >/dev/null 2>&1 || docker pull "$image"; done
docker run --rm "${go_git_env[@]}" -v "$repo:/work:ro" -v "$mod_cache:/go/pkg/mod" -w /work "$go_image" go mod download
docker image inspect "$go_image" "$frr_image" "$kafka_image" > "$state_dir/images.json"
docker run --rm --entrypoint /bin/sh "$frr_image" -c 'cat /etc/frr/daemons' > "$state_dir/frr/daemons"
sed -i -e 's/^bgpd=no/bgpd=yes/' -e 's/^zebra=no/zebra=yes/' "$state_dir/frr/daemons"
cat > "$state_dir/frr/frr.conf" <<'FRR'
frr defaults traditional
hostname r04-reflector
log stdout informational
service integrated-vtysh-config
bgp no-rib
router bgp 65000
 bgp router-id 10.231.84.2
 bgp cluster-id 10.231.84.2
 no bgp default ipv4-unicast
 neighbor CLIENTS peer-group
 neighbor CLIENTS remote-as 65000
 neighbor CLIENTS timers 3 90
 neighbor CLIENTS timers connect 1
 neighbor CLIENTS advertisement-interval 0
 neighbor 10.231.84.4 peer-group CLIENTS
 neighbor 10.231.84.5 peer-group CLIENTS
 neighbor 10.231.84.6 peer-group CLIENTS
 address-family ipv4 unicast
  neighbor CLIENTS activate
  neighbor CLIENTS route-reflector-client
 exit-address-family
 address-family ipv6 unicast
  neighbor CLIENTS activate
  neighbor CLIENTS route-reflector-client
 exit-address-family
FRR
touch "$state_dir/frr/vtysh.conf"
chmod 644 "$state_dir/frr/daemons" "$state_dir/frr/frr.conf" "$state_dir/frr/vtysh.conf"
docker network create --internal --subnet 10.231.84.0/24 --ipv6 --subnet fd42:231:84::/64 "$prefix" > "$state_dir/network-id"
network_created=1
docker run -d --name "$prefix-kafka" --network "$prefix" --ip 10.231.84.3 --ip6 fd42:231:84::3 --cpus=1 --memory=1g \
 -e KAFKA_NODE_ID=1 -e KAFKA_PROCESS_ROLES=broker,controller -e KAFKA_LISTENERS=PLAINTEXT://:9092,CONTROLLER://:9093 \
 -e KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://10.231.84.3:9092 -e KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT \
 -e KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER -e KAFKA_CONTROLLER_QUORUM_VOTERS=1@10.231.84.3:9093 \
 -e KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1 -e KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=1 -e KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=1 \
 -e 'KAFKA_HEAP_OPTS=-Xmx512m -Xms256m' "$kafka_image" > "$state_dir/kafka-id"
docker run -d --name "$prefix-rr" --network "$prefix" --ip 10.231.84.2 --ip6 fd42:231:84::2 --cpus=0.5 --memory=512m \
 --cap-add NET_ADMIN --cap-add NET_RAW --cap-add SYS_ADMIN -v "$state_dir/frr:/etc/frr" "$frr_image" > "$state_dir/rr-id"
for peer in 1 2; do
  docker run -d --name "$prefix-generator$peer" --network "$prefix" --ip "10.231.84.$((peer+4))" --ip6 "fd42:231:84::$((peer+4))" --cpus=0.25 --memory=128m \
   -e PEER_NUMBER="$peer" -v "$state_dir:/lab" -v "$repo/test/r04:/scripts:ro" "$go_image" python3 /scripts/generator.py > "$state_dir/generator-$peer.id"
done
watch_broker() {
 local stopped=0 started=0
 while [[ ! -e "$state_dir/finished" ]]; do
  if [[ -e "$state_dir/broker-stop.request" && "$stopped" == 0 ]]; then docker stop -t 5 "$prefix-kafka" > "$state_dir/broker-stop.log"; touch "$state_dir/broker-stop.done"; stopped=1; fi
  if [[ -e "$state_dir/broker-start.request" && "$started" == 0 ]]; then docker start "$prefix-kafka" > "$state_dir/broker-start.log"; touch "$state_dir/broker-start.done"; started=1; fi
  sleep 0.1
 done
}
watch_broker &
watch_pid=$!
set +e
docker run --rm "${go_git_env[@]}" --name "$prefix-collector" --network "$prefix" --ip 10.231.84.4 --ip6 fd42:231:84::4 --cpus=3 --memory=6g \
 -e NGOBGP_R04_LAB=1 -e LAB_DIR=/lab -v "$state_dir:/lab" -v "$repo:/work:ro" -v "$mod_cache:/go/pkg/mod" -v "$build_cache:/root/.cache/go-build" -w /work \
 "$go_image" sh -c 'set -e; go build -race -o /tmp/kafka-reference-consumer ./cmd/kafka-reference-consumer; go version -m /tmp/kafka-reference-consumer > /lab/reference-build.txt; NGOBGP_R04_REFERENCE=/tmp/kafka-reference-consumer go test -v -race ./test/r04 -run TestR04ReflectorHotRecovery -count=1 -timeout=300s'  > "$state_dir/collector.log" 2>&1
result=$?
set -e
touch "$state_dir/finished"
wait "$watch_pid" || true
watch_pid=''
docker exec -u 0 "$prefix-rr" chmod -R a+rX /etc/frr >/dev/null 2>&1 || true
docker logs "$prefix-rr" > "$state_dir/rr.log" 2>&1 || true
docker logs "$prefix-kafka" > "$state_dir/kafka.log" 2>&1 || true
docker exec "$prefix-rr" vtysh -c 'show bgp summary json' > "$state_dir/rr-summary.json" 2>&1 || true
 docker exec "$prefix-rr" vtysh -c 'show bgp ipv4 unicast 10.1.0.1/32 json' > "$state_dir/rr-ipv4-sample.json" 2>&1 || true
 docker exec "$prefix-rr" vtysh -c 'show bgp ipv6 unicast fd10:1::1/128 json' > "$state_dir/rr-ipv6-sample.json" 2>&1 || true
cat "$state_dir/collector.log"
echo "R04 evidence: $state_dir"
exit "$result"
