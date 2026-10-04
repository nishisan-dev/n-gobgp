package kafka

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/osrg/gobgp/v4/api"
	config "github.com/osrg/gobgp/v4/pkg/config/kafka"
	"github.com/osrg/gobgp/v4/pkg/event"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
	"google.golang.org/protobuf/encoding/protojson"
)

// Compare window=1 (the previous per-peer limit) with window=256 against a
// real broker. Includes spool reads, Kafka ACKs and synchronized ACK deletion;
// excludes initial capture, conversion and spool admission. Every record has
// the same peer key and a GoBGP BGP-LS node payload. See test/kafka/README.md.
func BenchmarkKafkaSinglePeerBacklog(b *testing.B) {
	seeds := os.Getenv("GOBGP_KAFKA_TEST_BROKERS")
	if seeds == "" {
		b.Skip("set GOBGP_KAFKA_TEST_BROKERS for real broker benchmark")
	}
	for _, window := range []int{1, 256} {
		b.Run(fmt.Sprintf("window-%d", window), func(b *testing.B) {
			const count = 2048
			b.StopTimer()
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
			defer cancel()
			topic := "gobgp.benchmark." + uuid.NewString()
			admin, err := kgo.NewClient(kgo.SeedBrokers(strings.Split(seeds, ",")...), kgo.ConsumeTopics(topic), kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
			if err != nil {
				b.Fatal(err)
			}
			defer admin.Close()
			create := kmsg.NewPtrCreateTopicsRequest()
			create.Topics = []kmsg.CreateTopicsRequestTopic{{Topic: topic, NumPartitions: 3, ReplicationFactor: 1}}
			response, err := create.RequestWith(ctx, admin)
			if err != nil {
				b.Fatal(err)
			}
			if response.Topics[0].ErrorCode != 0 {
				b.Fatal(response.Topics[0])
			}
			defer func() {
				req := kmsg.NewPtrDeleteTopicsRequest()
				req.TopicNames = []string{topic}
				_, _ = req.RequestWith(ctx, admin)
			}()
			msg := &api.WatchEventResponse{Event: &api.WatchEventResponse_Table{Table: &api.WatchEventResponse_TableEvent{Paths: []*api.Path{{Family: &api.Family{Afi: api.Family_AFI_LS, Safi: api.Family_SAFI_LS}, SourceAsn: 65001, NeighborIp: "192.0.2.1", Nlri: &api.NLRI{Nlri: &api.NLRI_LsAddrPrefix{LsAddrPrefix: &api.LsAddrPrefix{ProtocolId: api.LsProtocolID_LS_PROTOCOL_ID_ISIS_L2, Nlri: &api.LsAddrPrefix_LsNLRI{Nlri: &api.LsAddrPrefix_LsNLRI_Node{Node: &api.LsNodeNLRI{LocalNode: &api.LsNodeDescriptor{Asn: 65001, IgpRouterId: "0000.0000.0001"}}}}}}}}}}}}
			payload, err := protojson.Marshal(msg)
			if err != nil {
				b.Fatal(err)
			}
			for range b.N {
				c := config.Config{Enabled: true, ShutdownTimeout: 2 * time.Minute, SpoolDir: b.TempDir(), Clusters: map[string]config.Cluster{"primary": {Brokers: strings.Split(seeds, ",")}}, Sinks: map[string]config.Sink{"ls": {Cluster: "primary", Topic: topic}}}
				m, err := New(c, slog.New(slog.NewTextHandler(io.Discard, nil)), nil)
				if err != nil {
					b.Fatal(err)
				}
				s := m.sinks[0]
				s.windowSize = window
				batch := make([]*record, count)
				for j := range batch {
					batch[j] = &record{Version: 1, Value: payload, Key: []byte("192.0.2.1"), Destination: &s.destination, Metadata: event.Metadata{EventType: "route", Source: "adj-in", Family: "ls"}}
				}
				if _, err := s.store.append(batch); err != nil {
					b.Fatal(err)
				}
				b.StartTimer()
				if err := m.Start(new(fakeSource)); err != nil {
					b.Fatal(err)
				}
				if err := m.Close(ctx); err != nil {
					b.Fatal(err)
				}
				b.StopTimer()
				// Verify actual broker offsets for this peer follow the durable sequence.
				seen := 0
				for seen < count {
					fetch := admin.PollFetches(ctx)
					if err := fetch.Err(); err != nil {
						b.Fatal(err)
					}
					fetch.EachRecord(func(r *kgo.Record) {
						seen++
						if header(r, "spool-sequence") != fmt.Sprint(seen) {
							b.Fatalf("sequence mismatch at %d: %s", seen, header(r, "spool-sequence"))
						}
					})
				}
			}
			b.ReportMetric(float64(count*b.N)/b.Elapsed().Seconds(), "records/s")
			b.ReportMetric(float64(len(payload)), "payload-B")
		})
	}
}
