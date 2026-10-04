package kafka

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	config "github.com/osrg/gobgp/v4/pkg/config/kafka"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// Run against the disposable clusters in test/kafka/docker-compose.yml.
func TestKafkaBrokerIntegration(t *testing.T) {
	primary := os.Getenv("GOBGP_KAFKA_TEST_BROKERS")
	analytics := os.Getenv("GOBGP_KAFKA_TEST_ANALYTICS_BROKERS")
	if primary == "" || analytics == "" {
		t.Skip("set GOBGP_KAFKA_TEST_BROKERS and GOBGP_KAFKA_TEST_ANALYTICS_BROKERS for broker integration")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	suffix := uuid.NewString()
	rawTopic := "gobgp.raw." + suffix
	copyTopic := "gobgp.copy." + suffix
	analyticsTopic := "gobgp.analytics." + suffix
	consumers := []*kgo.Client{}
	for _, cluster := range []struct {
		brokers string
		topics  []string
	}{{primary, []string{rawTopic, copyTopic}}, {analytics, []string{analyticsTopic}}} {
		client, err := kgo.NewClient(kgo.SeedBrokers(strings.Split(cluster.brokers, ",")...), kgo.ConsumeTopics(cluster.topics...), kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
		require.NoError(t, err)
		consumers = append(consumers, client)
		defer client.Close()
		require.NoError(t, client.Ping(ctx))
		create := kmsg.NewPtrCreateTopicsRequest()
		create.TimeoutMillis = 10000
		for _, topic := range cluster.topics {
			create.Topics = append(create.Topics, kmsg.CreateTopicsRequestTopic{Topic: topic, NumPartitions: 3, ReplicationFactor: 1})
		}
		response, err := create.RequestWith(ctx, client)
		require.NoError(t, err)
		for _, topic := range response.Topics {
			require.Zero(t, topic.ErrorCode)
		}
		defer func() {
			cleanup, stop := context.WithTimeout(context.Background(), 5*time.Second)
			defer stop()
			request := kmsg.NewPtrDeleteTopicsRequest()
			request.TopicNames = cluster.topics
			_, _ = request.RequestWith(cleanup, client)
		}()
	}
	c := testConfig(t)
	c.Clusters["primary"] = config.Cluster{Brokers: strings.Split(primary, ",")}
	c.Clusters["analytics"] = config.Cluster{Brokers: strings.Split(analytics, ",")}
	c.Sinks["raw"] = config.Sink{Cluster: "primary", Topic: rawTopic}
	c.Sinks["copy"] = config.Sink{Cluster: "primary", Topic: copyTopic}
	c.Sinks["analytics"] = config.Sink{Cluster: "analytics", Topic: analyticsTopic}
	m, err := New(c, nil, prometheus.NewRegistry())
	require.NoError(t, err)
	src := new(fakeSource)
	require.NoError(t, m.Start(src))
	defer closeManager(t, m)
	original := testObservation()
	src.callback(original)
	records := []*kgo.Record{}
	for i, client := range consumers {
		expected := 2
		if i == 1 {
			expected = 1
		}
		for count := 0; count < expected; {
			fetches := client.PollFetches(ctx)
			require.NoError(t, fetches.Err())
			fetches.EachRecord(func(r *kgo.Record) { records = append(records, r); count++ })
		}
	}
	require.Len(t, records, 3)
	for _, r := range records {
		require.Equal(t, header(records[0], "event-id"), header(r, "event-id"))
		require.Equal(t, "192.0.2.1", string(r.Key))
		require.JSONEq(t, string(records[0].Value), string(r.Value))
	}
	waitEmpty(t, m)
}
