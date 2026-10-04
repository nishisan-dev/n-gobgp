package oc

import (
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestKafkaConfigFormats(t *testing.T) {
	inputs := map[string]string{
		"yaml": `global:
  config:
    as: 65000
    router-id: 192.0.2.1
kafka:
  enabled: true
  spoolDir: /var/lib/gobgp/kafka
  shutdownTimeout: 3s
  defaults:
    producer:
      linger: 20ms
  clusters:
    primary:
      brokers: [localhost:9092]
  sinks:
    raw:
      cluster: primary
      topic: network.bgp.raw
      match:
        eventTypes:
          include: [route, withdraw]
        families:
          include: [bgp-ls]
`,
		"toml": `[global.config]
as=65000
router-id="192.0.2.1"
[kafka]
enabled=true
spoolDir="/var/lib/gobgp/kafka"
shutdownTimeout="3s"
[kafka.defaults.producer]
linger="20ms"
[kafka.clusters.primary]
brokers=["localhost:9092"]
[kafka.sinks.raw]
cluster="primary"
topic="network.bgp.raw"
[kafka.sinks.raw.match.eventTypes]
include=["route","withdraw"]
[kafka.sinks.raw.match.families]
include=["bgp-ls"]
`,
		"json": `{"global":{"config":{"as":65000,"router-id":"192.0.2.1"}},"kafka":{"enabled":true,"spoolDir":"/var/lib/gobgp/kafka","shutdownTimeout":"3s","defaults":{"producer":{"linger":"20ms"}},"clusters":{"primary":{"brokers":["localhost:9092"]}},"sinks":{"raw":{"cluster":"primary","topic":"network.bgp.raw","match":{"eventTypes":{"include":["route","withdraw"]},"families":{"include":["bgp-ls"]}}}}}}`,
	}
	for format, input := range inputs {
		t.Run(format, func(t *testing.T) {
			c, err := ReadConfig(strings.NewReader(input), format)
			require.NoError(t, err)
			require.True(t, c.Kafka.Enabled)
			require.True(t, c.Kafka.Sinks["raw"].IsEnabled())
			require.Equal(t, "protojson", c.Kafka.Sinks["raw"].Format)
			require.Equal(t, 3*time.Second, c.Kafka.ShutdownTimeout)
			require.Equal(t, 20*time.Millisecond, c.Kafka.Clusters["primary"].Producer.Linger)
			require.Equal(t, []string{"route", "withdraw"}, c.Kafka.Sinks["raw"].Match.EventTypes.Include)
		})
	}
}

func TestKafkaConfigDisabledAndValidation(t *testing.T) {
	for _, input := range []string{"", "[kafka]\nenabled=false\n", "[kafka.sinks.disabled]\nenabled=false\n"} {
		c, err := ReadConfig(strings.NewReader(rangeTestGlobal+input), "toml")
		require.NoError(t, err)
		require.False(t, c.Kafka.Enabled)
	}
	base := rangeTestGlobal + `[kafka]
enabled=true
spoolDir="/tmp/spool"
[kafka.clusters.primary]
brokers=["localhost:9092"]
[kafka.sinks.raw]
cluster="primary"
topic="network.bgp.raw"
`
	for name, extra := range map[string]string{
		"block":          "[kafka.sinks.raw.queue]\nonFull=\"block\"\n",
		"invalid source": "[kafka.sinks.raw.match.sources]\ninclude=[\"adj-out\"]\n",
		"unknown field":  "[kafka.sinks.raw.key]\nunsupported=true\n",
		"negative queue": "[kafka.sinks.raw.queue]\nsize=-1\n",
		"ASN overflow":   "[kafka.sinks.raw.match.peers.asns]\ninclude=[4294967296]\n",
	} {
		t.Run(name, func(t *testing.T) { _, err := ReadConfig(strings.NewReader(base+extra), "toml"); require.Error(t, err) })
	}
}

func TestKafkaBootstrapReplayAndSpoolPolicy(t *testing.T) {
	inputs := map[string]string{
		"yaml": `global:
  config: {as: 65000, router-id: 192.0.2.1}
kafka:
  enabled: true
  spoolDir: /tmp/spool
  defaults:
    spool: {failOnError: true}
  clusters:
    primary:
      brokers: [localhost:9092]
  sinks:
    raw:
      cluster: primary
      topic: raw
      initialDump: true
      replayDestination: current
      spool: {failOnError: false}
    inherited:
      cluster: primary
      topic: inherited
`,
		"toml": rangeTestGlobal + `[kafka]
enabled=true
spoolDir="/tmp/spool"
[kafka.defaults.spool]
failOnError=true
[kafka.clusters.primary]
brokers=["localhost:9092"]
[kafka.sinks.raw]
cluster="primary"
topic="raw"
initialDump=true
replayDestination="current"
[kafka.sinks.raw.spool]
failOnError=false
[kafka.sinks.inherited]
cluster="primary"
topic="inherited"
`,
		"json": `{"global":{"config":{"as":65000,"router-id":"192.0.2.1"}},"kafka":{"enabled":true,"spoolDir":"/tmp/spool","defaults":{"spool":{"failOnError":true}},"clusters":{"primary":{"brokers":["localhost:9092"]}},"sinks":{"raw":{"cluster":"primary","topic":"raw","initialDump":true,"replayDestination":"current","spool":{"failOnError":false}},"inherited":{"cluster":"primary","topic":"inherited"}}}}`,
	}
	for format, input := range inputs {
		t.Run(format, func(t *testing.T) {
			c, err := ReadConfig(strings.NewReader(input), format)
			require.NoError(t, err)
			require.True(t, c.Kafka.Sinks["raw"].InitialDump)
			require.False(t, c.Kafka.Sinks["raw"].Spool.IsFatal())
			require.True(t, c.Kafka.Sinks["inherited"].Spool.IsFatal())
			require.Equal(t, "current", c.Kafka.Sinks["raw"].ReplayDestination)
			require.Equal(t, "original", c.Kafka.Sinks["inherited"].ReplayDestination)
		})
	}
	_, err := ReadConfig(strings.NewReader(strings.Replace(inputs["yaml"], "replayDestination: current", "replayDestination: unknown", 1)), "yaml")
	require.ErrorContains(t, err, "replayDestination")
}
