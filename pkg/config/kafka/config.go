// Package kafka contains Kafka configuration without producer dependencies.
package kafka

import (
	"fmt"
	"regexp"
	"slices"
	"time"

	"github.com/osrg/gobgp/v4/pkg/event"
)

type Queue struct {
	Size   int    `mapstructure:"size"`
	OnFull string `mapstructure:"onFull"`
}
type Spool struct {
	MaxBytes    int64 `mapstructure:"maxBytes"`
	FailOnError *bool `mapstructure:"failOnError"`
}

func (s Spool) IsFatal() bool { return s.FailOnError != nil && *s.FailOnError }

type Producer struct {
	BatchMaxBytes      int32         `mapstructure:"batchMaxBytes"`
	Linger             time.Duration `mapstructure:"linger"`
	MaxBufferedRecords int           `mapstructure:"maxBufferedRecords"`
	MaxBufferedBytes   int           `mapstructure:"maxBufferedBytes"`
	DeliveryTimeout    time.Duration `mapstructure:"deliveryTimeout"`
}
type Defaults struct {
	Queue    Queue    `mapstructure:"queue"`
	Spool    Spool    `mapstructure:"spool"`
	Producer Producer `mapstructure:"producer"`
}
type TLS struct {
	Enabled    bool   `mapstructure:"enabled"`
	CAFile     string `mapstructure:"caFile"`
	CertFile   string `mapstructure:"certFile"`
	KeyFile    string `mapstructure:"keyFile"`
	ServerName string `mapstructure:"serverName"`
}
type SASL struct {
	Mechanism   string `mapstructure:"mechanism"`
	UsernameEnv string `mapstructure:"usernameEnv"`
	PasswordEnv string `mapstructure:"passwordEnv"`
}
type Cluster struct {
	Brokers  []string `mapstructure:"brokers"`
	TLS      TLS      `mapstructure:"tls"`
	SASL     SASL     `mapstructure:"sasl"`
	Producer Producer `mapstructure:"producer"`
}
type Key struct {
	Strategy string `mapstructure:"strategy"`
}
type Sink struct {
	InitialDump       bool              `mapstructure:"initialDump"`
	ReplayDestination string            `mapstructure:"replayDestination"`
	Enabled           *bool             `mapstructure:"enabled"`
	Cluster           string            `mapstructure:"cluster"`
	Topic             string            `mapstructure:"topic"`
	Format            string            `mapstructure:"format"`
	Match             event.MatchConfig `mapstructure:"match"`
	Queue             Queue             `mapstructure:"queue"`
	Spool             Spool             `mapstructure:"spool"`
	Key               Key               `mapstructure:"key"`
}

func (s Sink) IsEnabled() bool { return s.Enabled == nil || *s.Enabled }

type Config struct {
	Enabled          bool               `mapstructure:"enabled"`
	Clusters         map[string]Cluster `mapstructure:"clusters"`
	Sinks            map[string]Sink    `mapstructure:"sinks"`
	Defaults         Defaults           `mapstructure:"defaults"`
	SpoolDir         string             `mapstructure:"spoolDir"`
	IngressQueueSize int                `mapstructure:"ingressQueueSize"`
	ShutdownTimeout  time.Duration      `mapstructure:"shutdownTimeout"`
	CollectorID      string             `mapstructure:"collectorId"`
}

var (
	namePattern  = regexp.MustCompile(`^[a-z0-9][a-z0-9._-]{0,63}$`)
	topicPattern = regexp.MustCompile(`^[a-zA-Z0-9._-]{1,249}$`)
)

func defaultProducer(p, d Producer) Producer {
	if p.BatchMaxBytes == 0 {
		p.BatchMaxBytes = d.BatchMaxBytes
	}
	if p.Linger == 0 {
		p.Linger = d.Linger
	}
	if p.MaxBufferedRecords == 0 {
		p.MaxBufferedRecords = d.MaxBufferedRecords
	}
	if p.MaxBufferedBytes == 0 {
		p.MaxBufferedBytes = d.MaxBufferedBytes
	}
	if p.DeliveryTimeout == 0 {
		p.DeliveryTimeout = d.DeliveryTimeout
	}
	return p
}

func validateProducer(p Producer) error {
	if p.BatchMaxBytes < 512 || p.BatchMaxBytes > 1<<30 || p.Linger < 0 || p.MaxBufferedRecords < 1 ||
		p.MaxBufferedBytes < int(p.BatchMaxBytes) || p.DeliveryTimeout <= 0 {
		return fmt.Errorf("invalid producer limits or durations")
	}
	return nil
}

// Normalize validates only active configuration. Disabled Kafka has no lifecycle.
func (c *Config) Normalize() error {
	if !c.Enabled {
		return nil
	}
	if c.IngressQueueSize == 0 {
		c.IngressQueueSize = 4096
	}
	if c.ShutdownTimeout == 0 {
		c.ShutdownTimeout = 10 * time.Second
	}
	if c.IngressQueueSize < 1 || c.ShutdownTimeout < 0 {
		return fmt.Errorf("kafka: invalid ingress queue size or shutdown timeout")
	}
	if c.Defaults.Queue.Size == 0 {
		c.Defaults.Queue.Size = 4096
	}
	if c.Defaults.Queue.OnFull == "" {
		c.Defaults.Queue.OnFull = "drop"
	}
	if c.Defaults.Spool.MaxBytes == 0 {
		c.Defaults.Spool.MaxBytes = 1 << 30
	}
	c.Defaults.Producer = defaultProducer(c.Defaults.Producer, Producer{1 << 20, 10 * time.Millisecond, 10000, 64 << 20, 30 * time.Second})
	if c.Defaults.Queue.Size < 1 || c.Defaults.Queue.OnFull != "drop" || c.Defaults.Spool.MaxBytes < 1 {
		return fmt.Errorf("kafka: invalid defaults (only non-blocking onFull=drop is supported)")
	}
	if err := validateProducer(c.Defaults.Producer); err != nil {
		return fmt.Errorf("kafka defaults: %w", err)
	}
	active := false
	for name, s := range c.Sinks {
		if !namePattern.MatchString(name) {
			return fmt.Errorf("kafka: invalid sink name %q; use lowercase letters, digits, '.', '_' or '-'", name)
		}
		if !s.IsEnabled() {
			continue
		}
		active = true
		if _, ok := c.Clusters[s.Cluster]; !ok {
			return fmt.Errorf("kafka sink %s: unknown cluster %q", name, s.Cluster)
		}
		if !topicPattern.MatchString(s.Topic) || s.Topic == "." || s.Topic == ".." {
			return fmt.Errorf("kafka sink %s: invalid topic", name)
		}
		if s.Format == "" {
			s.Format = "protojson"
		}
		if s.Format != "protojson" {
			return fmt.Errorf("kafka sink %s: unsupported format %q", name, s.Format)
		}
		if s.Queue.Size == 0 {
			s.Queue.Size = c.Defaults.Queue.Size
		}
		if s.Queue.OnFull == "" {
			s.Queue.OnFull = c.Defaults.Queue.OnFull
		}
		if s.Spool.MaxBytes == 0 {
			s.Spool.MaxBytes = c.Defaults.Spool.MaxBytes
		}
		if s.Spool.FailOnError == nil {
			s.Spool.FailOnError = c.Defaults.Spool.FailOnError
		}
		if s.ReplayDestination == "" {
			s.ReplayDestination = "original"
		}
		if !slices.Contains([]string{"original", "current"}, s.ReplayDestination) {
			return fmt.Errorf("kafka sink %s: invalid replayDestination", name)
		}
		if s.Queue.Size < 1 || s.Queue.OnFull != "drop" || s.Spool.MaxBytes < 1 {
			return fmt.Errorf("kafka sink %s: invalid queue/spool limits (only onFull=drop is supported)", name)
		}
		if s.Key.Strategy == "" {
			s.Key.Strategy = "peer"
		}
		if !slices.Contains([]string{"peer", "peer+family", "none"}, s.Key.Strategy) {
			return fmt.Errorf("kafka sink %s: invalid key strategy", name)
		}
		if _, err := event.NewMatcher(s.Match); err != nil {
			return fmt.Errorf("kafka sink %s: %w", name, err)
		}
		c.Sinks[name] = s
	}
	if active && c.SpoolDir == "" {
		return fmt.Errorf("kafka: spoolDir is required for active sinks")
	}
	for name, cluster := range c.Clusters {
		if !namePattern.MatchString(name) {
			return fmt.Errorf("kafka: invalid cluster name %q", name)
		}
		if len(cluster.Brokers) == 0 {
			return fmt.Errorf("kafka cluster %s: brokers are required", name)
		}
		for _, broker := range cluster.Brokers {
			if broker == "" {
				return fmt.Errorf("kafka cluster %s: empty broker", name)
			}
		}
		cluster.Producer = defaultProducer(cluster.Producer, c.Defaults.Producer)
		if err := validateProducer(cluster.Producer); err != nil {
			return fmt.Errorf("kafka cluster %s: %w", name, err)
		}
		if cluster.TLS.CertFile == "" != (cluster.TLS.KeyFile == "") {
			return fmt.Errorf("kafka cluster %s: TLS certFile and keyFile must be provided together", name)
		}
		if !cluster.TLS.Enabled && (cluster.TLS.CAFile != "" || cluster.TLS.CertFile != "" || cluster.TLS.ServerName != "") {
			return fmt.Errorf("kafka cluster %s: TLS files/serverName require tls.enabled", name)
		}
		a := cluster.SASL
		if a.Mechanism != "" && (!slices.Contains([]string{"PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512"}, a.Mechanism) || a.UsernameEnv == "" || a.PasswordEnv == "") {
			return fmt.Errorf("kafka cluster %s: SASL requires a supported mechanism and usernameEnv/passwordEnv", name)
		}
		if a.Mechanism == "" && (a.UsernameEnv != "" || a.PasswordEnv != "") {
			return fmt.Errorf("kafka cluster %s: SASL credentials require a mechanism", name)
		}
		c.Clusters[name] = cluster
	}
	return nil
}
