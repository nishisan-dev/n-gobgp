package kafka

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"

	config "github.com/osrg/gobgp/v4/pkg/config/kafka"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl/plain"
	"github.com/twmb/franz-go/pkg/sasl/scram"
)

type producer interface {
	Produce(context.Context, *kgo.Record, func(*kgo.Record, error))
	Flush(context.Context) error
	Close()
}

type producerFactory func(string, config.Cluster) (producer, error)

func newProducer(name string, c config.Cluster) (producer, error) {
	p := c.Producer
	opts := []kgo.Opt{
		kgo.SeedBrokers(c.Brokers...), kgo.ClientID("gobgp-" + name),
		kgo.RequiredAcks(kgo.AllISRAcks()), kgo.ProducerLinger(p.Linger), kgo.ProducerBatchMaxBytes(p.BatchMaxBytes),
		kgo.MaxBufferedRecords(p.MaxBufferedRecords), kgo.MaxBufferedBytes(p.MaxBufferedBytes), kgo.RecordDeliveryTimeout(p.DeliveryTimeout),
	}
	if c.TLS.Enabled {
		t := &tls.Config{MinVersion: tls.VersionTLS12, ServerName: c.TLS.ServerName}
		if c.TLS.CAFile != "" {
			pem, err := os.ReadFile(c.TLS.CAFile)
			if err != nil {
				return nil, fmt.Errorf("TLS CA: %w", err)
			}
			t.RootCAs = x509.NewCertPool()
			if !t.RootCAs.AppendCertsFromPEM(pem) {
				return nil, fmt.Errorf("TLS CA file contains no certificates")
			}
		}
		if c.TLS.CertFile != "" {
			cert, err := tls.LoadX509KeyPair(c.TLS.CertFile, c.TLS.KeyFile)
			if err != nil {
				return nil, fmt.Errorf("TLS client certificate: %w", err)
			}
			t.Certificates = []tls.Certificate{cert}
		}
		opts = append(opts, kgo.DialTLSConfig(t))
	}
	if c.SASL.Mechanism != "" {
		user := os.Getenv(c.SASL.UsernameEnv)
		password := os.Getenv(c.SASL.PasswordEnv)
		if user == "" || password == "" {
			return nil, fmt.Errorf("SASL requires nonempty environment variables %s and %s", c.SASL.UsernameEnv, c.SASL.PasswordEnv)
		}
		switch c.SASL.Mechanism {
		case "PLAIN":
			opts = append(opts, kgo.SASL(plain.Auth{User: user, Pass: password}.AsMechanism()))
		case "SCRAM-SHA-256":
			opts = append(opts, kgo.SASL(scram.Auth{User: user, Pass: password}.AsSha256Mechanism()))
		case "SCRAM-SHA-512":
			opts = append(opts, kgo.SASL(scram.Auth{User: user, Pass: password}.AsSha512Mechanism()))
		}
	}
	return kgo.NewClient(opts...)
}
