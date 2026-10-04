package kafka

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	config "github.com/osrg/gobgp/v4/pkg/config/kafka"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl"
)

func testCertificates(t *testing.T) (string, string, string, tls.Certificate, *x509.CertPool) {
	t.Helper()
	dir := t.TempDir()
	pub, key, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	ca := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "Kafka test CA"}, NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour), IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature}
	der, err := x509.CreateCertificate(rand.Reader, ca, ca, pub, key)
	require.NoError(t, err)
	caPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	roots := x509.NewCertPool()
	require.True(t, roots.AppendCertsFromPEM(caPEM))
	caPath := filepath.Join(dir, "ca.pem")
	require.NoError(t, os.WriteFile(caPath, caPEM, 0o600))
	issue := func(name string, usage x509.ExtKeyUsage) (string, string, tls.Certificate) {
		leafPub, leafKey, err := ed25519.GenerateKey(rand.Reader)
		require.NoError(t, err)
		template := &x509.Certificate{SerialNumber: big.NewInt(2), Subject: pkix.Name{CommonName: name}, DNSNames: []string{"localhost"}, IPAddresses: []net.IP{net.ParseIP("127.0.0.1")}, NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour), KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{usage}}
		cert, err := x509.CreateCertificate(rand.Reader, template, ca, leafPub, key)
		require.NoError(t, err)
		private, err := x509.MarshalPKCS8PrivateKey(leafKey)
		require.NoError(t, err)
		certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: cert})
		keyPEM := pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: private})
		cp, kp := filepath.Join(dir, name+".pem"), filepath.Join(dir, name+".key")
		require.NoError(t, os.WriteFile(cp, certPEM, 0o600))
		require.NoError(t, os.WriteFile(kp, keyPEM, 0o600))
		pair, err := tls.X509KeyPair(certPEM, keyPEM)
		require.NoError(t, err)
		return cp, kp, pair
	}
	certPath, keyPath, _ := issue("client", x509.ExtKeyUsageClientAuth)
	_, _, server := issue("server", x509.ExtKeyUsageServerAuth)
	return caPath, certPath, keyPath, server, roots
}

func TestProducerMutualTLSAndValidation(t *testing.T) {
	ca, cert, key, serverCert, roots := testCertificates(t)
	c := testConfig(t)
	cluster := c.Clusters["primary"]
	cluster.TLS = config.TLS{Enabled: true, CAFile: ca, CertFile: cert, KeyFile: key, ServerName: "localhost"}
	c.Clusters["primary"] = cluster
	require.NoError(t, c.Normalize())
	p, err := newProducer("primary", c.Clusters["primary"])
	require.NoError(t, err)
	defer p.Close()
	tlsConfig := p.(*kgo.Client).OptValue(kgo.DialTLSConfig).(*tls.Config)
	left, right := net.Pipe()
	defer left.Close()
	defer right.Close()
	require.NoError(t, left.SetDeadline(time.Now().Add(3*time.Second)))
	require.NoError(t, right.SetDeadline(time.Now().Add(3*time.Second)))
	server := tls.Server(left, &tls.Config{MinVersion: tls.VersionTLS12, Certificates: []tls.Certificate{serverCert}, ClientAuth: tls.RequireAndVerifyClientCert, ClientCAs: roots})
	done := make(chan error, 1)
	go func() { done <- server.Handshake() }()
	client := tls.Client(right, tlsConfig)
	require.NoError(t, client.HandshakeContext(context.Background()))
	require.NoError(t, <-done)
	require.NotEmpty(t, server.ConnectionState().PeerCertificates)
	cluster = c.Clusters["primary"]
	cluster.TLS.CAFile = filepath.Join(t.TempDir(), "missing.pem")
	_, err = newProducer("primary", cluster)
	require.ErrorContains(t, err, "TLS CA")
}

func TestProducerSASLMechanismsAndMissingCredentials(t *testing.T) {
	t.Setenv("GOBGP_TEST_SASL_USER", "operator")
	t.Setenv("GOBGP_TEST_SASL_PASSWORD", "test-secret")
	for _, mechanism := range []string{"PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512"} {
		t.Run(mechanism, func(t *testing.T) {
			c := testConfig(t)
			cluster := c.Clusters["primary"]
			cluster.SASL = config.SASL{Mechanism: mechanism, UsernameEnv: "GOBGP_TEST_SASL_USER", PasswordEnv: "GOBGP_TEST_SASL_PASSWORD"}
			c.Clusters["primary"] = cluster
			require.NoError(t, c.Normalize())
			p, err := newProducer("primary", c.Clusters["primary"])
			require.NoError(t, err)
			defer p.Close()
			mechanisms := p.(*kgo.Client).OptValue(kgo.SASL).([]sasl.Mechanism)
			require.Len(t, mechanisms, 1)
			require.Equal(t, mechanism, mechanisms[0].Name())
		})
	}
	c := testConfig(t)
	cluster := c.Clusters["primary"]
	cluster.SASL = config.SASL{Mechanism: "PLAIN", UsernameEnv: "GOBGP_TEST_SASL_USER", PasswordEnv: "GOBGP_TEST_MISSING_PASSWORD"}
	c.Clusters["primary"] = cluster
	require.NoError(t, c.Normalize())
	_, err := newProducer("primary", c.Clusters["primary"])
	require.ErrorContains(t, err, "GOBGP_TEST_MISSING_PASSWORD")
	require.NotContains(t, err.Error(), "operator")
	require.NotContains(t, err.Error(), "test-secret")
}
