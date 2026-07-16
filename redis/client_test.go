package redis

import (
	"bufio"
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"io"
	"math/big"
	"net"
	"strings"
	"testing"
	"time"
)

func TestTLSOptionsConfig(t *testing.T) {
	t.Parallel()

	config, err := (TLSOptions{Enabled: true, ServerName: "redis.internal"}).Config()
	if err != nil {
		t.Fatalf("Config() error = %v", err)
	}
	if config.MinVersion != tls.VersionTLS12 || config.ServerName != "redis.internal" {
		t.Fatalf("unexpected TLS config: %+v", config)
	}

	client := NewClient(Options{Addr: "redis.internal:6380", TLSConfig: config})
	defer client.Close()
	if client.Options().TLSConfig != config || client.Options().Addr != "redis.internal:6380" {
		t.Fatalf("client options did not retain Redis TLS configuration")
	}
}

func TestTLSOptionsRejectIncompleteOrDisabledConfiguration(t *testing.T) {
	t.Parallel()

	for _, options := range []TLSOptions{
		{CertFile: "client.pem"},
		{Enabled: true, CertFile: "client.pem"},
		{Enabled: true, KeyFile: "client-key.pem"},
	} {
		if _, err := options.Config(); err == nil {
			t.Fatalf("Config(%+v) error = nil, want validation failure", options)
		}
	}
}

func TestInfoValue(t *testing.T) {
	t.Parallel()

	info := "# Server\nredis_mode:standalone\n# Replication\nrole:master\n"
	if got := infoValue(info, "redis_mode"); got != "standalone" {
		t.Fatalf("redis_mode = %q, want standalone", got)
	}
	if got := infoValue(info, "missing"); got != "" {
		t.Fatalf("missing value = %q, want empty", got)
	}
}

func TestConnectValidatesTLSStandaloneRedis(t *testing.T) {
	serverConfig, clientConfig := testTLSConfigs(t)
	listener, err := tls.Listen("tcp", "127.0.0.1:0", serverConfig)
	if err != nil {
		if strings.Contains(err.Error(), "operation not permitted") {
			t.Skipf("sandbox denied TLS listener: %v", err)
		}
		t.Fatalf("tls.Listen() error = %v", err)
	}
	serverDone := make(chan struct{})
	go func() {
		defer close(serverDone)
		serveTestRedisTLS(listener)
	}()
	t.Cleanup(func() {
		_ = listener.Close()
		<-serverDone
	})

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	client, err := Connect(ctx, Options{Addr: listener.Addr().String(), TLSConfig: clientConfig})
	if err != nil {
		t.Fatalf("Connect() error = %v", err)
	}
	defer client.Close()
}

func testTLSConfigs(t *testing.T) (*tls.Config, *tls.Config) {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("GenerateKey() error = %v", err)
	}
	template := x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "localhost"},
		DNSNames:     []string{"localhost"},
		NotBefore:    time.Now().Add(-time.Minute),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageKeyEncipherment | x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, &template, &template, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("CreateCertificate() error = %v", err)
	}
	certificate, err := tls.X509KeyPair(
		pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}),
		pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(key)}),
	)
	if err != nil {
		t.Fatalf("X509KeyPair() error = %v", err)
	}
	parsedCertificate, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatalf("ParseCertificate() error = %v", err)
	}
	pool := x509.NewCertPool()
	pool.AddCert(parsedCertificate)
	return &tls.Config{Certificates: []tls.Certificate{certificate}, MinVersion: tls.VersionTLS12}, &tls.Config{
		RootCAs: pool, ServerName: "localhost", MinVersion: tls.VersionTLS12,
	}
}

func serveTestRedisTLS(listener net.Listener) {
	for {
		connection, err := listener.Accept()
		if err != nil {
			return
		}
		go handleTestRedisTLSConnection(connection)
	}
}

func handleTestRedisTLSConnection(connection net.Conn) {
	defer connection.Close()
	reader := bufio.NewReader(connection)
	for {
		command, err := readRESPCommand(reader)
		if err != nil {
			return
		}
		switch strings.ToLower(command[0]) {
		case "hello":
			_, _ = io.WriteString(connection, "-ERR unknown command\r\n")
		case "ping":
			_, _ = io.WriteString(connection, "+PONG\r\n")
		case "info":
			section := ""
			if len(command) > 1 {
				section = strings.ToLower(command[1])
			}
			info := ""
			switch section {
			case "server":
				info = "# Server\nredis_mode:standalone\n"
			case "replication":
				info = "# Replication\nrole:master\n"
			}
			_, _ = fmt.Fprintf(connection, "$%d\r\n%s\r\n", len(info), info)
		default:
			_, _ = io.WriteString(connection, "-ERR unsupported command\r\n")
		}
	}
}

func readRESPCommand(reader *bufio.Reader) ([]string, error) {
	line, err := reader.ReadString('\n')
	if err != nil {
		return nil, err
	}
	if len(line) < 4 || line[0] != '*' {
		return nil, fmt.Errorf("invalid RESP array")
	}
	var count int
	if _, err := fmt.Sscanf(strings.TrimSpace(line[1:]), "%d", &count); err != nil || count < 1 {
		return nil, fmt.Errorf("invalid RESP array count")
	}
	command := make([]string, count)
	for i := range command {
		line, err := reader.ReadString('\n')
		if err != nil {
			return nil, err
		}
		if len(line) < 4 || line[0] != '$' {
			return nil, fmt.Errorf("invalid RESP bulk string")
		}
		var length int
		if _, err := fmt.Sscanf(strings.TrimSpace(line[1:]), "%d", &length); err != nil || length < 0 {
			return nil, fmt.Errorf("invalid RESP bulk string length")
		}
		payload := make([]byte, length+2)
		if _, err := io.ReadFull(reader, payload); err != nil {
			return nil, err
		}
		command[i] = string(payload[:length])
	}
	return command, nil
}
