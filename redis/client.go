package redis

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
	"strings"

	goredis "github.com/redis/go-redis/v9"
)

// TLSOptions configures TLS for a direct Redis connection. Certificate and
// key must be supplied together when Redis requires client authentication.
// When CAFile is empty, the host's system certificate pool is used.
type TLSOptions struct {
	Enabled    bool
	CAFile     string
	CertFile   string
	KeyFile    string
	ServerName string
}

// Config returns the TLS configuration used by Redis clients.
func (o TLSOptions) Config() (*tls.Config, error) {
	if !o.Enabled {
		if o.CAFile != "" || o.CertFile != "" || o.KeyFile != "" || o.ServerName != "" {
			return nil, fmt.Errorf("redis TLS options require TLS to be enabled")
		}
		return nil, nil
	}
	if (o.CertFile == "") != (o.KeyFile == "") {
		return nil, fmt.Errorf("redis TLS client certificate and key must be supplied together")
	}

	config := &tls.Config{MinVersion: tls.VersionTLS12, ServerName: o.ServerName}
	if o.CAFile != "" {
		pem, err := os.ReadFile(o.CAFile)
		if err != nil {
			return nil, fmt.Errorf("read Redis TLS CA file: %w", err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pem) {
			return nil, fmt.Errorf("read Redis TLS CA file: no certificates found")
		}
		config.RootCAs = pool
	}
	if o.CertFile != "" {
		certificate, err := tls.LoadX509KeyPair(o.CertFile, o.KeyFile)
		if err != nil {
			return nil, fmt.Errorf("load Redis TLS client certificate: %w", err)
		}
		config.Certificates = []tls.Certificate{certificate}
	}
	return config, nil
}

// NewClient is the single client constructor used by TaskForge. It creates a
// direct Redis client only; Sentinel and Redis Cluster clients are deliberately
// unsupported because TaskForge's Lua scripts span keys that are not proven to
// share a Redis Cluster hash slot.
func NewClient(options Options) *goredis.Client {
	addr := options.Addr
	if addr == "" {
		addr = defaultAddr
	}
	return goredis.NewClient(&goredis.Options{
		Addr:      addr,
		Password:  options.Password,
		DB:        options.DB,
		TLSConfig: options.TLSConfig,
	})
}

// Connect creates and validates a direct connection. The returned client is
// owned by the caller and must be closed. Validation rejects Sentinel, Redis
// Cluster, and read-only replica endpoints before TaskForge starts work.
func Connect(ctx context.Context, options Options) (*goredis.Client, error) {
	if options.Client != nil {
		return nil, fmt.Errorf("connect Redis: Options.Client is already set")
	}
	client := NewClient(options)
	if err := ValidateClient(ctx, client); err != nil {
		_ = client.Close()
		return nil, err
	}
	return client, nil
}

// Open creates a broker with an owned, validated Redis connection.
func Open(ctx context.Context, options Options) (*Broker, error) {
	client, err := Connect(ctx, options)
	if err != nil {
		return nil, err
	}
	options.Client = client
	broker := New(options)
	broker.ownedClient = true
	return broker, nil
}

// ValidateClient verifies that client points at a writable standalone Redis
// primary. It is intentionally stricter than PING so an accidental Sentinel,
// Cluster, or replica endpoint cannot become a partially working deployment.
func ValidateClient(ctx context.Context, client *goredis.Client) error {
	if client == nil {
		return fmt.Errorf("validate Redis connection: nil client")
	}
	if err := client.Ping(ctx).Err(); err != nil {
		return fmt.Errorf("ping Redis: %w", err)
	}
	server, err := client.Info(ctx, "server").Result()
	if err != nil {
		return fmt.Errorf("inspect Redis server mode: %w", err)
	}
	if mode := infoValue(server, "redis_mode"); mode != "standalone" {
		return fmt.Errorf("unsupported Redis mode %q: TaskForge supports direct standalone Redis only", mode)
	}
	replication, err := client.Info(ctx, "replication").Result()
	if err != nil {
		return fmt.Errorf("inspect Redis replication role: %w", err)
	}
	if role := infoValue(replication, "role"); role != "master" {
		return fmt.Errorf("redis endpoint role %q is not writable: connect TaskForge to the standalone primary", role)
	}
	return nil
}

func infoValue(info, key string) string {
	prefix := key + ":"
	for _, line := range strings.Split(info, "\n") {
		line = strings.TrimSpace(line)
		if strings.HasPrefix(line, prefix) {
			return strings.TrimSpace(strings.TrimPrefix(line, prefix))
		}
	}
	return ""
}
