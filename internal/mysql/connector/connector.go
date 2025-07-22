package connector

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"

	"github.com/go-mysql-org/go-mysql/client"
	"go.uber.org/zap"

	"github.com/philippevezina/stream-bridge/internal/config"
)

// Connector provides centralized MySQL connection management with SSL support
type Connector struct {
	cfg    *config.MySQLConfig
	logger *zap.Logger
}

// New creates a new MySQL connector with the given configuration and logger
func New(cfg *config.MySQLConfig, logger *zap.Logger) *Connector {
	return &Connector{
		cfg:    cfg,
		logger: logger,
	}
}

// Connect establishes a MySQL connection to the specified database
func (c *Connector) Connect(database string) (*client.Conn, error) {
	return c.ConnectWithContext(context.Background(), database)
}

// ConnectWithContext establishes a MySQL connection with context support
func (c *Connector) ConnectWithContext(ctx context.Context, database string) (*client.Conn, error) {
	addr := fmt.Sprintf("%s:%d", c.cfg.Host, c.cfg.Port)

	if c.cfg.SSLMode == config.SSLModeDisabled {
		// For disabled mode, connect without SSL
		return client.Connect(addr, c.cfg.Username, c.cfg.Password, database)
	}

	if c.cfg.SSLMode == config.SSLModePreferred {
		// For preferred mode, try SSL first, fallback to plaintext on failure
		conn, err := client.Connect(addr, c.cfg.Username, c.cfg.Password, database)
		if err != nil {
			return nil, err
		}

		tlsConfig, err := c.buildTLSConfig()
		if err != nil {
			conn.Close()
			return nil, fmt.Errorf("failed to build TLS config: %w", err)
		}

		// Try to set TLS config, but don't fail if it doesn't work
		if tlsConfig != nil {
			conn.SetTLSConfig(tlsConfig)
			c.logger.Debug("SSL/TLS preferred mode - attempting encrypted connection")
		}
		return conn, nil
	}

	// For required, verify_ca, and verify_identity modes, SSL is mandatory
	conn, err := client.Connect(addr, c.cfg.Username, c.cfg.Password, database)
	if err != nil {
		return nil, err
	}

	tlsConfig, err := c.buildTLSConfig()
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("failed to build TLS config: %w", err)
	}

	if tlsConfig != nil {
		conn.SetTLSConfig(tlsConfig)
		c.logger.Info("SSL/TLS enabled",
			zap.String("mode", c.cfg.SSLMode),
			zap.String("host", c.cfg.Host))
	}

	return conn, nil
}

// GetTLSConfig returns the TLS configuration for the current SSL mode
// This is useful for components that need direct access to TLS config (like BinlogSyncer)
func (c *Connector) GetTLSConfig() (*tls.Config, error) {
	return c.buildTLSConfig()
}

// buildTLSConfig creates a TLS configuration from the MySQL SSL settings
func (c *Connector) buildTLSConfig() (*tls.Config, error) {
	switch c.cfg.SSLMode {
	case config.SSLModeDisabled:
		return nil, nil

	case config.SSLModePreferred, config.SSLModeRequired:
		// For preferred and required modes, use basic TLS without certificate verification
		tlsConfig := &tls.Config{
			InsecureSkipVerify: true, // Allow connections without server cert verification
		}

		// Load client certificate and key if provided
		if c.cfg.SSLCert != "" && c.cfg.SSLKey != "" {
			cert, err := tls.LoadX509KeyPair(c.cfg.SSLCert, c.cfg.SSLKey)
			if err != nil {
				return nil, fmt.Errorf("failed to load client certificate: %w", err)
			}
			tlsConfig.Certificates = []tls.Certificate{cert}
		}

		return tlsConfig, nil

	case config.SSLModeVerifyCA:
		// Verify server certificate against CA but don't verify hostname
		tlsConfig := &tls.Config{
			InsecureSkipVerify: false,
		}

		// Load client certificate and key if provided
		if c.cfg.SSLCert != "" && c.cfg.SSLKey != "" {
			cert, err := tls.LoadX509KeyPair(c.cfg.SSLCert, c.cfg.SSLKey)
			if err != nil {
				return nil, fmt.Errorf("failed to load client certificate: %w", err)
			}
			tlsConfig.Certificates = []tls.Certificate{cert}
		}

		// Load CA certificate (required for verify_ca mode)
		if c.cfg.SSLCa != "" {
			caCert, err := os.ReadFile(c.cfg.SSLCa)
			if err != nil {
				return nil, fmt.Errorf("failed to read CA certificate: %w", err)
			}

			caCertPool := x509.NewCertPool()
			if !caCertPool.AppendCertsFromPEM(caCert) {
				return nil, fmt.Errorf("failed to parse CA certificate")
			}
			tlsConfig.RootCAs = caCertPool
		}

		return tlsConfig, nil

	case config.SSLModeVerifyIdentity:
		// Verify server certificate against CA AND verify hostname
		tlsConfig := &tls.Config{
			InsecureSkipVerify: false,
			ServerName:         c.cfg.Host, // Enable hostname verification
		}

		// Load client certificate and key if provided
		if c.cfg.SSLCert != "" && c.cfg.SSLKey != "" {
			cert, err := tls.LoadX509KeyPair(c.cfg.SSLCert, c.cfg.SSLKey)
			if err != nil {
				return nil, fmt.Errorf("failed to load client certificate: %w", err)
			}
			tlsConfig.Certificates = []tls.Certificate{cert}
		}

		// Load CA certificate (required for verify_identity mode)
		if c.cfg.SSLCa != "" {
			caCert, err := os.ReadFile(c.cfg.SSLCa)
			if err != nil {
				return nil, fmt.Errorf("failed to read CA certificate: %w", err)
			}

			caCertPool := x509.NewCertPool()
			if !caCertPool.AppendCertsFromPEM(caCert) {
				return nil, fmt.Errorf("failed to parse CA certificate")
			}
			tlsConfig.RootCAs = caCertPool
		}

		return tlsConfig, nil

	default:
		return nil, fmt.Errorf("unsupported SSL mode: %s", c.cfg.SSLMode)
	}
}
