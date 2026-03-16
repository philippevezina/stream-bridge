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
		return client.Connect(addr, c.cfg.Username, c.cfg.Password, database)
	}

	// Build TLS config before connecting so it's available during the handshake.
	// The go-mysql library uses tlsConfig during the auth handshake (before Connect returns),
	// so it must be passed as an option, not set after the connection is established.
	tlsConfig, err := c.buildTLSConfig()
	if err != nil {
		return nil, fmt.Errorf("failed to build TLS config: %w", err)
	}

	var options []client.Option
	if tlsConfig != nil {
		options = append(options, func(conn *client.Conn) error {
			conn.SetTLSConfig(tlsConfig)
			return nil
		})
	}

	if c.cfg.SSLMode == config.SSLModePreferred {
		// For preferred mode, try with TLS first, fallback to plaintext
		conn, err := client.Connect(addr, c.cfg.Username, c.cfg.Password, database, options...)
		if err != nil {
			c.logger.Debug("SSL/TLS preferred mode - TLS connection failed, falling back to plaintext",
				zap.Error(err))
			return client.Connect(addr, c.cfg.Username, c.cfg.Password, database)
		}
		c.logger.Debug("SSL/TLS preferred mode - connected with encryption")
		return conn, nil
	}

	// For required, verify_ca, and verify_identity modes, SSL is mandatory
	conn, err := client.Connect(addr, c.cfg.Username, c.cfg.Password, database, options...)
	if err != nil {
		return nil, err
	}

	c.logger.Info("SSL/TLS enabled",
		zap.String("mode", c.cfg.SSLMode),
		zap.String("host", c.cfg.Host))

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
