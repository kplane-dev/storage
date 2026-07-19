package postgres

import (
	"context"
	"fmt"
	"runtime"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Config configures a PostgreSQL backend. DSN is a libpq/pgx connection
// string (or URL); pgx will honor multi-host DSNs by trying hosts in order.
type Config struct {
	DSN string

	// Database, when set, overrides the database named in the DSN after the
	// connection config is parsed. Leave empty to use the DSN's database.
	Database string

	// MaxConns caps the pgxpool size. Zero picks 4 * GOMAXPROCS.
	MaxConns int32

	// MaxConnLifetime bounds how long any pool connection stays open.
	// Finite values are required so rolling restarts don't strand conns.
	MaxConnLifetime time.Duration

	// MaxConnLifetimeJitter randomizes lifetimes so a pool doesn't rotate
	// its connections in lockstep. Recommended ~10% of MaxConnLifetime.
	MaxConnLifetimeJitter time.Duration

	// MaxConnIdleTime closes idle connections after this interval.
	MaxConnIdleTime time.Duration

	// HealthCheckPeriod controls how often the pool prunes broken conns.
	HealthCheckPeriod time.Duration
}

// buildPoolConfig returns a pgxpool.Config with our defaults applied.
func (c Config) buildPoolConfig() (*pgxpool.Config, error) {
	if c.DSN == "" {
		return nil, fmt.Errorf("postgres: DSN is required")
	}
	cfg, err := pgxpool.ParseConfig(c.DSN)
	if err != nil {
		return nil, fmt.Errorf("postgres: parse DSN: %w", err)
	}
	cfg.MaxConns = c.MaxConns
	if cfg.MaxConns <= 0 {
		cfg.MaxConns = int32(runtime.GOMAXPROCS(0) * 4)
	}
	cfg.MaxConnLifetime = c.MaxConnLifetime
	if cfg.MaxConnLifetime <= 0 {
		cfg.MaxConnLifetime = 5 * time.Minute
	}
	cfg.MaxConnLifetimeJitter = c.MaxConnLifetimeJitter
	if cfg.MaxConnLifetimeJitter <= 0 {
		cfg.MaxConnLifetimeJitter = 30 * time.Second
	}
	cfg.MaxConnIdleTime = c.MaxConnIdleTime
	if cfg.MaxConnIdleTime <= 0 {
		cfg.MaxConnIdleTime = 5 * time.Minute
	}
	cfg.HealthCheckPeriod = c.HealthCheckPeriod
	if cfg.HealthCheckPeriod <= 0 {
		cfg.HealthCheckPeriod = 30 * time.Second
	}
	if c.Database != "" {
		cfg.ConnConfig.Database = c.Database
	}
	return cfg, nil
}

// NewPool constructs a pgxpool.Pool from the config. Callers own the pool
// and must call Close when done.
func (c Config) NewPool(ctx context.Context) (*pgxpool.Pool, error) {
	poolCfg, err := c.buildPoolConfig()
	if err != nil {
		return nil, err
	}
	return pgxpool.NewWithConfig(ctx, poolCfg)
}

// ConnConfig returns the underlying single-connection config. The watch
// feed uses this for a dedicated LISTEN connection that lives outside the
// pool — a connection blocked in WaitForNotification can't be shared.
func (c Config) ConnConfig() (*pgx.ConnConfig, error) {
	poolCfg, err := c.buildPoolConfig()
	if err != nil {
		return nil, err
	}
	return poolCfg.ConnConfig, nil
}
