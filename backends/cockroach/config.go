package cockroach

import (
	"context"
	"fmt"
	"runtime"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Config configures a CockroachDB backend. DSN is a PostgreSQL connection
// string (multi-host is supported: pgx will round-robin across hosts).
type Config struct {
	DSN string

	// Database is applied after connection if the DSN doesn't already pin
	// one; leave empty to use the DSN's database.
	Database string

	// MaxConns caps the pgxpool size. Zero picks a default based on GOMAXPROCS.
	MaxConns int32

	// MaxConnLifetime bounds how long any pool connection stays open.
	// Finite values are required so rolling node restarts don't strand conns.
	MaxConnLifetime time.Duration

	// MaxConnLifetimeJitter randomizes lifetimes so pools don't rotate
	// in lockstep. Recommended: ~10% of MaxConnLifetime.
	MaxConnLifetimeJitter time.Duration

	// MaxConnIdleTime closes idle connections after this interval.
	MaxConnIdleTime time.Duration

	// HealthCheckPeriod controls how often the pool prunes broken conns.
	HealthCheckPeriod time.Duration
}

// buildPoolConfig returns a pgxpool.Config with our defaults applied.
func (c Config) buildPoolConfig() (*pgxpool.Config, error) {
	if c.DSN == "" {
		return nil, fmt.Errorf("cockroach: DSN is required")
	}
	cfg, err := pgxpool.ParseConfig(c.DSN)
	if err != nil {
		return nil, fmt.Errorf("cockroach: parse DSN: %w", err)
	}
	cfg.MaxConns = c.MaxConns
	if cfg.MaxConns <= 0 {
		cfg.MaxConns = int32(runtime.GOMAXPROCS(0) * 4)
	}
	cfg.MinConns = cfg.MaxConns
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

// ConnConfig returns the underlying single-connection config, used by the
// changefeed subscription which requires a dedicated conn outside the pool.
func (c Config) ConnConfig() (*pgx.ConnConfig, error) {
	poolCfg, err := c.buildPoolConfig()
	if err != nil {
		return nil, err
	}
	return poolCfg.ConnConfig, nil
}
