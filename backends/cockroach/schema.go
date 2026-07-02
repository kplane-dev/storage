package cockroach

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
)

// schemaDDL is the set of statements EnsureSchema applies. The kv table's
// expire_at column drives TTL: reads filter it (see the store), the built-in
// TTL job runs it as a backup sweeper, and a per-process scanner produces
// sub-minute deletions. crdb_internal_mvcc_timestamp gives us per-row
// commit timestamps without an explicit mod_ts column.
var schemaDDL = []string{
	`CREATE TABLE IF NOT EXISTS kv (
		key       STRING NOT NULL PRIMARY KEY,
		value     BYTES NOT NULL,
		lease_ttl INT8,
		expire_at TIMESTAMPTZ,
		INDEX kv_by_expire_at (expire_at) STORING (value) WHERE expire_at IS NOT NULL
	) WITH (
		ttl_expiration_expression = 'expire_at',
		ttl_job_cron              = '* * * * *'
	)`,
}

// clusterSettings are one-per-cluster tunings EnsureSchema applies at
// startup. The rangefeed enable is required by CREATE CHANGEFEED; without
// it every changefeed opens with error 55C00.
var clusterSettings = []string{
	`SET CLUSTER SETTING kv.rangefeed.enabled = true`,
}

// EnsureSchema applies the kv table and cluster settings. Idempotent: safe
// to call on every process start. CREATE TABLE IF NOT EXISTS + SET CLUSTER
// SETTING both no-op on a schema already at target.
func EnsureSchema(ctx context.Context, cfg Config) error {
	conn, err := pgx.Connect(ctx, cfg.DSN)
	if err != nil {
		return fmt.Errorf("cockroach: connect for schema: %w", err)
	}
	defer conn.Close(context.Background())

	if cfg.Database != "" {
		if _, err := conn.Exec(ctx, fmt.Sprintf("SET DATABASE = %q", cfg.Database)); err != nil {
			return fmt.Errorf("cockroach: set database: %w", err)
		}
	}
	for _, stmt := range clusterSettings {
		if _, err := conn.Exec(ctx, stmt); err != nil {
			return fmt.Errorf("cockroach: cluster setting: %w", err)
		}
	}
	for _, stmt := range schemaDDL {
		if _, err := conn.Exec(ctx, stmt); err != nil {
			return fmt.Errorf("cockroach: schema ddl: %w", err)
		}
	}
	return nil
}

// ErrTableMissing wraps a pgx error that indicates the kv table doesn't
// exist. Handy for tests and callers that want to gate operations on
// schema presence.
var ErrTableMissing = errors.New("cockroach: kv table missing (run EnsureSchema)")
