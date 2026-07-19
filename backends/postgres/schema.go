package postgres

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
)

// notifyChannel is the LISTEN/NOTIFY channel the watch feed subscribes to.
// The kv_notify trigger fires a payload of the new row's id on every INSERT;
// the id is only a low-latency wakeup hint — the feed's poll of the log is
// the authoritative source, so a missed NOTIFY never loses an event.
const notifyChannel = "kv_changes"

// initSentinelName is a reserved row inserted once at schema creation so the
// log is never empty. Its name sorts after every "/registry/..." key, so no
// prefix scan, Stats count, or watch ever observes it — it exists only so
// GetCurrentResourceVersion has a non-zero id to return on a fresh database
// (the cacher rejects a resource version of 0).
const initSentinelName = "\x7f__kplane_init__"

// schemaDDL is the append-only MVCC log plus its indexes. Every mutation
// INSERTs a row; the BIGSERIAL id is the resource version. `created` marks
// the row that first materialized a key (drives watch ADDED), `deleted`
// marks a tombstone (drives watch DELETED), and prev_value carries the
// pre-image so watches can emit prevValue without a second lookup.
// created_txid records the writing transaction so the feed can compute a
// gap-safe watermark (see rv.go / feed.go).
var schemaDDL = []string{
	`CREATE TABLE IF NOT EXISTS kv (
		id           BIGSERIAL   PRIMARY KEY,
		-- COLLATE "C" forces bytewise comparison. Keys are byte strings, and
		-- the prefix range scans (name >= start AND name < prefixEnd) plus
		-- prefixEnd's byte arithmetic all assume byte ordering; a locale
		-- collation reorders punctuation like '/' and silently breaks every
		-- prefix list. The (name, id DESC) index inherits this collation.
		name         TEXT COLLATE "C" NOT NULL,
		value        BYTEA,
		prev_value   BYTEA,
		created      BOOLEAN     NOT NULL DEFAULT false,
		deleted      BOOLEAN     NOT NULL DEFAULT false,
		lease_ttl    BIGINT,
		expire_at    TIMESTAMPTZ,
		created_txid BIGINT      NOT NULL DEFAULT txid_current()
	)`,
	// Serves both the "newest row per name" lookups (Get/Delete/Update) and
	// the DISTINCT ON (name) ... ORDER BY name, id DESC snapshot reads in
	// GetList.
	`CREATE INDEX IF NOT EXISTS kv_name_id ON kv (name, id DESC)`,
	// Bounds the TTL scanner to a small indexed range: only live rows that
	// carry an expiry.
	`CREATE INDEX IF NOT EXISTS kv_expire ON kv (expire_at)
		WHERE expire_at IS NOT NULL AND deleted = false`,
}

// notifyDDL installs the AFTER INSERT trigger that wakes the watch feed.
// Split from schemaDDL because CREATE TRIGGER has no IF NOT EXISTS before
// PostgreSQL 14; we DROP-then-CREATE for idempotency on every start.
var notifyDDL = []string{
	`CREATE OR REPLACE FUNCTION kv_notify() RETURNS trigger AS $$
	 BEGIN
	   PERFORM pg_notify('` + notifyChannel + `', NEW.id::text);
	   RETURN NEW;
	 END;
	 $$ LANGUAGE plpgsql`,
	`DROP TRIGGER IF EXISTS kv_notify_trigger ON kv`,
	`CREATE TRIGGER kv_notify_trigger AFTER INSERT ON kv
		FOR EACH ROW EXECUTE FUNCTION kv_notify()`,
}

// EnsureSchema applies the log table, indexes, NOTIFY trigger, and the init
// sentinel. Idempotent: safe to call on every process start.
func EnsureSchema(ctx context.Context, cfg Config) error {
	connCfg, err := cfg.ConnConfig()
	if err != nil {
		return err
	}
	conn, err := pgx.ConnectConfig(ctx, connCfg)
	if err != nil {
		return fmt.Errorf("postgres: connect for schema: %w", err)
	}
	defer func() { _ = conn.Close(context.Background()) }()

	for _, stmt := range schemaDDL {
		if _, err := conn.Exec(ctx, stmt); err != nil {
			return fmt.Errorf("postgres: schema ddl: %w", err)
		}
	}
	for _, stmt := range notifyDDL {
		if _, err := conn.Exec(ctx, stmt); err != nil {
			return fmt.Errorf("postgres: notify ddl: %w", err)
		}
	}
	// Seed the sentinel once so the log — and thus the resource-version
	// space — is never empty. WHERE NOT EXISTS keeps this a no-op on every
	// call after the first.
	if _, err := conn.Exec(ctx, `
		INSERT INTO kv (name, created, deleted, value)
		SELECT $1, false, true, NULL
		WHERE NOT EXISTS (SELECT 1 FROM kv)`, initSentinelName); err != nil {
		return fmt.Errorf("postgres: seed sentinel: %w", err)
	}
	return nil
}
