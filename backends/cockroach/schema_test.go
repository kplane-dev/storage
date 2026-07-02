package cockroach

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

func TestEnsureSchema_Idempotent(t *testing.T) {
	skipIfNoCockroach(t)
	ctx := context.Background()

	dbName := fmt.Sprintf("kv_schema_%d", time.Now().UnixNano())
	admin, err := pgx.Connect(ctx, testDSN())
	if err != nil {
		t.Fatalf("connect admin: %v", err)
	}
	if _, err := admin.Exec(ctx, fmt.Sprintf(`CREATE DATABASE %q`, dbName)); err != nil {
		t.Fatalf("create db: %v", err)
	}
	t.Cleanup(func() {
		_, _ = admin.Exec(context.Background(), fmt.Sprintf(`DROP DATABASE IF EXISTS %q CASCADE`, dbName))
		_ = admin.Close(context.Background())
	})

	cfg := Config{DSN: testDSN(), Database: dbName}

	for i := 0; i < 3; i++ {
		if err := EnsureSchema(ctx, cfg); err != nil {
			t.Fatalf("EnsureSchema iteration %d: %v", i, err)
		}
	}

	t.Run("kv table exists with expected columns", func(t *testing.T) {
		var count int
		if err := admin.QueryRow(ctx, fmt.Sprintf(`
			SELECT count(*) FROM %q.information_schema.columns
			WHERE table_name = 'kv' AND column_name IN ('key', 'value', 'lease_ttl', 'expire_at')`, dbName)).Scan(&count); err != nil {
			t.Fatalf("query columns: %v", err)
		}
		if count != 4 {
			t.Errorf("expected 4 columns, found %d", count)
		}
	})

	t.Run("partial storing index exists", func(t *testing.T) {
		var count int
		if err := admin.QueryRow(ctx, fmt.Sprintf(`
			SELECT count(*) FROM %q.information_schema.statistics
			WHERE table_name = 'kv' AND index_name = 'kv_by_expire_at'`, dbName)).Scan(&count); err != nil {
			t.Fatalf("query index: %v", err)
		}
		if count == 0 {
			t.Errorf("kv_by_expire_at index not found")
		}
	})

	t.Run("row-level TTL configured", func(t *testing.T) {
		var storageParams string
		if err := admin.QueryRow(ctx, fmt.Sprintf(`
			SELECT COALESCE(create_statement, '') FROM [SHOW CREATE TABLE %q.kv]`, dbName)).Scan(&storageParams); err != nil {
			t.Fatalf("show create table: %v", err)
		}
		if !strings.Contains(storageParams, "ttl_expiration_expression") {
			t.Errorf("ttl_expiration_expression not present in CREATE TABLE:\n%s", storageParams)
		}
	})

	t.Run("rangefeed cluster setting is on", func(t *testing.T) {
		var enabled bool
		if err := admin.QueryRow(ctx, `SHOW CLUSTER SETTING kv.rangefeed.enabled`).Scan(&enabled); err != nil {
			t.Fatalf("show cluster setting: %v", err)
		}
		if !enabled {
			t.Errorf("kv.rangefeed.enabled = false")
		}
	})
}

func TestEnsureSchema_MissingDSN(t *testing.T) {
	if err := EnsureSchema(context.Background(), Config{}); err == nil {
		t.Fatal("expected error for empty DSN")
	}
}
