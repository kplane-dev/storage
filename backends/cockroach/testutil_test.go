package cockroach

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// defaultTestDSN is the local single-node CockroachDB the tests connect to.
// Override with COCKROACH_TEST_DSN for a different cluster.
const defaultTestDSN = "postgres://root@localhost:26257/defaultdb?sslmode=disable"

func testDSN() string {
	if v := os.Getenv("COCKROACH_TEST_DSN"); v != "" {
		return v
	}
	return defaultTestDSN
}

// skipIfNoCockroach skips the test when no CockroachDB is reachable at
// testDSN(). Callers that require a live cluster invoke this first.
func skipIfNoCockroach(t *testing.T) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	conn, err := pgx.Connect(ctx, testDSN())
	if err != nil {
		t.Skipf("CockroachDB not reachable at %s: %v", testDSN(), err)
	}
	_ = conn.Close(ctx)
}

// setupTestPool returns a pgxpool.Pool connected to a fresh, uniquely-named
// database. The database is dropped at teardown.
func setupTestPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	skipIfNoCockroach(t)

	dbName := fmt.Sprintf("kv_test_%d", time.Now().UnixNano())
	ctx := context.Background()

	admin, err := pgx.Connect(ctx, testDSN())
	if err != nil {
		t.Fatalf("connect admin: %v", err)
	}
	if _, err := admin.Exec(ctx, fmt.Sprintf(`CREATE DATABASE %q`, dbName)); err != nil {
		_ = admin.Close(ctx)
		t.Fatalf("create db: %v", err)
	}
	_ = admin.Close(ctx)

	cfg := Config{
		DSN:      testDSN(),
		Database: dbName,
	}
	pool, err := cfg.NewPool(ctx)
	if err != nil {
		t.Fatalf("new pool: %v", err)
	}
	t.Cleanup(func() {
		pool.Close()
		drop, err := pgx.Connect(context.Background(), testDSN())
		if err != nil {
			return
		}
		defer drop.Close(context.Background())
		_, _ = drop.Exec(context.Background(), fmt.Sprintf(`DROP DATABASE IF EXISTS %q CASCADE`, dbName))
	})
	return pool
}
