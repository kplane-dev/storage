package postgres

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

// defaultTestDSN is the local Postgres the tests connect to. Override with
// POSTGRES_TEST_DSN for a different server.
const defaultTestDSN = "postgres://postgres@localhost:5432/postgres?sslmode=disable"

func testDSN() string {
	if v := os.Getenv("POSTGRES_TEST_DSN"); v != "" {
		return v
	}
	return defaultTestDSN
}

// skipIfNoPostgres skips the test when no Postgres is reachable at testDSN().
func skipIfNoPostgres(t *testing.T) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	conn, err := pgx.Connect(ctx, testDSN())
	if err != nil {
		t.Skipf("Postgres not reachable at %s: %v", testDSN(), err)
	}
	_ = conn.Close(ctx)
}

// createTestDatabase creates a fresh, uniquely-named database. CREATE
// DATABASE cannot run inside a transaction, so it goes through a plain
// autocommit Exec on a dedicated admin connection.
func createTestDatabase(ctx context.Context, name string) error {
	conn, err := pgx.Connect(ctx, testDSN())
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close(context.Background()) }()
	_, err = conn.Exec(ctx, fmt.Sprintf(`CREATE DATABASE %q`, name))
	return err
}

func dropTestDatabase(ctx context.Context, name string) error {
	conn, err := pgx.Connect(ctx, testDSN())
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close(context.Background()) }()
	_, err = conn.Exec(ctx, fmt.Sprintf(`DROP DATABASE IF EXISTS %q WITH (FORCE)`, name))
	return err
}

// freshConfig provisions a new database and returns a Config pointing at it,
// dropping the database at test teardown.
func freshConfig(t *testing.T) Config {
	t.Helper()
	skipIfNoPostgres(t)
	ctx := context.Background()
	dbName := fmt.Sprintf("kv_test_%d", time.Now().UnixNano())
	if err := createTestDatabase(ctx, dbName); err != nil {
		t.Fatalf("createTestDatabase: %v", err)
	}
	t.Cleanup(func() { _ = dropTestDatabase(context.Background(), dbName) })
	return Config{DSN: testDSN(), Database: dbName}
}
