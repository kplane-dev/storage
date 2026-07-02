package cockroach

import (
	"context"
	"runtime"
	"testing"
	"time"
)

func TestConfig_BuildPoolConfig(t *testing.T) {
	tests := []struct {
		name            string
		cfg             Config
		wantErr         bool
		wantMinEqMax    bool
		wantMaxConns    int32
		wantLifetime    time.Duration
		wantJitter      time.Duration
		wantIdleTime    time.Duration
		wantHealthCheck time.Duration
	}{
		{
			name:            "defaults applied",
			cfg:             Config{DSN: "postgres://user@example:26257/db"},
			wantMinEqMax:    true,
			wantMaxConns:    int32(runtime.GOMAXPROCS(0) * 4),
			wantLifetime:    5 * time.Minute,
			wantJitter:      30 * time.Second,
			wantIdleTime:    5 * time.Minute,
			wantHealthCheck: 30 * time.Second,
		},
		{
			name: "explicit overrides",
			cfg: Config{
				DSN:                   "postgres://user@example:26257/db",
				MaxConns:              64,
				MaxConnLifetime:       time.Minute,
				MaxConnLifetimeJitter: 6 * time.Second,
				MaxConnIdleTime:       2 * time.Minute,
				HealthCheckPeriod:     15 * time.Second,
			},
			wantMinEqMax:    true,
			wantMaxConns:    64,
			wantLifetime:    time.Minute,
			wantJitter:      6 * time.Second,
			wantIdleTime:    2 * time.Minute,
			wantHealthCheck: 15 * time.Second,
		},
		{
			name:    "empty DSN rejected",
			cfg:     Config{},
			wantErr: true,
		},
		{
			name:    "malformed DSN rejected",
			cfg:     Config{DSN: "not a url ://"},
			wantErr: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := tc.cfg.buildPoolConfig()
			if tc.wantErr {
				if err == nil {
					t.Fatalf("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got.MaxConns != tc.wantMaxConns {
				t.Errorf("MaxConns = %d, want %d", got.MaxConns, tc.wantMaxConns)
			}
			if tc.wantMinEqMax && got.MinConns != got.MaxConns {
				t.Errorf("MinConns = %d, want %d (equal to MaxConns)", got.MinConns, got.MaxConns)
			}
			if got.MaxConnLifetime != tc.wantLifetime {
				t.Errorf("MaxConnLifetime = %v, want %v", got.MaxConnLifetime, tc.wantLifetime)
			}
			if got.MaxConnLifetimeJitter != tc.wantJitter {
				t.Errorf("MaxConnLifetimeJitter = %v, want %v", got.MaxConnLifetimeJitter, tc.wantJitter)
			}
			if got.MaxConnIdleTime != tc.wantIdleTime {
				t.Errorf("MaxConnIdleTime = %v, want %v", got.MaxConnIdleTime, tc.wantIdleTime)
			}
			if got.HealthCheckPeriod != tc.wantHealthCheck {
				t.Errorf("HealthCheckPeriod = %v, want %v", got.HealthCheckPeriod, tc.wantHealthCheck)
			}
		})
	}
}

func TestConfig_NewPool_ConnectsToLiveNode(t *testing.T) {
	skipIfNoCockroach(t)
	cfg := Config{DSN: testDSN()}
	pool, err := cfg.NewPool(context.Background())
	if err != nil {
		t.Fatalf("NewPool: %v", err)
	}
	defer pool.Close()

	var one int
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := pool.QueryRow(ctx, "SELECT 1").Scan(&one); err != nil {
		t.Fatalf("SELECT 1: %v", err)
	}
	if one != 1 {
		t.Errorf("got %d, want 1", one)
	}
}

func TestConfig_ConnConfig_SharesDSNParse(t *testing.T) {
	cfg := Config{
		DSN:      "postgres://user@example:26257/original",
		Database: "override",
	}
	got, err := cfg.ConnConfig()
	if err != nil {
		t.Fatalf("ConnConfig: %v", err)
	}
	if got.Database != "override" {
		t.Errorf("Database = %q, want %q", got.Database, "override")
	}
}
