package spanner

import (
	"context"
	"fmt"
	"time"

	"cloud.google.com/go/spanner"
	database "cloud.google.com/go/spanner/admin/database/apiv1"
	databasepb "cloud.google.com/go/spanner/admin/database/apiv1/databasepb"
	instance "cloud.google.com/go/spanner/admin/instance/apiv1"
	instancepb "cloud.google.com/go/spanner/admin/instance/apiv1/instancepb"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"k8s.io/klog/v2"
)

// SpannerConfig holds configuration for connecting to a Spanner instance.
type SpannerConfig struct {
	// Project is the GCP project ID.
	Project string

	// Instance is the Spanner instance ID.
	Instance string

	// Database is the Spanner database name.
	Database string

	// EmulatorHost overrides the Spanner endpoint for local development.
	// When set, TLS is disabled and authentication is skipped.
	// Format: "host:port" (e.g. "localhost:9010").
	EmulatorHost string
}

// DatabasePath returns the fully qualified Spanner database path.
func (c SpannerConfig) DatabasePath() string {
	return fmt.Sprintf("projects/%s/instances/%s/databases/%s", c.Project, c.Instance, c.Database)
}

// InstancePath returns the fully qualified Spanner instance path.
func (c SpannerConfig) InstancePath() string {
	return fmt.Sprintf("projects/%s/instances/%s", c.Project, c.Instance)
}

// NewClient creates a Spanner client from the config.
func (c SpannerConfig) NewClient(ctx context.Context) (*spanner.Client, error) {
	var opts []option.ClientOption
	if c.EmulatorHost != "" {
		opts = append(opts,
			option.WithEndpoint(c.EmulatorHost),
			option.WithGRPCDialOption(grpc.WithTransportCredentials(insecure.NewCredentials())),
			option.WithoutAuthentication(),
		)
	}
	return spanner.NewClient(ctx, c.DatabasePath(), opts...)
}

// schemaDDL returns the DDL statements for the kv table and change stream.
var schemaDDL = []string{
	`CREATE TABLE kv (
    key        STRING(MAX) NOT NULL,
    value      BYTES(MAX) NOT NULL,
    mod_ts     TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp = true),
    create_ts  TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp = true),
    lease_ttl  INT64,
) PRIMARY KEY (key)`,
	`CREATE CHANGE STREAM kv_changes FOR kv
  OPTIONS (value_capture_type = 'OLD_AND_NEW_VALUES')`,
}

// EnsureInstance creates the Spanner instance if it doesn't already exist.
// Intended for development / testing with the Spanner emulator.
func EnsureInstance(ctx context.Context, cfg SpannerConfig) error {
	var opts []option.ClientOption
	if cfg.EmulatorHost != "" {
		opts = append(opts,
			option.WithEndpoint(cfg.EmulatorHost),
			option.WithGRPCDialOption(grpc.WithTransportCredentials(insecure.NewCredentials())),
			option.WithoutAuthentication(),
		)
	}
	adminClient, err := instance.NewInstanceAdminClient(ctx, opts...)
	if err != nil {
		return fmt.Errorf("creating instance admin client: %w", err)
	}
	defer adminClient.Close()

	op, err := adminClient.CreateInstance(ctx, &instancepb.CreateInstanceRequest{
		Parent:     fmt.Sprintf("projects/%s", cfg.Project),
		InstanceId: cfg.Instance,
		Instance: &instancepb.Instance{
			Config:      "emulator-config",
			DisplayName: cfg.Instance,
			NodeCount:   1,
		},
	})
	if err != nil {
		// Instance may already exist — not an error.
		return nil
	}
	_, _ = op.Wait(ctx)
	return nil
}

// EnsureSchema creates the database and applies the KV schema if it doesn't exist.
// Intended for development / testing with the Spanner emulator.
func EnsureSchema(ctx context.Context, cfg SpannerConfig) error {
	var opts []option.ClientOption
	if cfg.EmulatorHost != "" {
		opts = append(opts,
			option.WithEndpoint(cfg.EmulatorHost),
			option.WithGRPCDialOption(grpc.WithTransportCredentials(insecure.NewCredentials())),
			option.WithoutAuthentication(),
		)
	}

	adminClient, err := database.NewDatabaseAdminClient(ctx, opts...)
	if err != nil {
		return fmt.Errorf("creating database admin client: %w", err)
	}
	defer adminClient.Close()

	// Create the database with schema in one shot.
	op, err := adminClient.CreateDatabase(ctx, &databasepb.CreateDatabaseRequest{
		Parent:          cfg.InstancePath(),
		CreateStatement: fmt.Sprintf("CREATE DATABASE `%s`", cfg.Database),
		ExtraStatements: schemaDDL,
	})
	if err != nil {
		return fmt.Errorf("creating database: %w", err)
	}

	if _, err := op.Wait(ctx); err != nil {
		return fmt.Errorf("waiting for database creation: %w", err)
	}

	klog.V(2).InfoS("Spanner database created with schema", "database", cfg.DatabasePath())
	return nil
}

// DropDatabase drops the specified Spanner database.
// Intended for test cleanup to avoid hitting emulator database limits.
func DropDatabase(ctx context.Context, cfg SpannerConfig) error {
	var opts []option.ClientOption
	if cfg.EmulatorHost != "" {
		opts = append(opts,
			option.WithEndpoint(cfg.EmulatorHost),
			option.WithGRPCDialOption(grpc.WithTransportCredentials(insecure.NewCredentials())),
			option.WithoutAuthentication(),
		)
	}

	adminClient, err := database.NewDatabaseAdminClient(ctx, opts...)
	if err != nil {
		return fmt.Errorf("creating database admin client: %w", err)
	}
	defer adminClient.Close()

	return adminClient.DropDatabase(ctx, &databasepb.DropDatabaseRequest{
		Database: cfg.DatabasePath(),
	})
}

// revisionFromCommitTimestamp converts a Spanner commit timestamp to a
// resource version. We use UnixNano which gives us ~292 years of headroom
// from epoch and natural monotonicity via TrueTime.
func revisionFromCommitTimestamp(ts time.Time) uint64 {
	return uint64(ts.UnixNano())
}

// timestampFromRevision converts a resource version back to a time.Time
// for use in Spanner stale reads.
func timestampFromRevision(rv int64) time.Time {
	return time.Unix(0, rv)
}
