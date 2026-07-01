package cockroach

import (
	"context"
	"fmt"
	"time"

	"github.com/spf13/pflag"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apiserver/pkg/storage"
	"k8s.io/apiserver/pkg/storage/storagebackend"
	"k8s.io/apiserver/pkg/storage/storagebackend/factory"

	"github.com/kplane-dev/storage/registry"
)

// Options is the CockroachDB backend's flag block. Implements
// registry.Backend so an apiserver aggregator can register it via
// NewOptions() and select it at runtime with --storage-backend=cockroach.
type Options struct {
	cfg Config

	// fb caches the FactoryBackend across Build and BuildFactoryBackend so
	// CR storage and internal factory.Register dispatch share one pool.
	fb *FactoryBackend
}

// NewOptions returns a fresh Options for aggregator registration.
func NewOptions() *Options { return &Options{} }

// Name is the value matched against --storage-backend.
func (o *Options) Name() string { return "cockroach" }

// AddFlags binds the --cockroach-* flags. Called for every registered
// backend at startup so --help lists every backend's flags.
func (o *Options) AddFlags(fs *pflag.FlagSet) {
	fs.StringVar(&o.cfg.DSN, "cockroach-dsn", o.cfg.DSN,
		"PostgreSQL connection string for CockroachDB (multi-host supported: pgx will round-robin).")
	fs.StringVar(&o.cfg.Database, "cockroach-database", o.cfg.Database,
		"Optional database name override; when set, applied after connection instead of the DSN's database.")
	fs.Int32Var(&o.cfg.MaxConns, "cockroach-max-conns", o.cfg.MaxConns,
		"Maximum pool size. Zero picks 4 * GOMAXPROCS.")
	fs.DurationVar(&o.cfg.MaxConnLifetime, "cockroach-max-conn-lifetime", o.cfg.MaxConnLifetime,
		"Bounds how long any pool connection stays open. Finite values are required for rolling node restarts.")
	fs.DurationVar(&o.cfg.MaxConnIdleTime, "cockroach-max-conn-idle-time", o.cfg.MaxConnIdleTime,
		"Idle connections are closed after this interval.")
	fs.DurationVar(&o.cfg.HealthCheckPeriod, "cockroach-health-check-period", o.cfg.HealthCheckPeriod,
		"How often the pool prunes broken connections.")
}

// Validate runs flag-level checks. Only invoked when --storage-backend=cockroach.
func (o *Options) Validate() []error {
	var errs []error
	if o.cfg.DSN == "" {
		errs = append(errs, fmt.Errorf("--cockroach-dsn is required when --storage-backend=cockroach"))
	}
	return errs
}

// Build returns the per-resource Factory for CR storage. Applies the
// schema, dials the pool, and starts the changefeed + TTL scanner. Shares
// the resulting FactoryBackend with BuildFactoryBackend so a single
// runtime backs both CR storage and internal factory.Register dispatch.
func (o *Options) Build() (registry.Factory, error) {
	fb, err := o.factoryBackend()
	if err != nil {
		return nil, err
	}
	return registry.Factory(func(
		c *storagebackend.ConfigForResource,
		newFunc, newListFunc func() runtime.Object,
		resourcePrefix string,
	) (storage.Interface, factory.DestroyFunc, error) {
		return fb.Create(*c, newFunc, newListFunc, resourcePrefix)
	}), nil
}

// BuildFactoryBackend returns the fork-level factory.Backend implementation
// for internal state dispatch (leases, allocators). Idempotent.
func (o *Options) BuildFactoryBackend() (factory.Backend, error) {
	return o.factoryBackend()
}

func (o *Options) factoryBackend() (*FactoryBackend, error) {
	if o.fb != nil {
		return o.fb, nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	fb, err := NewFactoryBackend(ctx, o.cfg)
	if err != nil {
		return nil, err
	}
	o.fb = fb
	return fb, nil
}

// Compile-time assertion: Options satisfies registry.Backend.
var _ registry.Backend = (*Options)(nil)

// Config exposes the resolved configuration for callers that need it
// (health probes, debug endpoints, tests).
func (o *Options) Config() Config { return o.cfg }
