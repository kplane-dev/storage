package cockroach

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apiserver/pkg/storage"
	"k8s.io/apiserver/pkg/storage/etcd3/metrics"
	"k8s.io/apiserver/pkg/storage/storagebackend"
	"k8s.io/apiserver/pkg/storage/storagebackend/factory"
	"k8s.io/apiserver/pkg/storage/value/encrypt/identity"
)

// FactoryBackend implements factory.Backend by sharing a single
// sharedRuntime (pool, changefeed, TTL scanner) across every Create call
// the apiserver's storagebackend/factory dispatch makes — CR storage,
// master/peer endpoint leases, service IP allocators.
type FactoryBackend struct {
	cfg    Config
	shared *sharedRuntime
}

var _ factory.Backend = (*FactoryBackend)(nil)

// NewFactoryBackend applies the schema, dials the pool, starts the
// changefeed and the TTL scanner, and returns a Backend wrapping them.
func NewFactoryBackend(ctx context.Context, cfg Config) (*FactoryBackend, error) {
	if err := EnsureSchema(ctx, cfg); err != nil {
		return nil, fmt.Errorf("ensure schema: %w", err)
	}
	pool, err := cfg.NewPool(ctx)
	if err != nil {
		return nil, fmt.Errorf("new pool: %w", err)
	}
	connCfg, err := cfg.ConnConfig()
	if err != nil {
		pool.Close()
		return nil, err
	}
	cf := NewChangefeedSubscription(connCfg)
	cf.Start(context.Background())

	sr := &sharedRuntime{pool: pool, changefeed: cf}
	sr.scanner = NewTTLScanner(pool, 0)
	sr.scanner.Start(context.Background())

	return &FactoryBackend{cfg: cfg, shared: sr}, nil
}

// Create returns a Cockroach-backed storage.Interface for one resource.
// The returned DestroyFunc is a no-op — the shared runtime outlives any
// single resource and is disposed by Close.
func (b *FactoryBackend) Create(
	c storagebackend.ConfigForResource,
	newFunc, newListFunc func() runtime.Object,
	resourcePrefix string,
) (storage.Interface, factory.DestroyFunc, error) {
	transformer := c.Transformer
	if transformer == nil {
		transformer = identity.NewEncryptCheckTransformer()
	}
	s := NewStore(
		b.shared.pool,
		c.Codec,
		newFunc,
		newListFunc,
		c.Prefix,
		resourcePrefix,
		transformer,
		c.WrapDecodedObject,
	)
	s.SetChangefeed(b.shared.changefeed)
	return s, func() {}, nil
}

// CreateHealthCheck / CreateReadyCheck use the pool's Ping, which
// serializes a single-connection RTT against the cluster.
func (b *FactoryBackend) CreateHealthCheck(_ storagebackend.Config, _ <-chan struct{}) (func() error, error) {
	return b.ping, nil
}

func (b *FactoryBackend) CreateReadyCheck(_ storagebackend.Config, _ <-chan struct{}) (func() error, error) {
	return b.ping, nil
}

func (b *FactoryBackend) CreateProber(_ storagebackend.Config) (factory.Prober, error) {
	return &prober{ping: b.ping}, nil
}

func (b *FactoryBackend) CreateMonitor(_ storagebackend.Config) (metrics.Monitor, error) {
	return noopMonitor{}, nil
}

// Close disposes the shared runtime. Safe to call multiple times.
func (b *FactoryBackend) Close() {
	if b.shared != nil {
		b.shared.Close()
		b.shared = nil
	}
}

func (b *FactoryBackend) ping() error {
	if b.shared == nil || b.shared.pool == nil {
		return fmt.Errorf("cockroach: backend closed")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	return b.shared.pool.Ping(ctx)
}

// Pool exposes the shared connection pool for callers that need to run
// their own SQL (e.g. tests, ad-hoc probes).
func (b *FactoryBackend) Pool() *pgxpool.Pool {
	if b.shared == nil {
		return nil
	}
	return b.shared.pool
}

type prober struct {
	ping func() error
}

func (p *prober) Probe(_ context.Context) error { return p.ping() }
func (p *prober) Close() error                  { return nil }

type noopMonitor struct{}

func (noopMonitor) Monitor(_ context.Context) (metrics.StorageMetrics, error) {
	return metrics.StorageMetrics{}, nil
}
func (noopMonitor) Close() error { return nil }
