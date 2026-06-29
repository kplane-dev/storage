package spanner

import (
	"context"
	"fmt"
	"time"

	"cloud.google.com/go/spanner"
	"google.golang.org/api/iterator"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apiserver/pkg/storage"
	"k8s.io/apiserver/pkg/storage/etcd3/metrics"
	"k8s.io/apiserver/pkg/storage/storagebackend"
	"k8s.io/apiserver/pkg/storage/storagebackend/factory"
	"k8s.io/apiserver/pkg/storage/value/encrypt/identity"
)

// FactoryBackend implements factory.Backend by reusing a shared Spanner
// client across every Create call the apiserver makes through
// storagebackend/factory.Create — CR storage (via RESTOptionsGetter),
// master/peer endpoint leases, and service IP/NodePort allocators.
//
// The shared client is the whole point: the legacy per-Create NewBackendFactory
// dials a fresh client for every resource registry, which on a fleet apiserver
// means hundreds of Spanner clients per process. FactoryBackend dials once at
// Build time and hands every caller a thin view over it.
type FactoryBackend struct {
	cfg    SpannerConfig
	client *spanner.Client
}

var _ factory.Backend = (*FactoryBackend)(nil)

// NewFactoryBackend dials a Spanner client and returns a Backend that
// wraps it. EnsureSchema is expected to have run already (Options.Build
// does this); FactoryBackend doesn't apply schema so it can be safely
// constructed against a database the caller has already provisioned.
func NewFactoryBackend(ctx context.Context, cfg SpannerConfig) (*FactoryBackend, error) {
	client, err := cfg.NewClient(ctx)
	if err != nil {
		return nil, fmt.Errorf("dialing spanner: %w", err)
	}
	return &FactoryBackend{cfg: cfg, client: client}, nil
}

// Create returns a Spanner-backed storage.Interface for one resource. The
// returned DestroyFunc is intentionally a no-op: the shared client outlives
// any single resource, so per-resource destroy must not close it. The
// client is closed when the backend itself is torn down (see Close).
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
		b.client,
		c.Codec,
		newFunc,
		newListFunc,
		c.Prefix,
		resourcePrefix,
		transformer,
		c.WrapDecodedObject,
	)
	return s, func() {}, nil
}

// CreateHealthCheck returns a check that pings the Spanner control plane
// by doing a trivial single-use query. The apiserver wires this into
// /healthz/etcd despite the name — it's the backend's liveness probe.
func (b *FactoryBackend) CreateHealthCheck(_ storagebackend.Config, _ <-chan struct{}) (func() error, error) {
	return b.ping, nil
}

// CreateReadyCheck mirrors CreateHealthCheck. Spanner doesn't distinguish
// the two — if we can run a query, we're both healthy and ready.
func (b *FactoryBackend) CreateReadyCheck(_ storagebackend.Config, _ <-chan struct{}) (func() error, error) {
	return b.ping, nil
}

func (b *FactoryBackend) CreateProber(_ storagebackend.Config) (factory.Prober, error) {
	return &prober{ping: b.ping}, nil
}

func (b *FactoryBackend) CreateMonitor(_ storagebackend.Config) (metrics.Monitor, error) {
	return noopMonitor{}, nil
}

// Close releases the shared Spanner client. Called from the apiserver's
// shutdown path. Safe to call multiple times.
func (b *FactoryBackend) Close() {
	if b.client != nil {
		b.client.Close()
		b.client = nil
	}
}

func (b *FactoryBackend) ping() error {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	it := b.client.Single().Query(ctx, spanner.Statement{SQL: "SELECT 1"})
	defer it.Stop()
	if _, err := it.Next(); err != nil && err != iterator.Done {
		return fmt.Errorf("spanner ping: %w", err)
	}
	return nil
}

type prober struct {
	ping func() error
}

func (p *prober) Probe(_ context.Context) error { return p.ping() }
func (p *prober) Close() error                   { return nil }

// noopMonitor is the simplest metrics.Monitor that satisfies the interface
// for backends that don't expose database-size or histogram metrics yet.
// Storage-size / latency metrics live in the Spanner store itself.
type noopMonitor struct{}

func (noopMonitor) Monitor(_ context.Context) (metrics.StorageMetrics, error) {
	return metrics.StorageMetrics{}, nil
}
func (noopMonitor) Close() error { return nil }
