package cockroach

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apiserver/pkg/storage"
	"k8s.io/apiserver/pkg/storage/storagebackend"
	"k8s.io/apiserver/pkg/storage/storagebackend/factory"
	"k8s.io/apiserver/pkg/storage/value/encrypt/identity"

	kpstorage "github.com/kplane-dev/storage"
)

// NewBackendFactory returns a kpstorage.BackendFactory (== registry.Factory)
// that creates CockroachDB-backed storage.Interface instances sharing a
// single pgxpool, changefeed subscription, and TTL scanner across every
// resource. The returned factory must be paired with a Close() call at
// apiserver shutdown; see FactoryBackend.Close.
func NewBackendFactory(cfg Config) (kpstorage.BackendFactory, func(), error) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := EnsureSchema(ctx, cfg); err != nil {
		return nil, nil, fmt.Errorf("ensure schema: %w", err)
	}
	pool, err := cfg.NewPool(context.Background())
	if err != nil {
		return nil, nil, fmt.Errorf("new pool: %w", err)
	}
	connCfg, err := cfg.ConnConfig()
	if err != nil {
		pool.Close()
		return nil, nil, err
	}
	cf := NewChangefeedSubscription(connCfg)
	cf.Start(context.Background())

	shared := &sharedRuntime{pool: pool, changefeed: cf}
	shared.scanner = NewTTLScanner(pool, 0)
	shared.scanner.Start(context.Background())

	build := kpstorage.BackendFactory(func(
		c *storagebackend.ConfigForResource,
		newFunc, newListFunc func() runtime.Object,
		resourcePrefix string,
	) (storage.Interface, factory.DestroyFunc, error) {
		return shared.newStore(c, newFunc, newListFunc, resourcePrefix), func() {}, nil
	})

	return build, shared.Close, nil
}

// sharedRuntime is the process-wide state a Cockroach backend keeps —
// pool, changefeed subscription, TTL scanner — that every per-resource
// store references. Owns Close so callers dispose it once.
type sharedRuntime struct {
	pool       *pgxpool.Pool
	changefeed *ChangefeedSubscription
	scanner    *TTLScanner
}

func (r *sharedRuntime) newStore(
	c *storagebackend.ConfigForResource,
	newFunc, newListFunc func() runtime.Object,
	resourcePrefix string,
) *store {
	transformer := c.Transformer
	if transformer == nil {
		transformer = identity.NewEncryptCheckTransformer()
	}
	s := NewStore(
		r.pool,
		c.Codec,
		newFunc,
		newListFunc,
		c.Prefix,
		resourcePrefix,
		transformer,
		c.WrapDecodedObject,
	)
	s.SetChangefeed(r.changefeed)
	return s
}

// Close disposes the shared runtime. Safe to call multiple times.
func (r *sharedRuntime) Close() {
	if r.scanner != nil {
		r.scanner.Stop()
	}
	if r.changefeed != nil {
		r.changefeed.Stop()
	}
	if r.pool != nil {
		r.pool.Close()
	}
}
