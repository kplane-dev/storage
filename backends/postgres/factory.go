package postgres

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
// that creates PostgreSQL-backed storage.Interface instances sharing a
// single pgxpool, watch feed, and TTL scanner across every resource. The
// returned factory must be paired with a Close() call at apiserver shutdown;
// see sharedRuntime.Close.
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
	feed := NewFeed(pool, connCfg)
	feed.Start(context.Background())

	shared := &sharedRuntime{pool: pool, feed: feed}
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

// sharedRuntime is the process-wide state a Postgres backend keeps — pool,
// watch feed, TTL scanner — that every per-resource store references. Owns
// Close so callers dispose it once.
type sharedRuntime struct {
	pool    *pgxpool.Pool
	feed    *Feed
	scanner *TTLScanner
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
	s.SetFeed(r.feed)
	return s
}

// Close disposes the shared runtime. Safe to call multiple times.
func (r *sharedRuntime) Close() {
	if r.scanner != nil {
		r.scanner.Stop()
	}
	if r.feed != nil {
		r.feed.Stop()
	}
	if r.pool != nil {
		r.pool.Close()
	}
}
