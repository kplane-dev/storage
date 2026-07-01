package cockroach

import (
	"context"
	"errors"
	"strings"

	"k8s.io/apiserver/pkg/storage"
)

// errNotImplemented backs the storage.Interface methods that later
// commits will replace. Each stub is a self-contained TODO.
var errNotImplemented = errors.New("cockroach: not implemented yet")

// Stats returns a count of rows under this store's resource prefix.
func (s *store) Stats(ctx context.Context) (storage.Stats, error) {
	var count int64
	prefix := s.pathPrefix + strings.TrimPrefix(s.resourcePrefix, "/")
	if err := s.pool.QueryRow(ctx,
		`SELECT count(*) FROM kv WHERE key >= $1 AND key < $2`,
		prefix, prefixEnd(prefix),
	).Scan(&count); err != nil {
		return storage.Stats{}, err
	}
	return storage.Stats{ObjectCount: count}, nil
}

// ReadinessCheck pings the underlying pool.
func (s *store) ReadinessCheck() error {
	return s.pool.Ping(context.Background())
}

// RequestWatchProgress is a best-effort no-op. Cockroach changefeeds emit
// resolved-timestamp rows on their own cadence; we don't force an early
// one. Consumers relying on progress notifications will see them from
// the natural stream instead.
func (s *store) RequestWatchProgress(ctx context.Context) error { return nil }

// EnableResourceSizeEstimation is a no-op — we don't estimate sizes yet.
// Same shape as the Spanner backend; used by admission for RSS accounting.
func (s *store) EnableResourceSizeEstimation(fn storage.KeysFunc) error { return nil }

func (s *store) CompactRevision() int64 {
	return 0
}
