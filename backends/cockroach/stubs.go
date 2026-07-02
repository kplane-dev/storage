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

// RequestWatchProgress forces a progress notification to all active
// watchers using the current cluster_logical_timestamp. Called by the
// cacher's ConditionalProgressRequester when a client blocks waiting
// for the watchCache to reach a fresh RV. Without this, waitlist
// requests hang for 3 seconds and time out with TooLargeResourceVersion.
func (s *store) RequestWatchProgress(ctx context.Context) error {
	if s.changefeed == nil {
		return nil
	}
	rv, err := s.GetCurrentResourceVersion(ctx)
	if err != nil {
		return err
	}
	s.changefeed.PublishProgress(rvToHLC(rv))
	return nil
}

// EnableResourceSizeEstimation is a no-op — we don't estimate sizes yet.
// Same shape as the Spanner backend; used by admission for RSS accounting.
func (s *store) EnableResourceSizeEstimation(fn storage.KeysFunc) error { return nil }

func (s *store) CompactRevision() int64 {
	return 0
}
