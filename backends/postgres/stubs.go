package postgres

import (
	"context"
	"strings"

	"k8s.io/apiserver/pkg/storage"
)

// Stats returns the count of live objects under this store's resource
// prefix. In the append-only log that's the number of distinct names whose
// newest row is a non-tombstoned, unexpired object.
func (s *store) Stats(ctx context.Context) (storage.Stats, error) {
	prefix := s.pathPrefix + strings.TrimPrefix(s.resourcePrefix, "/")
	var count int64
	if err := s.pool.QueryRow(ctx, `
		SELECT count(*) FROM (
			SELECT DISTINCT ON (name) name, deleted, expire_at
			FROM kv WHERE name >= $1 AND name < $2
			ORDER BY name, id DESC
		) t
		WHERE deleted = false AND (expire_at IS NULL OR expire_at > now())`,
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

// RequestWatchProgress forces a progress notification to all active watchers
// at the current gap-safe resource version. Called by the cacher's
// ConditionalProgressRequester when a client blocks waiting for the
// watchCache to reach a fresh RV; without it, waitlist requests hang until
// the backstop poll instead of unblocking promptly.
func (s *store) RequestWatchProgress(ctx context.Context) error {
	if s.feed == nil {
		return nil
	}
	rv, err := s.GetCurrentResourceVersion(ctx)
	if err != nil {
		return err
	}
	s.feed.PublishProgress(rv)
	return nil
}

// EnableResourceSizeEstimation is a no-op — we don't estimate sizes yet.
// Same shape as the Spanner and Cockroach backends.
func (s *store) EnableResourceSizeEstimation(fn storage.KeysFunc) error { return nil }

// CompactRevision returns 0: this backend retains full history and does not
// yet expose a compaction horizon. The conformance suite's compaction phase
// self-skips when this is 0, matching Spanner and Cockroach.
func (s *store) CompactRevision() int64 { return 0 }
