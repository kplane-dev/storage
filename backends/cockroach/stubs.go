package cockroach

import (
	"context"
	"errors"

	"k8s.io/apiserver/pkg/storage"
)

// errNotImplemented backs the storage.Interface methods that later
// commits will replace. Each stub is a self-contained TODO.
var errNotImplemented = errors.New("cockroach: not implemented yet")

func (s *store) Stats(ctx context.Context) (storage.Stats, error) {
	return storage.Stats{}, errNotImplemented
}

func (s *store) ReadinessCheck() error {
	return s.pool.Ping(context.Background())
}

func (s *store) RequestWatchProgress(ctx context.Context) error {
	return errNotImplemented
}

func (s *store) EnableResourceSizeEstimation(fn storage.KeysFunc) error {
	return errNotImplemented
}

func (s *store) CompactRevision() int64 {
	return 0
}
