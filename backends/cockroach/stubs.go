package cockroach

import (
	"context"
	"errors"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/apiserver/pkg/storage"
)

// errNotImplemented backs the storage.Interface methods that later
// commits will replace. Each stub is a self-contained TODO.
var errNotImplemented = errors.New("cockroach: not implemented yet")

func (s *store) Watch(ctx context.Context, key string, opts storage.ListOptions) (watch.Interface, error) {
	return nil, errNotImplemented
}

func (s *store) GetList(ctx context.Context, key string, opts storage.ListOptions, listObj runtime.Object) error {
	return errNotImplemented
}

func (s *store) GuaranteedUpdate(
	ctx context.Context, key string, destination runtime.Object,
	ignoreNotFound bool, preconditions *storage.Preconditions,
	tryUpdate storage.UpdateFunc, cachedExistingObject runtime.Object,
) error {
	return errNotImplemented
}

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
