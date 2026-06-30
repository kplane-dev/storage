package spanner

import (
	"context"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apiserver/pkg/storage"
	"k8s.io/apiserver/pkg/storage/storagebackend"
	"k8s.io/apiserver/pkg/storage/storagebackend/factory"
	"k8s.io/apiserver/pkg/storage/value/encrypt/identity"

	kpstorage "github.com/kplane-dev/storage"
)

// NewBackendFactory returns a storage.BackendFactory (defined on the top
// level of kplane-dev/storage) that creates Spanner-backed
// storage.Interface instances. Each call to the returned factory creates a
// new store sharing the same Spanner client.
//
// Pre-KPEP-0001, this lived in a standalone kplane-dev/spanner repo and
// kept its own duplicated BackendFactory type "to avoid an import cycle."
// Now that Spanner is a subpackage of kplane-dev/storage, we can reference
// storage.BackendFactory (== registry.Factory) directly.
func NewBackendFactory(cfg SpannerConfig) kpstorage.BackendFactory {
	return func(
		config *storagebackend.ConfigForResource,
		newFunc, newListFunc func() runtime.Object,
		resourcePrefix string,
	) (storage.Interface, factory.DestroyFunc, error) {
		ctx := context.Background()

		client, err := cfg.NewClient(ctx)
		if err != nil {
			return nil, nil, err
		}

		transformer := config.Transformer
		if transformer == nil {
			transformer = identity.NewEncryptCheckTransformer()
		}

		s := NewStore(
			client,
			config.Codec,
			newFunc,
			newListFunc,
			config.Prefix,
			resourcePrefix,
			transformer,
			config.WrapDecodedObject,
		)

		destroyFunc := func() {
			client.Close()
		}

		return s, destroyFunc, nil
	}
}
