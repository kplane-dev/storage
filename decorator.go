package storage

import (
	"sync"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apiserver/pkg/storage"
	cacherstorage "k8s.io/apiserver/pkg/storage/cacher"
	"k8s.io/apiserver/pkg/registry/generic"
	"k8s.io/apiserver/pkg/storage/storagebackend"
	"k8s.io/apiserver/pkg/storage/storagebackend/factory"
	"k8s.io/client-go/tools/cache"

	"k8s.io/klog/v2"
)

// DecoratorConfig configures the cluster-aware StorageDecorator.
type DecoratorConfig struct {
	// KeyLayout defines how cluster identity is embedded in storage keys.
	KeyLayout KeyLayout

	// GroupResource identifies the resource type being stored.
	// Used for logging and metrics.
	GroupResource schema.GroupResource
}

// StorageWithClusterIdentity returns a generic.StorageDecorator that creates
// a standard cacher pipeline with cluster identity hooks configured.
//
// Watch events emitted by the cacher will have their objects wrapped in
// ObjectWithClusterIdentity, allowing consumers to extract cluster ID without
// modifying the underlying Kubernetes object.
//
// The decorator is a drop-in replacement for registry.StorageWithCacher().
func StorageWithClusterIdentity(cfg DecoratorConfig) generic.StorageDecorator {
	identityFromKey := cfg.KeyLayout.IdentityFromKey()

	wrapWatchObject := func(object runtime.Object, key string, clusterID string) runtime.Object {
		return &ObjectWithClusterIdentity{
			Object:     object,
			ClusterID:  clusterID,
			StorageKey: key,
		}
	}

	wrapDecodedObject := func(object runtime.Object, key string) runtime.Object {
		return &ObjectWithClusterIdentity{
			Object:     object,
			StorageKey: key,
		}
	}

	unwrapObject := func(obj runtime.Object) runtime.Object {
		if identity, ok := obj.(*ObjectWithClusterIdentity); ok && identity != nil {
			return identity.Object
		}
		return obj
	}

	return func(
		storageConfig *storagebackend.ConfigForResource,
		resourcePrefix string,
		keyFunc func(obj runtime.Object) (string, error),
		newFunc func() runtime.Object,
		newListFunc func() runtime.Object,
		getAttrsFunc storage.AttrFunc,
		triggerFuncs storage.IndexerFuncs,
		indexers *cache.Indexers,
	) (storage.Interface, factory.DestroyFunc, error) {
		// Set the wrapping hook on the storage config so the etcd3
		// layer wraps decoded objects with their storage key.
		storageConfig.WrapDecodedObject = wrapDecodedObject

		// Wrap the caller's keyFunc so that ObjectWithClusterIdentity
		// envelopes return their StorageKey directly. Without this, the
		// cacher's watchCache would call the caller's keyFunc on wrapped
		// objects, which typically cannot determine the correct cluster
		// from object metadata alone and falls back to a default cluster.
		callerKeyFunc := keyFunc
		keyFunc = func(obj runtime.Object) (string, error) {
			if identity, ok := obj.(*ObjectWithClusterIdentity); ok && identity != nil {
				if identity.StorageKey != "" {
					return identity.StorageKey, nil
				}
				obj = identity.Object
			}
			return callerKeyFunc(obj)
		}

		s, d, err := generic.NewRawStorage(storageConfig, newFunc, newListFunc, resourcePrefix)
		if err != nil {
			return s, d, err
		}

		if klogV := klog.V(5); klogV.Enabled() {
			klogV.InfoS("Cluster-aware storage caching enabled",
				"resource", cfg.GroupResource.String(),
				"prefix", resourcePrefix,
			)
		}

		cacherConfig := cacherstorage.Config{
			Storage:             s,
			Versioner:           storage.APIObjectVersioner{},
			GroupResource:       storageConfig.GroupResource,
			EventsHistoryWindow: storageConfig.EventsHistoryWindow,
			ResourcePrefix:      resourcePrefix,
			KeyFunc:             keyFunc,
			NewFunc:             newFunc,
			NewListFunc:         newListFunc,
			GetAttrsFunc:        getAttrsFunc,
			IndexerFuncs:        triggerFuncs,
			Indexers:            indexers,
			Codec:               storageConfig.Codec,
			// Cluster identity hooks:
			IdentityFromKey: identityFromKey,
			WrapWatchObject: wrapWatchObject,
			UnwrapObject:    unwrapObject,
		}

		cacher, err := cacherstorage.NewCacherFromConfig(cacherConfig)
		if err != nil {
			return nil, func() {}, err
		}
		delegator := cacherstorage.NewCacheDelegator(cacher, s)

		var once sync.Once
		destroyFunc := func() {
			once.Do(func() {
				delegator.Stop()
				cacher.Stop()
				d()
			})
		}

		return delegator, destroyFunc, nil
	}
}
