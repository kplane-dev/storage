// Package storage provides cluster-aware storage primitives for multicluster
// Kubernetes API servers. It bridges the kplane-dev/kubernetes fork (which adds
// cluster identity to the cacher pipeline) with a clean StorageDecorator that
// configures the identity hooks and provides typed envelopes for consumers.
package storage

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// Ensure ObjectWithClusterIdentity implements ObjectMetaAccessor so that
// meta.Accessor can extract metadata (e.g., ResourceVersion) from wrapped
// objects. Without this, the reflector's watchHandler fails because it calls
// meta.Accessor on every watch event.
var _ metav1.ObjectMetaAccessor = (*ObjectWithClusterIdentity)(nil)

// ObjectWithClusterIdentity wraps a runtime.Object with cluster identity
// metadata derived from the storage key layout. This envelope is used at the
// watch output boundary — watch events carry this type so consumers can
// extract the cluster ID without modifying the underlying object.
//
// ObjectWithClusterIdentity implements runtime.Object so it can be placed
// directly in watch.Event.Object.
type ObjectWithClusterIdentity struct {
	// Object is the original Kubernetes object (e.g., *v1.Pod).
	Object runtime.Object

	// ClusterID is the cluster identity derived from the storage key layout.
	ClusterID string

	// StorageKey is the full storage key path for the object.
	StorageKey string
}

// GetObjectMeta delegates metadata access to the underlying object so that
// wrapped objects remain transparent to meta.Accessor and the reflector.
func (o *ObjectWithClusterIdentity) GetObjectMeta() metav1.Object {
	if accessor, ok := o.Object.(metav1.ObjectMetaAccessor); ok {
		return accessor.GetObjectMeta()
	}
	return nil
}

// GetObjectKind delegates to the underlying object.
func (o *ObjectWithClusterIdentity) GetObjectKind() schema.ObjectKind {
	return o.Object.GetObjectKind()
}

// DeepCopyObject creates a deep copy of the envelope and its underlying object.
func (o *ObjectWithClusterIdentity) DeepCopyObject() runtime.Object {
	return &ObjectWithClusterIdentity{
		Object:     o.Object.DeepCopyObject(),
		ClusterID:  o.ClusterID,
		StorageKey: o.StorageKey,
	}
}

// UnwrapClusterIdentity extracts the underlying object and cluster ID from
// an object that may or may not be wrapped in ObjectWithClusterIdentity.
// If the object is not wrapped, clusterID is empty.
func UnwrapClusterIdentity(obj runtime.Object) (inner runtime.Object, clusterID string) {
	if env, ok := obj.(*ObjectWithClusterIdentity); ok {
		return env.Object, env.ClusterID
	}
	return obj, ""
}
