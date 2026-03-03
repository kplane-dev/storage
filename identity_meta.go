package storage

import (
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

// Compile-time assertion that ObjectWithClusterIdentity implements metav1.Object.
// This ensures meta.Accessor() works on wrapped objects, which is required by
// client-go's Reflector, cache key functions, and other Kubernetes infrastructure.
var _ metav1.Object = (*ObjectWithClusterIdentity)(nil)

// accessor returns the metav1.Object interface for the inner object.
// Falls back to an empty ObjectMeta if the inner object is not accessible.
func (o *ObjectWithClusterIdentity) accessor() metav1.Object {
	m, err := meta.Accessor(o.Object)
	if err != nil {
		return &metav1.ObjectMeta{}
	}
	return m
}

func (o *ObjectWithClusterIdentity) GetNamespace() string              { return o.accessor().GetNamespace() }
func (o *ObjectWithClusterIdentity) SetNamespace(namespace string)     { o.accessor().SetNamespace(namespace) }
func (o *ObjectWithClusterIdentity) GetName() string                   { return o.accessor().GetName() }
func (o *ObjectWithClusterIdentity) SetName(name string)               { o.accessor().SetName(name) }
func (o *ObjectWithClusterIdentity) GetGenerateName() string           { return o.accessor().GetGenerateName() }
func (o *ObjectWithClusterIdentity) SetGenerateName(name string)       { o.accessor().SetGenerateName(name) }
func (o *ObjectWithClusterIdentity) GetUID() types.UID                 { return o.accessor().GetUID() }
func (o *ObjectWithClusterIdentity) SetUID(uid types.UID)              { o.accessor().SetUID(uid) }
func (o *ObjectWithClusterIdentity) GetResourceVersion() string        { return o.accessor().GetResourceVersion() }
func (o *ObjectWithClusterIdentity) SetResourceVersion(version string) { o.accessor().SetResourceVersion(version) }
func (o *ObjectWithClusterIdentity) GetGeneration() int64              { return o.accessor().GetGeneration() }
func (o *ObjectWithClusterIdentity) SetGeneration(generation int64)    { o.accessor().SetGeneration(generation) }
func (o *ObjectWithClusterIdentity) GetSelfLink() string               { return o.accessor().GetSelfLink() }
func (o *ObjectWithClusterIdentity) SetSelfLink(selfLink string)       { o.accessor().SetSelfLink(selfLink) }
func (o *ObjectWithClusterIdentity) GetCreationTimestamp() metav1.Time { return o.accessor().GetCreationTimestamp() }
func (o *ObjectWithClusterIdentity) SetCreationTimestamp(timestamp metav1.Time) {
	o.accessor().SetCreationTimestamp(timestamp)
}
func (o *ObjectWithClusterIdentity) GetDeletionTimestamp() *metav1.Time {
	return o.accessor().GetDeletionTimestamp()
}
func (o *ObjectWithClusterIdentity) SetDeletionTimestamp(timestamp *metav1.Time) {
	o.accessor().SetDeletionTimestamp(timestamp)
}
func (o *ObjectWithClusterIdentity) GetDeletionGracePeriodSeconds() *int64 {
	return o.accessor().GetDeletionGracePeriodSeconds()
}
func (o *ObjectWithClusterIdentity) SetDeletionGracePeriodSeconds(i *int64) {
	o.accessor().SetDeletionGracePeriodSeconds(i)
}
func (o *ObjectWithClusterIdentity) GetLabels() map[string]string { return o.accessor().GetLabels() }
func (o *ObjectWithClusterIdentity) SetLabels(labels map[string]string) {
	o.accessor().SetLabels(labels)
}
func (o *ObjectWithClusterIdentity) GetAnnotations() map[string]string {
	return o.accessor().GetAnnotations()
}
func (o *ObjectWithClusterIdentity) SetAnnotations(annotations map[string]string) {
	o.accessor().SetAnnotations(annotations)
}
func (o *ObjectWithClusterIdentity) GetFinalizers() []string { return o.accessor().GetFinalizers() }
func (o *ObjectWithClusterIdentity) SetFinalizers(finalizers []string) {
	o.accessor().SetFinalizers(finalizers)
}
func (o *ObjectWithClusterIdentity) GetOwnerReferences() []metav1.OwnerReference {
	return o.accessor().GetOwnerReferences()
}
func (o *ObjectWithClusterIdentity) SetOwnerReferences(references []metav1.OwnerReference) {
	o.accessor().SetOwnerReferences(references)
}
func (o *ObjectWithClusterIdentity) GetManagedFields() []metav1.ManagedFieldsEntry {
	return o.accessor().GetManagedFields()
}
func (o *ObjectWithClusterIdentity) SetManagedFields(managedFields []metav1.ManagedFieldsEntry) {
	o.accessor().SetManagedFields(managedFields)
}
