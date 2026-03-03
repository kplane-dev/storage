package storage

import (
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
)

// testPod is a minimal runtime.Object with ObjectMeta for testing delegation.
type testPod struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata"`
}

func (t *testPod) DeepCopyObject() runtime.Object {
	out := *t
	return &out
}

func TestMetaAccessor_WorksOnWrappedObject(t *testing.T) {
	pod := &testPod{
		ObjectMeta: metav1.ObjectMeta{
			Name:            "nginx",
			Namespace:       "default",
			UID:             types.UID("abc-123"),
			ResourceVersion: "42",
		},
	}
	wrapped := &ObjectWithClusterIdentity{
		Object:    pod,
		ClusterID: "c1",
	}

	accessor, err := meta.Accessor(wrapped)
	if err != nil {
		t.Fatalf("meta.Accessor failed on ObjectWithClusterIdentity: %v", err)
	}
	if accessor.GetName() != "nginx" {
		t.Errorf("GetName() = %q, want %q", accessor.GetName(), "nginx")
	}
	if accessor.GetNamespace() != "default" {
		t.Errorf("GetNamespace() = %q, want %q", accessor.GetNamespace(), "default")
	}
	if accessor.GetUID() != types.UID("abc-123") {
		t.Errorf("GetUID() = %q, want %q", accessor.GetUID(), "abc-123")
	}
	if accessor.GetResourceVersion() != "42" {
		t.Errorf("GetResourceVersion() = %q, want %q", accessor.GetResourceVersion(), "42")
	}
}

func TestMetaDelegation_Getters(t *testing.T) {
	now := metav1.Now()
	grace := int64(30)
	pod := &testPod{
		ObjectMeta: metav1.ObjectMeta{
			Name:                       "nginx",
			Namespace:                  "default",
			GenerateName:               "nginx-",
			UID:                        types.UID("uid-1"),
			ResourceVersion:            "100",
			Generation:                 3,
			CreationTimestamp:           now,
			DeletionTimestamp:           &now,
			DeletionGracePeriodSeconds: &grace,
			Labels:                     map[string]string{"app": "web"},
			Annotations:                map[string]string{"note": "test"},
			Finalizers:                 []string{"fin1"},
			OwnerReferences:            []metav1.OwnerReference{{Name: "owner"}},
		},
	}
	w := &ObjectWithClusterIdentity{Object: pod, ClusterID: "c1"}

	if w.GetName() != "nginx" {
		t.Errorf("GetName() = %q", w.GetName())
	}
	if w.GetNamespace() != "default" {
		t.Errorf("GetNamespace() = %q", w.GetNamespace())
	}
	if w.GetGenerateName() != "nginx-" {
		t.Errorf("GetGenerateName() = %q", w.GetGenerateName())
	}
	if w.GetUID() != types.UID("uid-1") {
		t.Errorf("GetUID() = %q", w.GetUID())
	}
	if w.GetResourceVersion() != "100" {
		t.Errorf("GetResourceVersion() = %q", w.GetResourceVersion())
	}
	if w.GetGeneration() != 3 {
		t.Errorf("GetGeneration() = %d", w.GetGeneration())
	}
	ct := w.GetCreationTimestamp()
	if !ct.Equal(&now) {
		t.Errorf("GetCreationTimestamp() mismatch")
	}
	if w.GetDeletionTimestamp() == nil || !w.GetDeletionTimestamp().Equal(&now) {
		t.Errorf("GetDeletionTimestamp() mismatch")
	}
	if w.GetDeletionGracePeriodSeconds() == nil || *w.GetDeletionGracePeriodSeconds() != 30 {
		t.Errorf("GetDeletionGracePeriodSeconds() mismatch")
	}
	if w.GetLabels()["app"] != "web" {
		t.Errorf("GetLabels() mismatch")
	}
	if w.GetAnnotations()["note"] != "test" {
		t.Errorf("GetAnnotations() mismatch")
	}
	if len(w.GetFinalizers()) != 1 || w.GetFinalizers()[0] != "fin1" {
		t.Errorf("GetFinalizers() mismatch")
	}
	if len(w.GetOwnerReferences()) != 1 || w.GetOwnerReferences()[0].Name != "owner" {
		t.Errorf("GetOwnerReferences() mismatch")
	}
}

func TestMetaDelegation_Setters(t *testing.T) {
	pod := &testPod{
		ObjectMeta: metav1.ObjectMeta{Name: "original"},
	}
	w := &ObjectWithClusterIdentity{Object: pod, ClusterID: "c1"}

	w.SetName("updated")
	if pod.Name != "updated" {
		t.Errorf("SetName did not modify inner object: got %q", pod.Name)
	}

	w.SetNamespace("kube-system")
	if pod.Namespace != "kube-system" {
		t.Errorf("SetNamespace did not modify inner object: got %q", pod.Namespace)
	}

	w.SetResourceVersion("200")
	if pod.ResourceVersion != "200" {
		t.Errorf("SetResourceVersion did not modify inner object: got %q", pod.ResourceVersion)
	}

	w.SetLabels(map[string]string{"new": "label"})
	if pod.Labels["new"] != "label" {
		t.Errorf("SetLabels did not modify inner object")
	}

	now := metav1.NewTime(time.Now())
	w.SetCreationTimestamp(now)
	if !pod.CreationTimestamp.Equal(&now) {
		t.Errorf("SetCreationTimestamp did not modify inner object")
	}
}

func TestMetaDelegation_FallbackOnNonAccessible(t *testing.T) {
	// fakeObject from identity_test.go does not implement metav1.Object
	inner := &fakeObject{name: "test"}
	w := &ObjectWithClusterIdentity{Object: inner, ClusterID: "c1"}

	// Should not panic, should return zero values
	if w.GetName() != "" {
		t.Errorf("expected empty name for non-accessible object, got %q", w.GetName())
	}
	if w.GetNamespace() != "" {
		t.Errorf("expected empty namespace for non-accessible object, got %q", w.GetNamespace())
	}
	if w.GetResourceVersion() != "" {
		t.Errorf("expected empty resource version for non-accessible object, got %q", w.GetResourceVersion())
	}
}
