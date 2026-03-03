package storage

import (
	"testing"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// fakeObject implements runtime.Object for testing.
type fakeObject struct {
	name string
}

func (f *fakeObject) GetObjectKind() schema.ObjectKind { return schema.EmptyObjectKind }
func (f *fakeObject) DeepCopyObject() runtime.Object    { return &fakeObject{name: f.name} }

func TestObjectWithClusterIdentity_Implements_RuntimeObject(t *testing.T) {
	var _ runtime.Object = &ObjectWithClusterIdentity{}
}

func TestObjectWithClusterIdentity_DeepCopy(t *testing.T) {
	orig := &ObjectWithClusterIdentity{
		Object:     &fakeObject{name: "test"},
		ClusterID:  "c1",
		StorageKey: "/pods/clusters/c1/default/test",
	}

	copied := orig.DeepCopyObject()
	env, ok := copied.(*ObjectWithClusterIdentity)
	if !ok {
		t.Fatalf("DeepCopyObject returned %T, want *ObjectWithClusterIdentity", copied)
	}
	if env.ClusterID != "c1" {
		t.Errorf("ClusterID = %q, want %q", env.ClusterID, "c1")
	}
	if env.StorageKey != "/pods/clusters/c1/default/test" {
		t.Errorf("StorageKey = %q, want %q", env.StorageKey, "/pods/clusters/c1/default/test")
	}
}

func TestUnwrapClusterIdentity_Wrapped(t *testing.T) {
	inner := &fakeObject{name: "test"}
	wrapped := &ObjectWithClusterIdentity{
		Object:    inner,
		ClusterID: "c1",
	}

	obj, clusterID := UnwrapClusterIdentity(wrapped)
	if obj != inner {
		t.Error("UnwrapClusterIdentity did not return the inner object")
	}
	if clusterID != "c1" {
		t.Errorf("clusterID = %q, want %q", clusterID, "c1")
	}
}

func TestUnwrapClusterIdentity_Unwrapped(t *testing.T) {
	inner := &fakeObject{name: "test"}

	obj, clusterID := UnwrapClusterIdentity(inner)
	if obj != inner {
		t.Error("UnwrapClusterIdentity did not return the original object")
	}
	if clusterID != "" {
		t.Errorf("clusterID = %q, want empty", clusterID)
	}
}
