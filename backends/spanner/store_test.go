package spanner

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	instance "cloud.google.com/go/spanner/admin/instance/apiv1"
	instancepb "cloud.google.com/go/spanner/admin/instance/apiv1/instancepb"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/labels"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/runtime/serializer"
	"k8s.io/apimachinery/pkg/types"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/apiserver/pkg/storage"
	"k8s.io/apiserver/pkg/storage/value/encrypt/identity"
)

// testObj is a minimal runtime.Object for testing.
type testObj struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`
	Data              string `json:"data,omitempty"`
}

func (t *testObj) DeepCopyObject() runtime.Object {
	return &testObj{
		TypeMeta:   t.TypeMeta,
		ObjectMeta: *t.ObjectMeta.DeepCopy(),
		Data:       t.Data,
	}
}

type testObjList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []testObj `json:"items"`
}

func (t *testObjList) DeepCopyObject() runtime.Object {
	out := &testObjList{
		TypeMeta: t.TypeMeta,
		ListMeta: *t.ListMeta.DeepCopy(),
	}
	for _, item := range t.Items {
		out.Items = append(out.Items, *item.DeepCopyObject().(*testObj))
	}
	return out
}

var (
	testScheme = runtime.NewScheme()
	testCodecs serializer.CodecFactory
	testGVK    = schema.GroupVersionKind{Group: "test.io", Version: "v1", Kind: "TestObj"}
	testGVR    = schema.GroupVersionResource{Group: "test.io", Version: "v1", Resource: "testobjs"}
)

func init() {
	schemeBuilder := runtime.NewSchemeBuilder(func(s *runtime.Scheme) error {
		s.AddKnownTypeWithName(testGVK, &testObj{})
		s.AddKnownTypeWithName(schema.GroupVersionKind{Group: "test.io", Version: "v1", Kind: "TestObjList"}, &testObjList{})
		metav1.AddToGroupVersion(s, schema.GroupVersion{Group: "test.io", Version: "v1"})
		return nil
	})
	utilruntime.Must(schemeBuilder.AddToScheme(testScheme))
	testCodecs = serializer.NewCodecFactory(testScheme)
}

func emulatorHost() string {
	if h := os.Getenv("SPANNER_EMULATOR_HOST"); h != "" {
		return h
	}
	return "localhost:9010"
}

func skipIfNoEmulator(t *testing.T) {
	t.Helper()
	host := emulatorHost()
	// Quick connection test.
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	conn, err := grpc.DialContext(ctx, host, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithBlock())
	if err != nil {
		t.Skipf("Spanner emulator not available at %s: %v", host, err)
	}
	conn.Close()
}

func setupTestStore(t *testing.T) *store {
	t.Helper()
	skipIfNoEmulator(t)

	ctx := context.Background()
	host := emulatorHost()
	dbName := fmt.Sprintf("testdb_%d", time.Now().UnixNano())

	cfg := SpannerConfig{
		Project:      "test-project",
		Instance:     "test-instance",
		Database:     dbName,
		EmulatorHost: host,
	}

	// Create instance if not exists.
	opts := []option.ClientOption{
		option.WithEndpoint(host),
		option.WithGRPCDialOption(grpc.WithTransportCredentials(insecure.NewCredentials())),
		option.WithoutAuthentication(),
	}
	adminClient, err := instance.NewInstanceAdminClient(ctx, opts...)
	if err != nil {
		t.Fatalf("creating instance admin client: %v", err)
	}
	defer adminClient.Close()

	op, err := adminClient.CreateInstance(ctx, &instancepb.CreateInstanceRequest{
		Parent:     "projects/test-project",
		InstanceId: "test-instance",
		Instance: &instancepb.Instance{
			Config:      "emulator-config",
			DisplayName: "Test Instance",
			NodeCount:   1,
		},
	})
	if err == nil {
		if _, err := op.Wait(ctx); err != nil {
			// Instance may already exist, ignore.
			_ = err
		}
	}

	// Create database with schema.
	if err := EnsureSchema(ctx, cfg); err != nil {
		t.Fatalf("EnsureSchema: %v", err)
	}

	client, err := cfg.NewClient(ctx)
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	t.Cleanup(func() { client.Close() })

	codec := testCodecs.LegacyCodec(schema.GroupVersion{Group: "test.io", Version: "v1"})

	s := NewStore(
		client,
		codec,
		func() runtime.Object { return &testObj{} },
		func() runtime.Object { return &testObjList{} },
		"/registry",
		"/testobjs",
		identity.NewEncryptCheckTransformer(),
		nil,
	)

	return s
}

func TestCreate(t *testing.T) {
	s := setupTestStore(t)
	ctx := context.Background()

	obj := &testObj{
		TypeMeta: metav1.TypeMeta{APIVersion: "test.io/v1", Kind: "TestObj"},
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test1",
			Namespace: "default",
		},
		Data: "hello",
	}

	out := &testObj{}
	err := s.Create(ctx, "/testobjs/default/test1", obj, out, 0)
	if err != nil {
		t.Fatalf("Create: %v", err)
	}

	if out.Name != "test1" {
		t.Errorf("expected name test1, got %s", out.Name)
	}
	if out.ResourceVersion == "" {
		t.Error("expected non-empty resource version")
	}
	if out.Data != "hello" {
		t.Errorf("expected data hello, got %s", out.Data)
	}

	// Create duplicate should fail.
	err = s.Create(ctx, "/testobjs/default/test1", obj, out, 0)
	if !storage.IsExist(err) {
		t.Errorf("expected key exists error, got: %v", err)
	}
}

func TestCreateRejectsNonZeroRV(t *testing.T) {
	s := setupTestStore(t)
	ctx := context.Background()

	obj := &testObj{
		TypeMeta: metav1.TypeMeta{APIVersion: "test.io/v1", Kind: "TestObj"},
		ObjectMeta: metav1.ObjectMeta{
			Name:            "rv-test",
			Namespace:       "default",
			ResourceVersion: "12345",
		},
	}

	out := &testObj{}
	err := s.Create(ctx, "/testobjs/default/rv-test", obj, out, 0)
	if err != storage.ErrResourceVersionSetOnCreate {
		t.Errorf("expected ErrResourceVersionSetOnCreate, got: %v", err)
	}
}

func TestGet(t *testing.T) {
	s := setupTestStore(t)
	ctx := context.Background()

	// Create an object first.
	obj := &testObj{
		TypeMeta: metav1.TypeMeta{APIVersion: "test.io/v1", Kind: "TestObj"},
		ObjectMeta: metav1.ObjectMeta{
			Name:      "get-test",
			Namespace: "default",
		},
		Data: "world",
	}
	created := &testObj{}
	if err := s.Create(ctx, "/testobjs/default/get-test", obj, created, 0); err != nil {
		t.Fatalf("Create: %v", err)
	}

	// Get it back.
	got := &testObj{}
	if err := s.Get(ctx, "/testobjs/default/get-test", storage.GetOptions{}, got); err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.Name != "get-test" {
		t.Errorf("expected name get-test, got %s", got.Name)
	}
	if got.Data != "world" {
		t.Errorf("expected data world, got %s", got.Data)
	}

	// Get non-existent key.
	notFound := &testObj{}
	err := s.Get(ctx, "/testobjs/default/nonexistent", storage.GetOptions{}, notFound)
	if !storage.IsNotFound(err) {
		t.Errorf("expected not found error, got: %v", err)
	}

	// Get non-existent key with IgnoreNotFound.
	notFound2 := &testObj{}
	err = s.Get(ctx, "/testobjs/default/nonexistent", storage.GetOptions{IgnoreNotFound: true}, notFound2)
	if err != nil {
		t.Errorf("expected no error with IgnoreNotFound, got: %v", err)
	}
}

func TestGetWithResourceVersion(t *testing.T) {
	s := setupTestStore(t)
	ctx := context.Background()

	obj := &testObj{
		TypeMeta: metav1.TypeMeta{APIVersion: "test.io/v1", Kind: "TestObj"},
		ObjectMeta: metav1.ObjectMeta{
			Name:      "rv-get",
			Namespace: "default",
		},
		Data: "v1",
	}
	created := &testObj{}
	if err := s.Create(ctx, "/testobjs/default/rv-get", obj, created, 0); err != nil {
		t.Fatalf("Create: %v", err)
	}
	createRV := created.ResourceVersion

	// Update the object.
	updated := &testObj{}
	if err := s.GuaranteedUpdate(ctx, "/testobjs/default/rv-get", updated, false, nil,
		func(input runtime.Object, res storage.ResponseMeta) (runtime.Object, *uint64, error) {
			existing := input.(*testObj)
			existing.Data = "v2"
			return existing, nil, nil
		}, nil); err != nil {
		t.Fatalf("GuaranteedUpdate: %v", err)
	}

	// Get at the old RV should return v1.
	got := &testObj{}
	if err := s.Get(ctx, "/testobjs/default/rv-get", storage.GetOptions{ResourceVersion: createRV}, got); err != nil {
		t.Fatalf("Get at old RV: %v", err)
	}
	if got.Data != "v1" {
		t.Errorf("expected data v1 at old RV, got %s", got.Data)
	}

	// Get without RV (strong read) should return v2.
	got2 := &testObj{}
	if err := s.Get(ctx, "/testobjs/default/rv-get", storage.GetOptions{}, got2); err != nil {
		t.Fatalf("Get strong: %v", err)
	}
	if got2.Data != "v2" {
		t.Errorf("expected data v2 on strong read, got %s", got2.Data)
	}

	// Get with RV "0" should do strong read (v2).
	got3 := &testObj{}
	if err := s.Get(ctx, "/testobjs/default/rv-get", storage.GetOptions{ResourceVersion: "0"}, got3); err != nil {
		t.Fatalf("Get RV=0: %v", err)
	}
	if got3.Data != "v2" {
		t.Errorf("expected data v2 for RV=0, got %s", got3.Data)
	}
}

func TestDeleteWithCachedObject(t *testing.T) {
	s := setupTestStore(t)
	ctx := context.Background()

	obj := &testObj{
		TypeMeta: metav1.TypeMeta{APIVersion: "test.io/v1", Kind: "TestObj"},
		ObjectMeta: metav1.ObjectMeta{
			Name:      "cached-del",
			Namespace: "default",
			UID:       "the-uid",
		},
	}
	created := &testObj{}
	if err := s.Create(ctx, "/testobjs/default/cached-del", obj, created, 0); err != nil {
		t.Fatalf("Create: %v", err)
	}

	// Delete with cachedExistingObject — should use it for precondition check.
	correctUID := types.UID("the-uid")
	deleted := &testObj{}
	err := s.Delete(ctx, "/testobjs/default/cached-del", deleted, &storage.Preconditions{UID: &correctUID},
		storage.ValidateAllObjectFunc, created, storage.DeleteOptions{})
	if err != nil {
		t.Fatalf("Delete with cached object: %v", err)
	}
	if deleted.Name != "cached-del" {
		t.Errorf("expected name cached-del, got %s", deleted.Name)
	}
}

func TestGuaranteedUpdateWithCachedObject(t *testing.T) {
	s := setupTestStore(t)
	ctx := context.Background()

	obj := &testObj{
		TypeMeta: metav1.TypeMeta{APIVersion: "test.io/v1", Kind: "TestObj"},
		ObjectMeta: metav1.ObjectMeta{
			Name:      "cached-update",
			Namespace: "default",
		},
		Data: "original",
	}
	created := &testObj{}
	if err := s.Create(ctx, "/testobjs/default/cached-update", obj, created, 0); err != nil {
		t.Fatalf("Create: %v", err)
	}

	// Update with cachedExistingObject — should use it for tryUpdate.
	dest := &testObj{}
	err := s.GuaranteedUpdate(ctx, "/testobjs/default/cached-update", dest, false, nil,
		func(input runtime.Object, res storage.ResponseMeta) (runtime.Object, *uint64, error) {
			existing := input.(*testObj)
			existing.Data = "via-cached"
			return existing, nil, nil
		}, created)
	if err != nil {
		t.Fatalf("GuaranteedUpdate with cached: %v", err)
	}
	if dest.Data != "via-cached" {
		t.Errorf("expected data via-cached, got %s", dest.Data)
	}
	if dest.ResourceVersion == created.ResourceVersion {
		t.Error("expected RV to change after update")
	}

	// Verify via Get.
	got := &testObj{}
	if err := s.Get(ctx, "/testobjs/default/cached-update", storage.GetOptions{}, got); err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.Data != "via-cached" {
		t.Errorf("expected data via-cached, got %s", got.Data)
	}
}

func TestDelete(t *testing.T) {
	s := setupTestStore(t)
	ctx := context.Background()

	obj := &testObj{
		TypeMeta: metav1.TypeMeta{APIVersion: "test.io/v1", Kind: "TestObj"},
		ObjectMeta: metav1.ObjectMeta{
			Name:      "del-test",
			Namespace: "default",
		},
		Data: "to-delete",
	}
	created := &testObj{}
	if err := s.Create(ctx, "/testobjs/default/del-test", obj, created, 0); err != nil {
		t.Fatalf("Create: %v", err)
	}

	deleted := &testObj{}
	err := s.Delete(ctx, "/testobjs/default/del-test", deleted, nil, storage.ValidateAllObjectFunc, nil, storage.DeleteOptions{})
	if err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if deleted.Name != "del-test" {
		t.Errorf("expected deleted object name del-test, got %s", deleted.Name)
	}
	if deleted.ResourceVersion == "" {
		t.Error("expected non-empty resource version on deleted object")
	}

	// Verify it's gone.
	got := &testObj{}
	err = s.Get(ctx, "/testobjs/default/del-test", storage.GetOptions{}, got)
	if !storage.IsNotFound(err) {
		t.Errorf("expected not found after delete, got: %v", err)
	}
}

func TestDeleteNotFound(t *testing.T) {
	s := setupTestStore(t)
	ctx := context.Background()

	deleted := &testObj{}
	err := s.Delete(ctx, "/testobjs/default/nonexistent", deleted, nil, storage.ValidateAllObjectFunc, nil, storage.DeleteOptions{})
	if !storage.IsNotFound(err) {
		t.Errorf("expected not found error, got: %v", err)
	}
}

func TestDeletePreconditions(t *testing.T) {
	s := setupTestStore(t)
	ctx := context.Background()

	obj := &testObj{
		TypeMeta: metav1.TypeMeta{APIVersion: "test.io/v1", Kind: "TestObj"},
		ObjectMeta: metav1.ObjectMeta{
			Name:      "del-precond",
			Namespace: "default",
			UID:       "correct-uid",
		},
	}
	created := &testObj{}
	if err := s.Create(ctx, "/testobjs/default/del-precond", obj, created, 0); err != nil {
		t.Fatalf("Create: %v", err)
	}

	// Wrong UID should fail.
	wrongUID := types.UID("wrong-uid")
	deleted := &testObj{}
	err := s.Delete(ctx, "/testobjs/default/del-precond", deleted, &storage.Preconditions{UID: &wrongUID}, storage.ValidateAllObjectFunc, nil, storage.DeleteOptions{})
	if err == nil {
		t.Fatal("expected precondition error for wrong UID")
	}

	// Correct UID should succeed.
	correctUID := types.UID("correct-uid")
	deleted = &testObj{}
	err = s.Delete(ctx, "/testobjs/default/del-precond", deleted, &storage.Preconditions{UID: &correctUID}, storage.ValidateAllObjectFunc, nil, storage.DeleteOptions{})
	if err != nil {
		t.Fatalf("Delete with correct UID: %v", err)
	}
}

func TestGuaranteedUpdate(t *testing.T) {
	s := setupTestStore(t)
	ctx := context.Background()

	obj := &testObj{
		TypeMeta: metav1.TypeMeta{APIVersion: "test.io/v1", Kind: "TestObj"},
		ObjectMeta: metav1.ObjectMeta{
			Name:      "update-test",
			Namespace: "default",
		},
		Data: "original",
	}
	created := &testObj{}
	if err := s.Create(ctx, "/testobjs/default/update-test", obj, created, 0); err != nil {
		t.Fatalf("Create: %v", err)
	}

	updated := &testObj{}
	err := s.GuaranteedUpdate(ctx, "/testobjs/default/update-test", updated, false, nil,
		func(input runtime.Object, res storage.ResponseMeta) (runtime.Object, *uint64, error) {
			existing := input.(*testObj)
			existing.Data = "updated"
			return existing, nil, nil
		}, nil)
	if err != nil {
		t.Fatalf("GuaranteedUpdate: %v", err)
	}
	if updated.Data != "updated" {
		t.Errorf("expected data updated, got %s", updated.Data)
	}
	if updated.ResourceVersion == "" {
		t.Error("expected non-empty resource version after update")
	}
	if updated.ResourceVersion == created.ResourceVersion {
		t.Error("expected resource version to change after update")
	}
}

func TestGuaranteedUpdateNotFoundIgnored(t *testing.T) {
	s := setupTestStore(t)
	ctx := context.Background()

	// ignoreNotFound=true should create the object.
	dest := &testObj{}
	err := s.GuaranteedUpdate(ctx, "/testobjs/default/upsert-test", dest, true, nil,
		func(input runtime.Object, res storage.ResponseMeta) (runtime.Object, *uint64, error) {
			obj := input.(*testObj)
			obj.TypeMeta = metav1.TypeMeta{APIVersion: "test.io/v1", Kind: "TestObj"}
			obj.Name = "upsert-test"
			obj.Namespace = "default"
			obj.Data = "created-via-update"
			return obj, nil, nil
		}, nil)
	if err != nil {
		t.Fatalf("GuaranteedUpdate (upsert): %v", err)
	}
	if dest.Data != "created-via-update" {
		t.Errorf("expected data created-via-update, got %s", dest.Data)
	}
	if dest.ResourceVersion == "" {
		t.Error("expected non-empty resource version")
	}

	// Verify it exists via Get.
	got := &testObj{}
	if err := s.Get(ctx, "/testobjs/default/upsert-test", storage.GetOptions{}, got); err != nil {
		t.Fatalf("Get after upsert: %v", err)
	}
	if got.Data != "created-via-update" {
		t.Errorf("expected data created-via-update, got %s", got.Data)
	}
}

func TestGuaranteedUpdateNotFoundError(t *testing.T) {
	s := setupTestStore(t)
	ctx := context.Background()

	// ignoreNotFound=false should return not found.
	dest := &testObj{}
	err := s.GuaranteedUpdate(ctx, "/testobjs/default/no-exist", dest, false, nil,
		func(input runtime.Object, res storage.ResponseMeta) (runtime.Object, *uint64, error) {
			return input, nil, nil
		}, nil)
	if !storage.IsNotFound(err) {
		t.Errorf("expected not found error, got: %v", err)
	}
}

func TestGuaranteedUpdateNoOp(t *testing.T) {
	s := setupTestStore(t)
	ctx := context.Background()

	obj := &testObj{
		TypeMeta: metav1.TypeMeta{APIVersion: "test.io/v1", Kind: "TestObj"},
		ObjectMeta: metav1.ObjectMeta{
			Name:      "noop-test",
			Namespace: "default",
		},
		Data: "unchanged",
	}
	created := &testObj{}
	if err := s.Create(ctx, "/testobjs/default/noop-test", obj, created, 0); err != nil {
		t.Fatalf("Create: %v", err)
	}

	// Start a watcher to verify no event is emitted.
	w, err := s.Watch(ctx, "/testobjs/", storage.ListOptions{
		Predicate: storage.Everything,
		Recursive: true,
	})
	if err != nil {
		t.Fatalf("Watch: %v", err)
	}
	defer w.Stop()
	time.Sleep(50 * time.Millisecond)

	// Update with no actual change.
	dest := &testObj{}
	err = s.GuaranteedUpdate(ctx, "/testobjs/default/noop-test", dest, false, nil,
		func(input runtime.Object, res storage.ResponseMeta) (runtime.Object, *uint64, error) {
			// Return unchanged object.
			return input, nil, nil
		}, nil)
	if err != nil {
		t.Fatalf("GuaranteedUpdate (no-op): %v", err)
	}

	// RV should stay the same.
	if dest.ResourceVersion != created.ResourceVersion {
		t.Errorf("expected resource version %s to stay unchanged, got %s", created.ResourceVersion, dest.ResourceVersion)
	}

	// No watch event should arrive.
	select {
	case event := <-w.ResultChan():
		t.Errorf("expected no watch event for no-op, got %s", event.Type)
	case <-time.After(200 * time.Millisecond):
		// Good — no event.
	}
}

func TestGuaranteedUpdatePreconditions(t *testing.T) {
	s := setupTestStore(t)
	ctx := context.Background()

	obj := &testObj{
		TypeMeta: metav1.TypeMeta{APIVersion: "test.io/v1", Kind: "TestObj"},
		ObjectMeta: metav1.ObjectMeta{
			Name:      "precond-test",
			Namespace: "default",
			UID:       "the-uid",
		},
	}
	created := &testObj{}
	if err := s.Create(ctx, "/testobjs/default/precond-test", obj, created, 0); err != nil {
		t.Fatalf("Create: %v", err)
	}

	// Wrong UID precondition should fail.
	wrongUID := types.UID("wrong-uid")
	dest := &testObj{}
	err := s.GuaranteedUpdate(ctx, "/testobjs/default/precond-test", dest, false,
		&storage.Preconditions{UID: &wrongUID},
		func(input runtime.Object, res storage.ResponseMeta) (runtime.Object, *uint64, error) {
			return input, nil, nil
		}, nil)
	if err == nil {
		t.Fatal("expected precondition error for wrong UID")
	}
}

func TestGuaranteedUpdateConflictRetry(t *testing.T) {
	s := setupTestStore(t)
	ctx := context.Background()

	obj := &testObj{
		TypeMeta: metav1.TypeMeta{APIVersion: "test.io/v1", Kind: "TestObj"},
		ObjectMeta: metav1.ObjectMeta{
			Name:      "conflict-test",
			Namespace: "default",
		},
		Data: "v1",
	}
	created := &testObj{}
	if err := s.Create(ctx, "/testobjs/default/conflict-test", obj, created, 0); err != nil {
		t.Fatalf("Create: %v", err)
	}

	// tryUpdate that fails with conflict on the first call, succeeds on retry.
	callCount := 0
	dest := &testObj{}
	err := s.GuaranteedUpdate(ctx, "/testobjs/default/conflict-test", dest, false, nil,
		func(input runtime.Object, res storage.ResponseMeta) (runtime.Object, *uint64, error) {
			callCount++
			existing := input.(*testObj)
			if callCount == 1 {
				return nil, nil, apierrors.NewConflict(
					schema.GroupResource{Group: "test.io", Resource: "testobjs"},
					"conflict-test",
					fmt.Errorf("simulated conflict"),
				)
			}
			existing.Data = "v2-after-retry"
			return existing, nil, nil
		}, nil)
	if err != nil {
		t.Fatalf("GuaranteedUpdate: %v (callCount=%d)", err, callCount)
	}
	if callCount < 2 {
		t.Errorf("expected tryUpdate to be called at least 2 times, got %d", callCount)
	}
	if dest.Data != "v2-after-retry" {
		t.Errorf("expected data v2-after-retry, got %s", dest.Data)
	}
}

func TestGetList(t *testing.T) {
	s := setupTestStore(t)
	ctx := context.Background()

	// Create a few objects.
	for i := 0; i < 3; i++ {
		obj := &testObj{
			TypeMeta: metav1.TypeMeta{APIVersion: "test.io/v1", Kind: "TestObj"},
			ObjectMeta: metav1.ObjectMeta{
				Name:      fmt.Sprintf("list-%d", i),
				Namespace: "default",
			},
			Data: fmt.Sprintf("item-%d", i),
		}
		out := &testObj{}
		if err := s.Create(ctx, fmt.Sprintf("/testobjs/default/list-%d", i), obj, out, 0); err != nil {
			t.Fatalf("Create %d: %v", i, err)
		}
	}

	// List all.
	listObj := &testObjList{}
	err := s.GetList(ctx, "/testobjs/", storage.ListOptions{
		Predicate: storage.Everything,
		Recursive: true,
	}, listObj)
	if err != nil {
		t.Fatalf("GetList: %v", err)
	}
	if len(listObj.Items) != 3 {
		t.Errorf("expected 3 items, got %d", len(listObj.Items))
	}
	if listObj.ResourceVersion == "" {
		t.Error("expected non-empty list resource version")
	}
}

func TestWatch(t *testing.T) {
	s := setupTestStore(t)
	ctx := context.Background()

	// Start watching.
	w, err := s.Watch(ctx, "/testobjs/", storage.ListOptions{
		Predicate: storage.Everything,
		Recursive: true,
	})
	if err != nil {
		t.Fatalf("Watch: %v", err)
	}
	defer w.Stop()

	// Give the watcher time to start.
	time.Sleep(50 * time.Millisecond)

	// Create an object.
	obj := &testObj{
		TypeMeta: metav1.TypeMeta{APIVersion: "test.io/v1", Kind: "TestObj"},
		ObjectMeta: metav1.ObjectMeta{
			Name:      "watch-test",
			Namespace: "default",
		},
		Data: "watched",
	}
	out := &testObj{}
	if err := s.Create(ctx, "/testobjs/default/watch-test", obj, out, 0); err != nil {
		t.Fatalf("Create: %v", err)
	}

	// Read the watch event.
	select {
	case event := <-w.ResultChan():
		if event.Type != watch.Added {
			t.Errorf("expected Added event, got %s", event.Type)
		}
		watchedObj, ok := event.Object.(*testObj)
		if !ok {
			t.Fatalf("expected *testObj, got %T", event.Object)
		}
		if watchedObj.Name != "watch-test" {
			t.Errorf("expected name watch-test, got %s", watchedObj.Name)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for watch event")
	}
}

func TestStats(t *testing.T) {
	s := setupTestStore(t)
	ctx := context.Background()

	// Create an object.
	obj := &testObj{
		TypeMeta: metav1.TypeMeta{APIVersion: "test.io/v1", Kind: "TestObj"},
		ObjectMeta: metav1.ObjectMeta{
			Name:      "stats-test",
			Namespace: "default",
		},
	}
	out := &testObj{}
	if err := s.Create(ctx, "/testobjs/default/stats-test", obj, out, 0); err != nil {
		t.Fatalf("Create: %v", err)
	}

	stats, err := s.Stats(ctx)
	if err != nil {
		t.Fatalf("Stats: %v", err)
	}
	if stats.ObjectCount < 1 {
		t.Errorf("expected at least 1 object, got %d", stats.ObjectCount)
	}
}

func TestGetCurrentResourceVersion(t *testing.T) {
	s := setupTestStore(t)
	ctx := context.Background()

	rv, err := s.GetCurrentResourceVersion(ctx)
	if err != nil {
		t.Fatalf("GetCurrentResourceVersion: %v", err)
	}
	if rv == 0 {
		t.Error("expected non-zero resource version")
	}
}

func TestReadinessCheck(t *testing.T) {
	s := setupTestStore(t)
	if err := s.ReadinessCheck(); err != nil {
		t.Fatalf("ReadinessCheck: %v", err)
	}
}

func TestDecodeCallback(t *testing.T) {
	s := setupTestStore(t)
	ctx := context.Background()

	// Create objects.
	for i := 0; i < 2; i++ {
		obj := &testObj{
			TypeMeta: metav1.TypeMeta{APIVersion: "test.io/v1", Kind: "TestObj"},
			ObjectMeta: metav1.ObjectMeta{
				Name:      fmt.Sprintf("cb-%d", i),
				Namespace: "default",
			},
		}
		out := &testObj{}
		if err := s.Create(ctx, fmt.Sprintf("/testobjs/default/cb-%d", i), obj, out, 0); err != nil {
			t.Fatalf("Create %d: %v", i, err)
		}
	}

	// List with decode callback.
	var callbackKeys []string
	ctx = storage.WithDecodeCallback(ctx, func(obj runtime.Object, storageKey string, modRevision int64) {
		callbackKeys = append(callbackKeys, storageKey)
	})

	listObj := &testObjList{}
	err := s.GetList(ctx, "/testobjs/", storage.ListOptions{
		Predicate: storage.Everything,
		Recursive: true,
	}, listObj)
	if err != nil {
		t.Fatalf("GetList: %v", err)
	}

	if len(callbackKeys) != 2 {
		t.Errorf("expected 2 callback calls, got %d: %v", len(callbackKeys), callbackKeys)
	}
}

func TestRequestWatchProgress(t *testing.T) {
	s := setupTestStore(t)
	ctx := context.Background()

	// Start watching with progress notifications.
	w, err := s.Watch(ctx, "/testobjs/", storage.ListOptions{
		Predicate:      storage.Everything,
		Recursive:      true,
		ProgressNotify: true,
	})
	if err != nil {
		t.Fatalf("Watch: %v", err)
	}
	defer w.Stop()

	time.Sleep(50 * time.Millisecond)

	// Request progress.
	if err := s.RequestWatchProgress(ctx); err != nil {
		t.Fatalf("RequestWatchProgress: %v", err)
	}

	// Should receive a bookmark event.
	select {
	case event := <-w.ResultChan():
		if event.Type != watch.Bookmark {
			t.Errorf("expected Bookmark event, got %s", event.Type)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for progress event")
	}
}

func TestResourceVersionMonotonicity(t *testing.T) {
	s := setupTestStore(t)
	ctx := context.Background()

	var rvs []uint64

	// Create multiple objects and track RVs.
	for i := 0; i < 5; i++ {
		obj := &testObj{
			TypeMeta: metav1.TypeMeta{APIVersion: "test.io/v1", Kind: "TestObj"},
			ObjectMeta: metav1.ObjectMeta{
				Name:      fmt.Sprintf("mono-%d", i),
				Namespace: "default",
			},
			Data: fmt.Sprintf("data-%d", i),
		}
		out := &testObj{}
		if err := s.Create(ctx, fmt.Sprintf("/testobjs/default/mono-%d", i), obj, out, 0); err != nil {
			t.Fatalf("Create %d: %v", i, err)
		}
		parsed, err := s.versioner.ParseResourceVersion(out.ResourceVersion)
		if err != nil {
			t.Fatalf("parse RV: %v", err)
		}
		rvs = append(rvs, parsed)
	}

	// Now update to generate more RVs.
	for i := 0; i < 3; i++ {
		updated := &testObj{}
		err := s.GuaranteedUpdate(ctx, "/testobjs/default/mono-0", updated, false, nil,
			func(input runtime.Object, res storage.ResponseMeta) (runtime.Object, *uint64, error) {
				existing := input.(*testObj)
				existing.Data = fmt.Sprintf("updated-%d", i)
				return existing, nil, nil
			}, nil)
		if err != nil {
			t.Fatalf("GuaranteedUpdate %d: %v", i, err)
		}
		parsed, err := s.versioner.ParseResourceVersion(updated.ResourceVersion)
		if err != nil {
			t.Fatalf("parse RV: %v", err)
		}
		rvs = append(rvs, parsed)
	}

	// Verify strict monotonicity.
	for i := 1; i < len(rvs); i++ {
		if rvs[i] <= rvs[i-1] {
			t.Errorf("resource versions not strictly monotonic: rv[%d]=%d <= rv[%d]=%d", i, rvs[i], i-1, rvs[i-1])
		}
	}
}

func TestGetListPagination(t *testing.T) {
	s := setupTestStore(t)
	ctx := context.Background()

	// Create 5 objects.
	for i := 0; i < 5; i++ {
		obj := &testObj{
			TypeMeta: metav1.TypeMeta{APIVersion: "test.io/v1", Kind: "TestObj"},
			ObjectMeta: metav1.ObjectMeta{
				Name:      fmt.Sprintf("page-%d", i),
				Namespace: "default",
			},
		}
		out := &testObj{}
		if err := s.Create(ctx, fmt.Sprintf("/testobjs/default/page-%d", i), obj, out, 0); err != nil {
			t.Fatalf("Create %d: %v", i, err)
		}
	}

	// Page 1: limit 2.
	listObj := &testObjList{}
	pred := storage.Everything
	pred.Limit = 2
	err := s.GetList(ctx, "/testobjs/", storage.ListOptions{
		Predicate: pred,
		Recursive: true,
	}, listObj)
	if err != nil {
		t.Fatalf("GetList page 1: %v", err)
	}
	if len(listObj.Items) != 2 {
		t.Fatalf("expected 2 items on page 1, got %d", len(listObj.Items))
	}
	if listObj.Continue == "" {
		t.Fatal("expected non-empty continue token")
	}

	// Page 2: use continue token (RV is encoded in the token, not passed separately).
	listObj2 := &testObjList{}
	pred2 := storage.Everything
	pred2.Limit = 2
	pred2.Continue = listObj.Continue
	err = s.GetList(ctx, "/testobjs/", storage.ListOptions{
		Predicate: pred2,
		Recursive: true,
	}, listObj2)
	if err != nil {
		t.Fatalf("GetList page 2: %v", err)
	}
	if len(listObj2.Items) != 2 {
		t.Fatalf("expected 2 items on page 2, got %d", len(listObj2.Items))
	}

	// Collect all names to verify no duplicates.
	seen := map[string]bool{}
	for _, item := range listObj.Items {
		if seen[item.Name] {
			t.Errorf("duplicate name in pages: %s", item.Name)
		}
		seen[item.Name] = true
	}
	for _, item := range listObj2.Items {
		if seen[item.Name] {
			t.Errorf("duplicate name across pages: %s", item.Name)
		}
		seen[item.Name] = true
	}
}

func TestWatchSendInitialEvents(t *testing.T) {
	s := setupTestStore(t)
	ctx := context.Background()

	// Create objects before starting the watch.
	for i := 0; i < 3; i++ {
		obj := &testObj{
			TypeMeta: metav1.TypeMeta{APIVersion: "test.io/v1", Kind: "TestObj"},
			ObjectMeta: metav1.ObjectMeta{
				Name:      fmt.Sprintf("init-%d", i),
				Namespace: "default",
			},
		}
		out := &testObj{}
		if err := s.Create(ctx, fmt.Sprintf("/testobjs/default/init-%d", i), obj, out, 0); err != nil {
			t.Fatalf("Create %d: %v", i, err)
		}
	}

	// Watch with SendInitialEvents.
	sendInitial := true
	pred := storage.Everything
	pred.AllowWatchBookmarks = true
	w, err := s.Watch(ctx, "/testobjs/", storage.ListOptions{
		Predicate:         pred,
		Recursive:         true,
		SendInitialEvents: &sendInitial,
	})
	if err != nil {
		t.Fatalf("Watch: %v", err)
	}
	defer w.Stop()

	// Should receive 3 Added events + 1 Bookmark.
	addedCount := 0
	bookmarkCount := 0
	timeout := time.After(5 * time.Second)
	for addedCount < 3 || bookmarkCount < 1 {
		select {
		case event := <-w.ResultChan():
			switch event.Type {
			case watch.Added:
				addedCount++
			case watch.Bookmark:
				bookmarkCount++
			default:
				t.Errorf("unexpected event type: %s", event.Type)
			}
		case <-timeout:
			t.Fatalf("timed out: got %d added, %d bookmarks", addedCount, bookmarkCount)
		}
	}
	if addedCount != 3 {
		t.Errorf("expected 3 Added events, got %d", addedCount)
	}
}

func TestWatchNonRecursive(t *testing.T) {
	s := setupTestStore(t)
	ctx := context.Background()

	// Watch a specific key (non-recursive).
	w, err := s.Watch(ctx, "/testobjs/default/specific", storage.ListOptions{
		Predicate: storage.Everything,
		Recursive: false,
	})
	if err != nil {
		t.Fatalf("Watch: %v", err)
	}
	defer w.Stop()
	time.Sleep(50 * time.Millisecond)

	// Create an object at a different key — should NOT trigger.
	other := &testObj{
		TypeMeta:   metav1.TypeMeta{APIVersion: "test.io/v1", Kind: "TestObj"},
		ObjectMeta: metav1.ObjectMeta{Name: "other", Namespace: "default"},
	}
	out := &testObj{}
	if err := s.Create(ctx, "/testobjs/default/other", other, out, 0); err != nil {
		t.Fatalf("Create other: %v", err)
	}

	// Create the watched object — should trigger.
	specific := &testObj{
		TypeMeta:   metav1.TypeMeta{APIVersion: "test.io/v1", Kind: "TestObj"},
		ObjectMeta: metav1.ObjectMeta{Name: "specific", Namespace: "default"},
	}
	if err := s.Create(ctx, "/testobjs/default/specific", specific, out, 0); err != nil {
		t.Fatalf("Create specific: %v", err)
	}

	// Should receive only the specific key event.
	select {
	case event := <-w.ResultChan():
		if event.Type != watch.Added {
			t.Errorf("expected Added, got %s", event.Type)
		}
		obj := event.Object.(*testObj)
		if obj.Name != "specific" {
			t.Errorf("expected name specific, got %s", obj.Name)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for event")
	}

	// No other event should arrive.
	select {
	case event := <-w.ResultChan():
		t.Errorf("unexpected extra event: %s %v", event.Type, event.Object)
	case <-time.After(200 * time.Millisecond):
		// Good.
	}
}

func TestDeleteReturnsRV(t *testing.T) {
	s := setupTestStore(t)
	ctx := context.Background()

	obj := &testObj{
		TypeMeta: metav1.TypeMeta{APIVersion: "test.io/v1", Kind: "TestObj"},
		ObjectMeta: metav1.ObjectMeta{
			Name:      "del-rv",
			Namespace: "default",
		},
	}
	created := &testObj{}
	if err := s.Create(ctx, "/testobjs/default/del-rv", obj, created, 0); err != nil {
		t.Fatalf("Create: %v", err)
	}

	deleted := &testObj{}
	if err := s.Delete(ctx, "/testobjs/default/del-rv", deleted, nil, storage.ValidateAllObjectFunc, nil, storage.DeleteOptions{}); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	// Deleted object must have an RV, and it must be >= the create RV (delete happens after create).
	if deleted.ResourceVersion == "" {
		t.Fatal("expected non-empty resource version on deleted object")
	}
	createRV, _ := s.versioner.ParseResourceVersion(created.ResourceVersion)
	deleteRV, _ := s.versioner.ParseResourceVersion(deleted.ResourceVersion)
	if deleteRV < createRV {
		t.Errorf("delete RV (%d) should be >= create RV (%d)", deleteRV, createRV)
	}
}

func TestWatchDeleteHasObject(t *testing.T) {
	s := setupTestStore(t)
	ctx := context.Background()

	obj := &testObj{
		TypeMeta: metav1.TypeMeta{APIVersion: "test.io/v1", Kind: "TestObj"},
		ObjectMeta: metav1.ObjectMeta{
			Name:      "watch-del-obj",
			Namespace: "default",
		},
		Data: "before-delete",
	}
	out := &testObj{}
	if err := s.Create(ctx, "/testobjs/default/watch-del-obj", obj, out, 0); err != nil {
		t.Fatalf("Create: %v", err)
	}

	w, err := s.Watch(ctx, "/testobjs/", storage.ListOptions{
		Predicate: storage.Everything,
		Recursive: true,
	})
	if err != nil {
		t.Fatalf("Watch: %v", err)
	}
	defer w.Stop()
	time.Sleep(50 * time.Millisecond)

	deleted := &testObj{}
	if err := s.Delete(ctx, "/testobjs/default/watch-del-obj", deleted, nil, storage.ValidateAllObjectFunc, nil, storage.DeleteOptions{}); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	// The delete watch event should contain the previous object state.
	select {
	case event := <-w.ResultChan():
		if event.Type != watch.Deleted {
			t.Fatalf("expected Deleted, got %s", event.Type)
		}
		delObj := event.Object.(*testObj)
		if delObj.Name != "watch-del-obj" {
			t.Errorf("expected name watch-del-obj, got %s", delObj.Name)
		}
		if delObj.Data != "before-delete" {
			t.Errorf("expected data before-delete, got %s", delObj.Data)
		}
		if delObj.ResourceVersion == "" {
			t.Error("expected non-empty RV on watched delete event object")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for delete event")
	}
}

func TestWatchUpsertIsAdded(t *testing.T) {
	s := setupTestStore(t)
	ctx := context.Background()

	// Watch before the upsert.
	w, err := s.Watch(ctx, "/testobjs/", storage.ListOptions{
		Predicate: storage.Everything,
		Recursive: true,
	})
	if err != nil {
		t.Fatalf("Watch: %v", err)
	}
	defer w.Stop()
	time.Sleep(50 * time.Millisecond)

	// GuaranteedUpdate with ignoreNotFound=true creates a new object.
	dest := &testObj{}
	err = s.GuaranteedUpdate(ctx, "/testobjs/default/upsert-watch", dest, true, nil,
		func(input runtime.Object, res storage.ResponseMeta) (runtime.Object, *uint64, error) {
			obj := input.(*testObj)
			obj.TypeMeta = metav1.TypeMeta{APIVersion: "test.io/v1", Kind: "TestObj"}
			obj.Name = "upsert-watch"
			obj.Namespace = "default"
			obj.Data = "created"
			return obj, nil, nil
		}, nil)
	if err != nil {
		t.Fatalf("GuaranteedUpdate (upsert): %v", err)
	}

	// The watch event should be Added (not Modified).
	select {
	case event := <-w.ResultChan():
		if event.Type != watch.Added {
			t.Errorf("expected Added event for upsert, got %s", event.Type)
		}
		obj := event.Object.(*testObj)
		if obj.Data != "created" {
			t.Errorf("expected data created, got %s", obj.Data)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for upsert watch event")
	}
}

func TestSendInitialEventsNoDuplicates(t *testing.T) {
	s := setupTestStore(t)
	ctx := context.Background()

	// Create objects before watching.
	for i := 0; i < 3; i++ {
		obj := &testObj{
			TypeMeta: metav1.TypeMeta{APIVersion: "test.io/v1", Kind: "TestObj"},
			ObjectMeta: metav1.ObjectMeta{
				Name:      fmt.Sprintf("dedup-%d", i),
				Namespace: "default",
			},
		}
		out := &testObj{}
		if err := s.Create(ctx, fmt.Sprintf("/testobjs/default/dedup-%d", i), obj, out, 0); err != nil {
			t.Fatalf("Create %d: %v", i, err)
		}
	}

	// Watch with SendInitialEvents.
	sendInitial := true
	pred := storage.Everything
	pred.AllowWatchBookmarks = true
	w, err := s.Watch(ctx, "/testobjs/", storage.ListOptions{
		Predicate:         pred,
		Recursive:         true,
		SendInitialEvents: &sendInitial,
	})
	if err != nil {
		t.Fatalf("Watch: %v", err)
	}
	defer w.Stop()

	// Collect initial events: 3 Added + 1 Bookmark.
	addedNames := map[string]int{}
	timeout := time.After(5 * time.Second)
	for i := 0; i < 4; i++ {
		select {
		case event := <-w.ResultChan():
			switch event.Type {
			case watch.Added:
				obj := event.Object.(*testObj)
				addedNames[obj.Name]++
			case watch.Bookmark:
				// expected
			}
		case <-timeout:
			t.Fatalf("timed out after %d events", i)
		}
	}

	// Verify no duplicates.
	for name, count := range addedNames {
		if count > 1 {
			t.Errorf("duplicate Added event for %s: %d times", name, count)
		}
	}

	// No extra events should arrive (no duplicates from broadcaster).
	select {
	case event := <-w.ResultChan():
		t.Errorf("unexpected extra event after initial: %s %T", event.Type, event.Object)
	case <-time.After(300 * time.Millisecond):
		// Good — no duplicates.
	}
}

func TestWatchDelete(t *testing.T) {
	s := setupTestStore(t)
	ctx := context.Background()

	// Create object first.
	obj := &testObj{
		TypeMeta: metav1.TypeMeta{APIVersion: "test.io/v1", Kind: "TestObj"},
		ObjectMeta: metav1.ObjectMeta{
			Name:      "watch-del",
			Namespace: "default",
		},
		Data: "will-delete",
	}
	out := &testObj{}
	if err := s.Create(ctx, "/testobjs/default/watch-del", obj, out, 0); err != nil {
		t.Fatalf("Create: %v", err)
	}

	// Start watching.
	w, err := s.Watch(ctx, "/testobjs/", storage.ListOptions{
		Predicate: storage.Everything,
		Recursive: true,
	})
	if err != nil {
		t.Fatalf("Watch: %v", err)
	}
	defer w.Stop()

	time.Sleep(50 * time.Millisecond)

	// Delete the object.
	deleted := &testObj{}
	if err := s.Delete(ctx, "/testobjs/default/watch-del", deleted, nil, storage.ValidateAllObjectFunc, nil, storage.DeleteOptions{}); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	// Should receive a Deleted event.
	select {
	case event := <-w.ResultChan():
		if event.Type != watch.Deleted {
			t.Errorf("expected Deleted event, got %s", event.Type)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for delete event")
	}
}

func TestWatchModify(t *testing.T) {
	s := setupTestStore(t)
	ctx := context.Background()

	// Create object.
	obj := &testObj{
		TypeMeta: metav1.TypeMeta{APIVersion: "test.io/v1", Kind: "TestObj"},
		ObjectMeta: metav1.ObjectMeta{
			Name:      "watch-mod",
			Namespace: "default",
		},
		Data: "original",
	}
	out := &testObj{}
	if err := s.Create(ctx, "/testobjs/default/watch-mod", obj, out, 0); err != nil {
		t.Fatalf("Create: %v", err)
	}

	// Start watching.
	w, err := s.Watch(ctx, "/testobjs/", storage.ListOptions{
		Predicate: storage.Everything,
		Recursive: true,
	})
	if err != nil {
		t.Fatalf("Watch: %v", err)
	}
	defer w.Stop()

	time.Sleep(50 * time.Millisecond)

	// Update the object.
	updated := &testObj{}
	if err := s.GuaranteedUpdate(ctx, "/testobjs/default/watch-mod", updated, false, nil,
		func(input runtime.Object, res storage.ResponseMeta) (runtime.Object, *uint64, error) {
			existing := input.(*testObj)
			existing.Data = "modified"
			return existing, nil, nil
		}, nil); err != nil {
		t.Fatalf("GuaranteedUpdate: %v", err)
	}

	// Should receive a Modified event.
	select {
	case event := <-w.ResultChan():
		if event.Type != watch.Modified {
			t.Errorf("expected Modified event, got %s", event.Type)
		}
		modObj, ok := event.Object.(*testObj)
		if !ok {
			t.Fatalf("expected *testObj, got %T", event.Object)
		}
		if modObj.Data != "modified" {
			t.Errorf("expected data modified, got %s", modObj.Data)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for modified event")
	}
}

func TestPrepareKeyValidation(t *testing.T) {
	s := setupTestStore(t)
	ctx := context.Background()

	tests := []struct {
		name string
		key  string
	}{
		{"path traversal with ..", "../etc/passwd"},
		{"path traversal mid-key", "/testobjs/../../secrets"},
		{"path traversal suffix", "/testobjs/default/.."},
		{"dot traversal", "./something"},
		{"dot mid-key", "/testobjs/./default"},
		{"dot suffix", "/testobjs/default/."},
		{"empty key", ""},
		{"slash only", "/"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			obj := &testObj{
				TypeMeta:   metav1.TypeMeta{APIVersion: "test.io/v1", Kind: "TestObj"},
				ObjectMeta: metav1.ObjectMeta{Name: "bad", Namespace: "default"},
			}
			out := &testObj{}
			err := s.Create(ctx, tc.key, obj, out, 0)
			if err == nil {
				t.Errorf("expected error for key %q, got nil", tc.key)
			}

			err = s.Get(ctx, tc.key, storage.GetOptions{}, out)
			if err == nil {
				t.Errorf("expected error for Get with key %q, got nil", tc.key)
			}

			err = s.Delete(ctx, tc.key, out, nil, func(_ context.Context, _ runtime.Object) error { return nil }, nil, storage.DeleteOptions{})
			if err == nil {
				t.Errorf("expected error for Delete with key %q, got nil", tc.key)
			}

			err = s.GuaranteedUpdate(ctx, tc.key, out, false, nil,
				func(input runtime.Object, res storage.ResponseMeta) (runtime.Object, *uint64, error) {
					return input, nil, nil
				}, nil)
			if err == nil {
				t.Errorf("expected error for GuaranteedUpdate with key %q, got nil", tc.key)
			}
		})
	}
}

func TestGetListContextCancellation(t *testing.T) {
	s := setupTestStore(t)
	ctx := context.Background()

	// Create several objects so the list has items to iterate.
	for i := 0; i < 10; i++ {
		obj := &testObj{
			TypeMeta:   metav1.TypeMeta{APIVersion: "test.io/v1", Kind: "TestObj"},
			ObjectMeta: metav1.ObjectMeta{Name: fmt.Sprintf("item-%d", i), Namespace: "default"},
			Data:       fmt.Sprintf("data-%d", i),
		}
		out := &testObj{}
		if err := s.Create(ctx, fmt.Sprintf("/testobjs/default/item-%d", i), obj, out, 0); err != nil {
			t.Fatalf("Create: %v", err)
		}
	}

	// Cancel the context before listing.
	cancelCtx, cancel := context.WithCancel(ctx)
	cancel() // immediately cancelled

	listObj := &testObjList{}
	err := s.GetList(cancelCtx, "/testobjs/", storage.ListOptions{
		Recursive: true,
		Predicate: storage.SelectionPredicate{},
	}, listObj)
	if err == nil {
		t.Fatal("expected error from cancelled context, got nil")
	}
}

// testObjGetAttrs returns labels, fields, and an error for a testObj.
// Required for SelectionPredicate.Matches() to work.
func testObjGetAttrs(obj runtime.Object) (labels.Set, fields.Set, error) {
	t, ok := obj.(*testObj)
	if !ok {
		return nil, nil, fmt.Errorf("not a testObj: %T", obj)
	}
	return t.Labels, fields.Set{"metadata.name": t.Name}, nil
}

func TestWatchPredicateFilterTransitions(t *testing.T) {
	s := setupTestStore(t)
	ctx := context.Background()

	// Create an object that matches the predicate (has label "color=blue").
	obj := &testObj{
		TypeMeta: metav1.TypeMeta{APIVersion: "test.io/v1", Kind: "TestObj"},
		ObjectMeta: metav1.ObjectMeta{
			Name:      "filter-test",
			Namespace: "default",
			Labels:    map[string]string{"color": "blue"},
		},
		Data: "initial",
	}
	out := &testObj{}
	if err := s.Create(ctx, "/testobjs/default/filter-test", obj, out, 0); err != nil {
		t.Fatalf("Create: %v", err)
	}
	createdRV := out.ResourceVersion

	// Start a watch with a label predicate selecting color=blue.
	pred := storage.SelectionPredicate{
		Label:    labels.SelectorFromSet(labels.Set{"color": "blue"}),
		Field:    fields.Everything(),
		GetAttrs: testObjGetAttrs,
	}
	w, err := s.Watch(ctx, "/testobjs/", storage.ListOptions{
		Recursive:       true,
		Predicate:       pred,
		ResourceVersion: createdRV,
	})
	if err != nil {
		t.Fatalf("Watch: %v", err)
	}
	defer w.Stop()
	time.Sleep(50 * time.Millisecond)

	// --- Test 1: Object leaves predicate window (blue → red) ---
	// Should produce a synthetic Deleted event with the OLD object.
	updated := &testObj{}
	if err := s.GuaranteedUpdate(ctx, "/testobjs/default/filter-test", updated, false, nil,
		func(input runtime.Object, res storage.ResponseMeta) (runtime.Object, *uint64, error) {
			existing := input.(*testObj)
			existing.Labels = map[string]string{"color": "red"}
			existing.Data = "now-red"
			return existing, nil, nil
		}, nil); err != nil {
		t.Fatalf("GuaranteedUpdate (blue→red): %v", err)
	}

	select {
	case event := <-w.ResultChan():
		if event.Type != watch.Deleted {
			t.Errorf("expected synthetic Deleted when leaving predicate window, got %s", event.Type)
		}
		delObj := event.Object.(*testObj)
		// The deleted event should carry the OLD object (color=blue).
		if delObj.Labels["color"] != "blue" {
			t.Errorf("expected old object with color=blue in synthetic Deleted, got color=%s", delObj.Labels["color"])
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for synthetic Deleted event")
	}

	// --- Test 2: Object re-enters predicate window (red → blue) ---
	// Should produce a synthetic Added event with the NEW object.
	if err := s.GuaranteedUpdate(ctx, "/testobjs/default/filter-test", updated, false, nil,
		func(input runtime.Object, res storage.ResponseMeta) (runtime.Object, *uint64, error) {
			existing := input.(*testObj)
			existing.Labels = map[string]string{"color": "blue"}
			existing.Data = "blue-again"
			return existing, nil, nil
		}, nil); err != nil {
		t.Fatalf("GuaranteedUpdate (red→blue): %v", err)
	}

	select {
	case event := <-w.ResultChan():
		if event.Type != watch.Added {
			t.Errorf("expected synthetic Added when entering predicate window, got %s", event.Type)
		}
		addObj := event.Object.(*testObj)
		if addObj.Data != "blue-again" {
			t.Errorf("expected data blue-again, got %s", addObj.Data)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for synthetic Added event")
	}

	// --- Test 3: Object stays in predicate window (blue → blue, different data) ---
	// Should produce a normal Modified event.
	if err := s.GuaranteedUpdate(ctx, "/testobjs/default/filter-test", updated, false, nil,
		func(input runtime.Object, res storage.ResponseMeta) (runtime.Object, *uint64, error) {
			existing := input.(*testObj)
			existing.Data = "still-blue"
			return existing, nil, nil
		}, nil); err != nil {
		t.Fatalf("GuaranteedUpdate (blue→blue): %v", err)
	}

	select {
	case event := <-w.ResultChan():
		if event.Type != watch.Modified {
			t.Errorf("expected Modified when staying in predicate window, got %s", event.Type)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for Modified event")
	}

	// --- Test 4: Object modified outside predicate window (red → green) ---
	// First move it out of the window.
	if err := s.GuaranteedUpdate(ctx, "/testobjs/default/filter-test", updated, false, nil,
		func(input runtime.Object, res storage.ResponseMeta) (runtime.Object, *uint64, error) {
			existing := input.(*testObj)
			existing.Labels = map[string]string{"color": "red"}
			return existing, nil, nil
		}, nil); err != nil {
		t.Fatalf("GuaranteedUpdate (blue→red): %v", err)
	}
	// Drain the Deleted event.
	select {
	case <-w.ResultChan():
	case <-time.After(5 * time.Second):
		t.Fatal("timed out draining Deleted event")
	}

	// Now modify while still outside — should produce no event.
	if err := s.GuaranteedUpdate(ctx, "/testobjs/default/filter-test", updated, false, nil,
		func(input runtime.Object, res storage.ResponseMeta) (runtime.Object, *uint64, error) {
			existing := input.(*testObj)
			existing.Labels = map[string]string{"color": "green"}
			existing.Data = "green-data"
			return existing, nil, nil
		}, nil); err != nil {
		t.Fatalf("GuaranteedUpdate (red→green): %v", err)
	}

	select {
	case event := <-w.ResultChan():
		t.Errorf("expected no event for modification outside predicate window, got %s", event.Type)
	case <-time.After(200 * time.Millisecond):
		// Good — no event received.
	}
}
