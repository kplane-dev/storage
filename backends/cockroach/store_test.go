package cockroach

import (
	"context"
	"fmt"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/runtime/serializer"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apiserver/pkg/storage"
	"k8s.io/apiserver/pkg/storage/value/encrypt/identity"
)

// testObj is the minimal runtime.Object used by store tests. Registered
// as test.io/v1 TestObj at package init.
type testObj struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`
	Data              string `json:"data,omitempty"`
}

func (t *testObj) DeepCopyObject() runtime.Object {
	return &testObj{TypeMeta: t.TypeMeta, ObjectMeta: *t.ObjectMeta.DeepCopy(), Data: t.Data}
}

type testObjList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []testObj `json:"items"`
}

func (t *testObjList) DeepCopyObject() runtime.Object {
	out := &testObjList{TypeMeta: t.TypeMeta, ListMeta: *t.ListMeta.DeepCopy()}
	for i := range t.Items {
		out.Items = append(out.Items, *t.Items[i].DeepCopyObject().(*testObj))
	}
	return out
}

var (
	testScheme = runtime.NewScheme()
	testCodecs serializer.CodecFactory
	testGV     = schema.GroupVersion{Group: "test.io", Version: "v1"}
)

func init() {
	sb := runtime.NewSchemeBuilder(func(s *runtime.Scheme) error {
		s.AddKnownTypes(testGV, &testObj{}, &testObjList{})
		metav1.AddToGroupVersion(s, testGV)
		return nil
	})
	utilruntime.Must(sb.AddToScheme(testScheme))
	testCodecs = serializer.NewCodecFactory(testScheme)
}

// setupTestStore returns a store connected to a fresh, schema-applied
// database with a clean kv table. Cleanup drops the database.
func setupTestStore(t *testing.T) *store {
	t.Helper()
	skipIfNoCockroach(t)
	ctx := context.Background()
	dbName := fmt.Sprintf("kv_store_%d", time.Now().UnixNano())

	if err := createTestDatabase(ctx, dbName); err != nil {
		t.Fatalf("createTestDatabase: %v", err)
	}
	t.Cleanup(func() { _ = dropTestDatabase(context.Background(), dbName) })

	cfg := Config{DSN: testDSN(), Database: dbName}
	if err := EnsureSchema(ctx, cfg); err != nil {
		t.Fatalf("EnsureSchema: %v", err)
	}
	pool, err := cfg.NewPool(ctx)
	if err != nil {
		t.Fatalf("NewPool: %v", err)
	}
	t.Cleanup(pool.Close)

	codec := testCodecs.LegacyCodec(testGV)
	return NewStore(
		pool,
		codec,
		func() runtime.Object { return &testObj{} },
		func() runtime.Object { return &testObjList{} },
		"/registry",
		"/testobjs",
		identity.NewEncryptCheckTransformer(),
		nil,
	)
}

func TestStore_Create(t *testing.T) {
	s := setupTestStore(t)
	ctx := context.Background()
	in := &testObj{
		TypeMeta:   metav1.TypeMeta{APIVersion: "test.io/v1", Kind: "TestObj"},
		ObjectMeta: metav1.ObjectMeta{Name: "one", Namespace: "default"},
		Data:       "hello",
	}
	out := &testObj{}
	if err := s.Create(ctx, "/testobjs/default/one", in, out, 0); err != nil {
		t.Fatalf("Create: %v", err)
	}
	if out.Name != "one" || out.Data != "hello" {
		t.Errorf("unexpected out: %+v", out)
	}
	if out.ResourceVersion == "" {
		t.Errorf("out.ResourceVersion is empty")
	}
}

func TestStore_Create_DuplicateKey(t *testing.T) {
	s := setupTestStore(t)
	ctx := context.Background()
	in := &testObj{
		TypeMeta:   metav1.TypeMeta{APIVersion: "test.io/v1", Kind: "TestObj"},
		ObjectMeta: metav1.ObjectMeta{Name: "dup", Namespace: "default"},
	}
	if err := s.Create(ctx, "/testobjs/default/dup", in, &testObj{}, 0); err != nil {
		t.Fatalf("Create #1: %v", err)
	}
	err := s.Create(ctx, "/testobjs/default/dup", in, &testObj{}, 0)
	if !storage.IsExist(err) {
		t.Errorf("expected KeyExistsError, got %v", err)
	}
}

func TestStore_Create_RejectsResourceVersion(t *testing.T) {
	s := setupTestStore(t)
	ctx := context.Background()
	in := &testObj{
		TypeMeta: metav1.TypeMeta{APIVersion: "test.io/v1", Kind: "TestObj"},
		ObjectMeta: metav1.ObjectMeta{
			Name: "rv", Namespace: "default", ResourceVersion: "12345",
		},
	}
	err := s.Create(ctx, "/testobjs/default/rv", in, &testObj{}, 0)
	if err != storage.ErrResourceVersionSetOnCreate {
		t.Errorf("got %v, want ErrResourceVersionSetOnCreate", err)
	}
}

func TestStore_Get(t *testing.T) {
	s := setupTestStore(t)
	ctx := context.Background()
	created := &testObj{}
	if err := s.Create(ctx, "/testobjs/default/get",
		&testObj{
			TypeMeta:   metav1.TypeMeta{APIVersion: "test.io/v1", Kind: "TestObj"},
			ObjectMeta: metav1.ObjectMeta{Name: "get", Namespace: "default"},
			Data:       "abc",
		}, created, 0); err != nil {
		t.Fatalf("Create: %v", err)
	}

	got := &testObj{}
	if err := s.Get(ctx, "/testobjs/default/get", storage.GetOptions{}, got); err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.Data != "abc" {
		t.Errorf("Data = %q, want %q", got.Data, "abc")
	}
	if got.ResourceVersion == "" {
		t.Errorf("ResourceVersion is empty")
	}
}

func TestStore_Get_NotFound(t *testing.T) {
	s := setupTestStore(t)
	ctx := context.Background()
	err := s.Get(ctx, "/testobjs/default/missing", storage.GetOptions{}, &testObj{})
	if !storage.IsNotFound(err) {
		t.Errorf("got %v, want NotFound", err)
	}
}

func TestStore_Get_IgnoreNotFound(t *testing.T) {
	s := setupTestStore(t)
	ctx := context.Background()
	out := &testObj{Data: "should-be-cleared"}
	if err := s.Get(ctx, "/testobjs/default/missing",
		storage.GetOptions{IgnoreNotFound: true}, out); err != nil {
		t.Fatalf("Get: %v", err)
	}
	if out.Data != "" {
		t.Errorf("Data = %q, want cleared", out.Data)
	}
}

func TestStore_Delete(t *testing.T) {
	s := setupTestStore(t)
	ctx := context.Background()
	created := &testObj{}
	if err := s.Create(ctx, "/testobjs/default/del",
		&testObj{
			TypeMeta:   metav1.TypeMeta{APIVersion: "test.io/v1", Kind: "TestObj"},
			ObjectMeta: metav1.ObjectMeta{Name: "del", Namespace: "default"},
			Data:       "gone",
		}, created, 0); err != nil {
		t.Fatalf("Create: %v", err)
	}

	deleted := &testObj{}
	if err := s.Delete(ctx, "/testobjs/default/del", deleted,
		nil, storage.ValidateAllObjectFunc, nil, storage.DeleteOptions{}); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if deleted.Data != "gone" {
		t.Errorf("Delete out.Data = %q, want %q", deleted.Data, "gone")
	}

	if err := s.Get(ctx, "/testobjs/default/del", storage.GetOptions{}, &testObj{}); !storage.IsNotFound(err) {
		t.Errorf("post-delete Get: got %v, want NotFound", err)
	}
}

func TestStore_Delete_NotFound(t *testing.T) {
	s := setupTestStore(t)
	err := s.Delete(context.Background(), "/testobjs/default/missing",
		&testObj{}, nil, storage.ValidateAllObjectFunc, nil, storage.DeleteOptions{})
	if !storage.IsNotFound(err) {
		t.Errorf("got %v, want NotFound", err)
	}
}

func TestStore_GetCurrentResourceVersion(t *testing.T) {
	s := setupTestStore(t)
	ctx := context.Background()
	rv1, err := s.GetCurrentResourceVersion(ctx)
	if err != nil {
		t.Fatalf("GetCurrentResourceVersion: %v", err)
	}
	if rv1 == 0 {
		t.Errorf("first RV is zero")
	}
	time.Sleep(2 * time.Millisecond)
	rv2, err := s.GetCurrentResourceVersion(ctx)
	if err != nil {
		t.Fatalf("GetCurrentResourceVersion #2: %v", err)
	}
	if rv2 <= rv1 {
		t.Errorf("RV not advancing: rv1=%d rv2=%d", rv1, rv2)
	}
}

func TestStore_TTL_ExpiredRowInvisible(t *testing.T) {
	s := setupTestStore(t)
	ctx := context.Background()
	in := &testObj{
		TypeMeta:   metav1.TypeMeta{APIVersion: "test.io/v1", Kind: "TestObj"},
		ObjectMeta: metav1.ObjectMeta{Name: "ephemeral", Namespace: "default"},
		Data:       "temp",
	}
	if err := s.Create(ctx, "/testobjs/default/ephemeral", in, &testObj{}, 1); err != nil {
		t.Fatalf("Create: %v", err)
	}
	// Overwrite expire_at to sit in the past so the read filter treats
	// the row as expired without waiting a second in the test.
	if _, err := s.pool.Exec(ctx,
		`UPDATE kv SET expire_at = now() - INTERVAL '1 minute' WHERE key = $1`,
		"/registry/testobjs/default/ephemeral"); err != nil {
		t.Fatalf("backdate expire_at: %v", err)
	}
	err := s.Get(ctx, "/testobjs/default/ephemeral", storage.GetOptions{}, &testObj{})
	if !storage.IsNotFound(err) {
		t.Errorf("expired row should read as NotFound, got %v", err)
	}
}

// createTestDatabase / dropTestDatabase intentionally use a raw pgx.Conn
// so schema creation doesn't depend on any store code under test.
func createTestDatabase(ctx context.Context, dbName string) error {
	return withAdmin(ctx, func(ctx context.Context, exec sqlExec) error {
		_, err := exec(ctx, fmt.Sprintf(`CREATE DATABASE %q`, dbName))
		return err
	})
}

func dropTestDatabase(ctx context.Context, dbName string) error {
	return withAdmin(ctx, func(ctx context.Context, exec sqlExec) error {
		_, err := exec(ctx, fmt.Sprintf(`DROP DATABASE IF EXISTS %q CASCADE`, dbName))
		return err
	})
}
