package cockroach

import (
	"context"
	"fmt"
	"math"
	"sort"
	"strconv"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apiserver/pkg/storage"
)

func seedObjects(t *testing.T, s *store, keys []string) {
	t.Helper()
	ctx := context.Background()
	for _, k := range keys {
		obj := &testObj{
			TypeMeta:   metav1.TypeMeta{APIVersion: "test.io/v1", Kind: "TestObj"},
			ObjectMeta: metav1.ObjectMeta{Name: k[len("/testobjs/default/"):], Namespace: "default"},
			Data:       "data-" + k,
		}
		if err := s.Create(ctx, k, obj, &testObj{}, 0); err != nil {
			t.Fatalf("seed %s: %v", k, err)
		}
	}
}

func TestGetList_Recursive(t *testing.T) {
	s := setupTestStore(t)
	ctx := context.Background()
	keys := []string{
		"/testobjs/default/a",
		"/testobjs/default/b",
		"/testobjs/default/c",
	}
	seedObjects(t, s, keys)

	list := &testObjList{}
	if err := s.GetList(ctx, "/testobjs/default/", storage.ListOptions{
		Recursive: true, Predicate: storage.Everything,
	}, list); err != nil {
		t.Fatalf("GetList: %v", err)
	}
	if got := len(list.Items); got != 3 {
		t.Fatalf("items = %d, want 3", got)
	}
	names := make([]string, len(list.Items))
	for i, it := range list.Items {
		names[i] = it.Name
	}
	sort.Strings(names)
	want := []string{"a", "b", "c"}
	for i, n := range want {
		if names[i] != n {
			t.Errorf("names[%d] = %q, want %q", i, names[i], n)
		}
	}
	if list.ResourceVersion == "" {
		t.Errorf("list.ResourceVersion is empty")
	}
}

func TestGetList_Empty(t *testing.T) {
	s := setupTestStore(t)
	ctx := context.Background()
	list := &testObjList{}
	if err := s.GetList(ctx, "/testobjs/none/", storage.ListOptions{
		Recursive: true, Predicate: storage.Everything,
	}, list); err != nil {
		t.Fatalf("GetList: %v", err)
	}
	if list.Items == nil {
		t.Errorf("Items is nil; want []")
	}
	if len(list.Items) != 0 {
		t.Errorf("Items = %d, want 0", len(list.Items))
	}
	if list.ResourceVersion == "" {
		t.Errorf("empty list must still carry a RV")
	}
}

func TestGetList_TTLExpiredExcluded(t *testing.T) {
	s := setupTestStore(t)
	ctx := context.Background()
	seedObjects(t, s, []string{
		"/testobjs/default/live",
		"/testobjs/default/dead",
	})
	if _, err := s.pool.Exec(ctx,
		`UPDATE kv SET expire_at = now() - INTERVAL '1 minute' WHERE key = $1`,
		"/registry/testobjs/default/dead"); err != nil {
		t.Fatalf("backdate: %v", err)
	}
	list := &testObjList{}
	if err := s.GetList(ctx, "/testobjs/default/", storage.ListOptions{
		Recursive: true, Predicate: storage.Everything,
	}, list); err != nil {
		t.Fatalf("GetList: %v", err)
	}
	if len(list.Items) != 1 {
		t.Fatalf("items = %d, want 1 (only 'live')", len(list.Items))
	}
	if list.Items[0].Name != "live" {
		t.Errorf("got %q, want live", list.Items[0].Name)
	}
}

func TestGetList_Pagination(t *testing.T) {
	s := setupTestStore(t)
	ctx := context.Background()
	var keys []string
	for i := 0; i < 10; i++ {
		keys = append(keys, fmt.Sprintf("/testobjs/default/p%02d", i))
	}
	seedObjects(t, s, keys)

	page := &testObjList{}
	opts := storage.ListOptions{Recursive: true, Predicate: storage.Everything}
	opts.Predicate.Limit = 4
	if err := s.GetList(ctx, "/testobjs/default/", opts, page); err != nil {
		t.Fatalf("GetList: %v", err)
	}
	if got := len(page.Items); got != 4 {
		t.Fatalf("page1 items = %d, want 4", got)
	}
	if page.Continue == "" {
		t.Errorf("expected continue token, got empty")
	}

	next := &testObjList{}
	opts2 := storage.ListOptions{Recursive: true, Predicate: storage.Everything}
	opts2.Predicate.Limit = 4
	opts2.Predicate.Continue = page.Continue
	if err := s.GetList(ctx, "/testobjs/default/", opts2, next); err != nil {
		t.Fatalf("GetList #2: %v", err)
	}
	if got := len(next.Items); got != 4 {
		t.Errorf("page2 items = %d, want 4", got)
	}
}

func TestGetList_TooHighResourceVersion(t *testing.T) {
	s := setupTestStore(t)
	ctx := context.Background()
	err := s.GetList(ctx, "/testobjs/default/", storage.ListOptions{
		Recursive:            true,
		ResourceVersion:      strconv.FormatInt(math.MaxInt64, 10),
		ResourceVersionMatch: metav1.ResourceVersionMatchExact,
		Predicate:            storage.Everything,
	}, &testObjList{})
	if !storage.IsTooLargeResourceVersion(err) {
		t.Errorf("got %v, want TooLargeResourceVersion", err)
	}
}

// nolint: unused - placeholder for the non-recursive branch coverage
var _ runtime.Object = &testObj{}
