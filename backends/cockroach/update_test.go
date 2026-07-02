package cockroach

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apiserver/pkg/storage"
)

func TestGuaranteedUpdate_Existing(t *testing.T) {
	s := setupTestStore(t)
	ctx := context.Background()
	seedObjects(t, s, []string{"/testobjs/default/upd"})

	out := &testObj{}
	err := s.GuaranteedUpdate(ctx, "/testobjs/default/upd", out, false, nil,
		func(input runtime.Object, _ storage.ResponseMeta) (runtime.Object, *uint64, error) {
			cur := input.(*testObj)
			cur.Data = "updated"
			return cur, nil, nil
		}, nil)
	if err != nil {
		t.Fatalf("GuaranteedUpdate: %v", err)
	}
	if out.Data != "updated" {
		t.Errorf("Data = %q, want updated", out.Data)
	}
	if out.ResourceVersion == "" {
		t.Errorf("ResourceVersion is empty")
	}
}

func TestGuaranteedUpdate_NotFound(t *testing.T) {
	s := setupTestStore(t)
	err := s.GuaranteedUpdate(context.Background(), "/testobjs/default/missing", &testObj{},
		false, nil,
		func(input runtime.Object, _ storage.ResponseMeta) (runtime.Object, *uint64, error) {
			return input, nil, nil
		}, nil)
	if !storage.IsNotFound(err) {
		t.Errorf("got %v, want NotFound", err)
	}
}

func TestGuaranteedUpdate_IgnoreNotFound(t *testing.T) {
	s := setupTestStore(t)
	ctx := context.Background()
	out := &testObj{}
	err := s.GuaranteedUpdate(ctx, "/testobjs/default/new", out, true, nil,
		func(input runtime.Object, _ storage.ResponseMeta) (runtime.Object, *uint64, error) {
			return &testObj{
				TypeMeta:   metav1.TypeMeta{APIVersion: "test.io/v1", Kind: "TestObj"},
				ObjectMeta: metav1.ObjectMeta{Name: "new", Namespace: "default"},
				Data:       "created",
			}, nil, nil
		}, nil)
	if err != nil {
		t.Fatalf("GuaranteedUpdate: %v", err)
	}
	if out.Data != "created" {
		t.Errorf("Data = %q, want created", out.Data)
	}
}

func TestGuaranteedUpdate_Preconditions(t *testing.T) {
	s := setupTestStore(t)
	ctx := context.Background()
	seedObjects(t, s, []string{"/testobjs/default/pre"})

	wrongUID := types.UID("not-the-uid")
	preconditions := &storage.Preconditions{UID: &wrongUID}
	err := s.GuaranteedUpdate(ctx, "/testobjs/default/pre", &testObj{}, false, preconditions,
		func(input runtime.Object, _ storage.ResponseMeta) (runtime.Object, *uint64, error) {
			return input, nil, nil
		}, nil)
	if err == nil {
		t.Fatal("expected preconditions failure")
	}
	if !isPreconditionsError(err) {
		t.Errorf("got %v, want preconditions failure", err)
	}
}

// TestGuaranteedUpdate_ConflictRetries verifies the retry loop when
// tryUpdate reports a semantic conflict (as opposed to a serialization
// failure in the DB — those are handled by crdbpgx).
func TestGuaranteedUpdate_ConflictRetries(t *testing.T) {
	s := setupTestStore(t)
	ctx := context.Background()
	seedObjects(t, s, []string{"/testobjs/default/conflict"})

	var attempts int
	err := s.GuaranteedUpdate(ctx, "/testobjs/default/conflict", &testObj{}, false, nil,
		func(input runtime.Object, _ storage.ResponseMeta) (runtime.Object, *uint64, error) {
			attempts++
			if attempts < 3 {
				return nil, nil, apierrors.NewConflict(
					schema.GroupResource{Group: "test.io", Resource: "testobjs"},
					"conflict", fmt.Errorf("temporary conflict"))
			}
			cur := input.(*testObj)
			cur.Data = "won"
			return cur, nil, nil
		}, nil)
	if err != nil {
		t.Fatalf("GuaranteedUpdate: %v", err)
	}
	if attempts != 3 {
		t.Errorf("attempts = %d, want 3", attempts)
	}
}

func TestGuaranteedUpdate_NoOpKeepsRV(t *testing.T) {
	s := setupTestStore(t)
	ctx := context.Background()
	created := &testObj{}
	if err := s.Create(ctx, "/testobjs/default/noop",
		&testObj{
			TypeMeta:   metav1.TypeMeta{APIVersion: "test.io/v1", Kind: "TestObj"},
			ObjectMeta: metav1.ObjectMeta{Name: "noop", Namespace: "default"},
			Data:       "same",
		}, created, 0); err != nil {
		t.Fatalf("Create: %v", err)
	}

	out := &testObj{}
	err := s.GuaranteedUpdate(ctx, "/testobjs/default/noop", out, false, nil,
		func(input runtime.Object, _ storage.ResponseMeta) (runtime.Object, *uint64, error) {
			return input, nil, nil
		}, nil)
	if err != nil {
		t.Fatalf("GuaranteedUpdate: %v", err)
	}
	if out.ResourceVersion != created.ResourceVersion {
		t.Errorf("no-op should keep RV %q, got %q", created.ResourceVersion, out.ResourceVersion)
	}
}

func TestGuaranteedUpdate_ConcurrentWritersSerialize(t *testing.T) {
	s := setupTestStore(t)
	ctx := context.Background()
	seedObjects(t, s, []string{"/testobjs/default/race"})

	var wg sync.WaitGroup
	const N = 5
	errs := make(chan error, N)
	for i := 0; i < N; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			suffix := fmt.Sprintf("-writer%d", i)
			errs <- s.GuaranteedUpdate(ctx, "/testobjs/default/race", &testObj{}, false, nil,
				func(input runtime.Object, _ storage.ResponseMeta) (runtime.Object, *uint64, error) {
					cur := input.(*testObj)
					cur.Data += suffix
					return cur, nil, nil
				}, nil)
		}(i)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Errorf("writer error: %v", err)
		}
	}
	final := &testObj{}
	if err := s.Get(ctx, "/testobjs/default/race", storage.GetOptions{}, final); err != nil {
		t.Fatalf("Get: %v", err)
	}
	if final.Data == "" {
		t.Errorf("Data lost all writer suffixes")
	}
}

// isPreconditionsError matches Preconditions.Check failure by message,
// since the storage package doesn't expose a typed sentinel.
func isPreconditionsError(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	return strings.Contains(msg, "Precondition") || strings.Contains(msg, "precondition")
}
