package cockroach

import (
	"context"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/apiserver/pkg/storage"
)

// setupTestStoreWithWatch returns a store with a live ChangefeedSubscription.
func setupTestStoreWithWatch(t *testing.T) *store {
	t.Helper()
	s := setupTestStore(t)
	cc, err := (Config{DSN: testDSN(), Database: currentDatabase(t, s)}).ConnConfig()
	if err != nil {
		t.Fatalf("ConnConfig: %v", err)
	}
	cf := NewChangefeedSubscription(cc)
	cf.Start(context.Background())
	s.SetChangefeed(cf)
	t.Cleanup(cf.Stop)
	time.Sleep(200 * time.Millisecond)
	return s
}

func TestWatch_CreateEmitsAdded(t *testing.T) {
	s := setupTestStoreWithWatch(t)
	ctx := context.Background()

	w, err := s.Watch(ctx, "/testobjs/default/", storage.ListOptions{
		Predicate: storage.Everything,
		Recursive: true,
		// SendInitialEvents nil + RV="" → legacy: send current state.
	})
	if err != nil {
		t.Fatalf("Watch: %v", err)
	}
	t.Cleanup(w.Stop)
	drainWatch(w, 200*time.Millisecond)

	obj := &testObj{
		TypeMeta:   metav1.TypeMeta{APIVersion: "test.io/v1", Kind: "TestObj"},
		ObjectMeta: metav1.ObjectMeta{Name: "w-add", Namespace: "default"},
		Data:       "arrived",
	}
	if err := s.Create(ctx, "/testobjs/default/w-add", obj, &testObj{}, 0); err != nil {
		t.Fatalf("Create: %v", err)
	}
	ev := waitForEvent(t, w, watch.Added, 5*time.Second)
	got := ev.Object.(*testObj)
	if got.Name != "w-add" {
		t.Errorf("event name = %q, want w-add", got.Name)
	}
	if got.Data != "arrived" {
		t.Errorf("event data = %q, want arrived", got.Data)
	}
}

func TestWatch_UpdateEmitsModified(t *testing.T) {
	s := setupTestStoreWithWatch(t)
	ctx := context.Background()
	// Watch from post-create RV so the initial replay doesn't fire.
	created := &testObj{}
	if err := s.Create(ctx, "/testobjs/default/w-mod",
		&testObj{
			TypeMeta:   metav1.TypeMeta{APIVersion: "test.io/v1", Kind: "TestObj"},
			ObjectMeta: metav1.ObjectMeta{Name: "w-mod", Namespace: "default"},
			Data:       "before",
		}, created, 0); err != nil {
		t.Fatalf("Create: %v", err)
	}

	w, err := s.Watch(ctx, "/testobjs/default/", storage.ListOptions{
		ResourceVersion: created.ResourceVersion,
		Predicate:       storage.Everything,
		Recursive:       true,
	})
	if err != nil {
		t.Fatalf("Watch: %v", err)
	}
	t.Cleanup(w.Stop)

	out := &testObj{}
	if err := s.GuaranteedUpdate(ctx, "/testobjs/default/w-mod", out, false, nil,
		mutateData("after"), nil,
	); err != nil {
		t.Fatalf("update: %v", err)
	}
	ev := waitForEvent(t, w, watch.Modified, 5*time.Second)
	if got := ev.Object.(*testObj); got.Data != "after" {
		t.Errorf("event data = %q, want after", got.Data)
	}
}

func TestWatch_DeleteEmitsDeleted(t *testing.T) {
	s := setupTestStoreWithWatch(t)
	ctx := context.Background()
	created := &testObj{}
	if err := s.Create(ctx, "/testobjs/default/w-del",
		&testObj{
			TypeMeta:   metav1.TypeMeta{APIVersion: "test.io/v1", Kind: "TestObj"},
			ObjectMeta: metav1.ObjectMeta{Name: "w-del", Namespace: "default"},
			Data:       "gone",
		}, created, 0); err != nil {
		t.Fatalf("Create: %v", err)
	}
	w, err := s.Watch(ctx, "/testobjs/default/", storage.ListOptions{
		ResourceVersion: created.ResourceVersion,
		Predicate:       storage.Everything,
		Recursive:       true,
	})
	if err != nil {
		t.Fatalf("Watch: %v", err)
	}
	t.Cleanup(w.Stop)

	if err := s.Delete(ctx, "/testobjs/default/w-del", &testObj{}, nil,
		storage.ValidateAllObjectFunc, nil, storage.DeleteOptions{}); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	ev := waitForEvent(t, w, watch.Deleted, 5*time.Second)
	if got := ev.Object.(*testObj); got.Data != "gone" {
		t.Errorf("delete event should carry prevValue; data = %q", got.Data)
	}
}

func TestWatch_InitialEventsReplayCurrentState(t *testing.T) {
	s := setupTestStoreWithWatch(t)
	ctx := context.Background()

	seedObjects(t, s, []string{"/testobjs/default/pre-a", "/testobjs/default/pre-b"})

	w, err := s.Watch(ctx, "/testobjs/default/", storage.ListOptions{
		// No RV, no SendInitialEvents flag → legacy replay.
		Predicate: storage.Everything,
		Recursive: true,
	})
	if err != nil {
		t.Fatalf("Watch: %v", err)
	}
	t.Cleanup(w.Stop)

	seen := map[string]bool{}
	deadline := time.After(5 * time.Second)
	for len(seen) < 2 {
		select {
		case ev, ok := <-w.ResultChan():
			if !ok {
				t.Fatalf("watch closed; seen=%v", seen)
			}
			if ev.Type != watch.Added {
				continue
			}
			seen[ev.Object.(*testObj).Name] = true
		case <-deadline:
			t.Fatalf("timed out; seen=%v", seen)
		}
	}
	if !seen["pre-a"] || !seen["pre-b"] {
		t.Errorf("missing replay events: %v", seen)
	}
}

func TestWatch_RespectsCancelledContext(t *testing.T) {
	s := setupTestStoreWithWatch(t)
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()

	w, err := s.Watch(cancelled, "/testobjs/default/", storage.ListOptions{
		Predicate: storage.Everything,
		Recursive: true,
	})
	if err != nil {
		t.Fatalf("Watch: %v", err)
	}
	select {
	case _, ok := <-w.ResultChan():
		if ok {
			t.Errorf("ResultChan should be closed on cancelled context")
		}
	case <-time.After(2 * time.Second):
		t.Errorf("watch did not close on cancelled context")
	}
}

func drainWatch(w watch.Interface, d time.Duration) {
	deadline := time.After(d)
	for {
		select {
		case _, ok := <-w.ResultChan():
			if !ok {
				return
			}
		case <-deadline:
			return
		}
	}
}

func waitForEvent(t *testing.T, w watch.Interface, want watch.EventType, d time.Duration) watch.Event {
	t.Helper()
	deadline := time.After(d)
	for {
		select {
		case ev, ok := <-w.ResultChan():
			if !ok {
				t.Fatalf("watch closed before %s", want)
			}
			if ev.Type == want {
				return ev
			}
		case <-deadline:
			t.Fatalf("timed out waiting for %s", want)
		}
	}
}
