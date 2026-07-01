package cockroach

import (
	"context"
	"fmt"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/apiserver/pkg/storage"
)

func TestTTLScanner_DeletesExpiredRows(t *testing.T) {
	s := setupTestStore(t)
	ctx := context.Background()

	seedObjects(t, s, []string{
		"/testobjs/default/keep",
		"/testobjs/default/drop-a",
		"/testobjs/default/drop-b",
	})
	// Backdate two rows so they're expired.
	for _, k := range []string{"/registry/testobjs/default/drop-a", "/registry/testobjs/default/drop-b"} {
		if _, err := s.pool.Exec(ctx,
			`UPDATE kv SET expire_at = now() - INTERVAL '1 minute' WHERE key = $1`, k); err != nil {
			t.Fatalf("backdate: %v", err)
		}
	}

	scanner := NewTTLScanner(s, 100*time.Millisecond)
	scanner.Start(ctx)
	t.Cleanup(scanner.Stop)

	deadline := time.After(5 * time.Second)
	for {
		select {
		case <-deadline:
			t.Fatalf("expired rows not swept in time")
		case <-time.After(200 * time.Millisecond):
		}
		var n int
		if err := s.pool.QueryRow(ctx, `SELECT count(*) FROM kv WHERE expire_at IS NOT NULL AND expire_at <= now()`).Scan(&n); err != nil {
			t.Fatalf("count: %v", err)
		}
		if n == 0 {
			break
		}
	}
	// The unexpired row must survive.
	if err := s.Get(ctx, "/testobjs/default/keep", storage.GetOptions{}, &testObj{}); err != nil {
		t.Errorf("live row was removed: %v", err)
	}
}

func TestTTLScanner_EmitsChangefeedDeleteEvents(t *testing.T) {
	s := setupTestStoreWithWatch(t)
	ctx := context.Background()

	seedObjects(t, s, []string{"/testobjs/default/expiring"})
	if _, err := s.pool.Exec(ctx,
		`UPDATE kv SET expire_at = now() - INTERVAL '1 minute' WHERE key = $1`,
		"/registry/testobjs/default/expiring"); err != nil {
		t.Fatalf("backdate: %v", err)
	}

	w, err := s.Watch(ctx, "/testobjs/default/", storage.ListOptions{
		Predicate: storage.Everything,
		Recursive: true,
	})
	if err != nil {
		t.Fatalf("Watch: %v", err)
	}
	t.Cleanup(w.Stop)
	drainWatch(w, 200*time.Millisecond)

	scanner := NewTTLScanner(s, 100*time.Millisecond)
	scanner.Start(ctx)
	t.Cleanup(scanner.Stop)

	ev := waitForEvent(t, w, watch.Deleted, 5*time.Second)
	if got := ev.Object.(*testObj); got.Name != "expiring" {
		t.Errorf("Deleted event object.Name = %q, want expiring", got.Name)
	}
}

func TestTTLScanner_RefreshWinsRace(t *testing.T) {
	s := setupTestStore(t)
	ctx := context.Background()

	obj := &testObj{
		TypeMeta:   metav1.TypeMeta{APIVersion: "test.io/v1", Kind: "TestObj"},
		ObjectMeta: metav1.ObjectMeta{Name: "refresh", Namespace: "default"},
		Data:       "orig",
	}
	if err := s.Create(ctx, "/testobjs/default/refresh", obj, &testObj{}, 0); err != nil {
		t.Fatalf("Create: %v", err)
	}

	// Backdate expire_at, then IMMEDIATELY refresh it before the scanner
	// runs. The scanner's DELETE ... WHERE expire_at <= now() should not
	// touch the refreshed row.
	if _, err := s.pool.Exec(ctx, `
		UPDATE kv SET expire_at = now() - INTERVAL '1 minute'
		WHERE key = $1`, "/registry/testobjs/default/refresh"); err != nil {
		t.Fatalf("backdate: %v", err)
	}
	if _, err := s.pool.Exec(ctx, `
		UPDATE kv SET expire_at = now() + INTERVAL '1 hour'
		WHERE key = $1`, "/registry/testobjs/default/refresh"); err != nil {
		t.Fatalf("refresh: %v", err)
	}

	scanner := NewTTLScanner(s, 50*time.Millisecond)
	scanner.Start(ctx)
	t.Cleanup(scanner.Stop)
	time.Sleep(500 * time.Millisecond)

	// The row survived — refresh serialized ahead of scanner's DELETE.
	if err := s.Get(ctx, "/testobjs/default/refresh", storage.GetOptions{}, &testObj{}); err != nil {
		t.Errorf("refreshed row was deleted: %v", err)
	}
}

func TestTTLScanner_BatchLimit(t *testing.T) {
	s := setupTestStore(t)
	ctx := context.Background()

	var keys []string
	for i := 0; i < 250; i++ {
		keys = append(keys, fmt.Sprintf("/testobjs/default/e-%03d", i))
	}
	seedObjects(t, s, keys)
	if _, err := s.pool.Exec(ctx,
		`UPDATE kv SET expire_at = now() - INTERVAL '1 minute'
		 WHERE key LIKE '/registry/testobjs/default/e-%'`); err != nil {
		t.Fatalf("backdate: %v", err)
	}

	scanner := NewTTLScanner(s, 100*time.Millisecond)
	scanner.batch = 50 // small batch to force multiple ticks
	scanner.Start(ctx)
	t.Cleanup(scanner.Stop)

	deadline := time.After(10 * time.Second)
	for {
		select {
		case <-deadline:
			t.Fatalf("scanner didn't drain 250 rows in 10s")
		case <-time.After(200 * time.Millisecond):
		}
		var n int
		if err := s.pool.QueryRow(ctx, `SELECT count(*) FROM kv WHERE expire_at IS NOT NULL AND expire_at <= now()`).Scan(&n); err != nil {
			t.Fatalf("count: %v", err)
		}
		if n == 0 {
			return
		}
	}
}
