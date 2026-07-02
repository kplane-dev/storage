package cockroach

import (
	"context"
	"encoding/hex"
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apiserver/pkg/storage"
)

// setupTestChangefeed spins up a store plus a ChangefeedSubscription
// wired to the same database. Cleanup stops the subscription.
func setupTestChangefeed(t *testing.T) (*store, *ChangefeedSubscription) {
	t.Helper()
	s := setupTestStore(t)
	cc, err := (Config{DSN: testDSN(), Database: currentDatabase(t, s)}).ConnConfig()
	if err != nil {
		t.Fatalf("ConnConfig: %v", err)
	}
	cf := NewChangefeedSubscription(cc)
	cf.Start(context.Background())
	t.Cleanup(cf.Stop)
	// Give the subscription a beat to open its query before writes start.
	time.Sleep(200 * time.Millisecond)
	return s, cf
}

func currentDatabase(t *testing.T, s *store) string {
	t.Helper()
	var db string
	if err := s.pool.QueryRow(context.Background(), `SELECT current_database()`).Scan(&db); err != nil {
		t.Fatalf("current_database: %v", err)
	}
	return db
}

func TestChangefeed_SeesCreate(t *testing.T) {
	s, cf := setupTestChangefeed(t)
	sub := cf.Subscribe("/registry/testobjs/", 128)
	t.Cleanup(func() { cf.Unsubscribe(sub) })
	// Absorb any historical rows delivered from the initial scan.
	drainStale(sub, 200*time.Millisecond)

	obj := &testObj{
		TypeMeta:   metav1.TypeMeta{APIVersion: "test.io/v1", Kind: "TestObj"},
		ObjectMeta: metav1.ObjectMeta{Name: "cf-create", Namespace: "default"},
		Data:       "hello",
	}
	if err := s.Create(context.Background(), "/testobjs/default/cf-create", obj, &testObj{}, 0); err != nil {
		t.Fatalf("Create: %v", err)
	}

	ev := waitForKey(t, sub, "/registry/testobjs/default/cf-create", 5*time.Second)
	if !ev.isCreate {
		t.Errorf("event isCreate = false; want true")
	}
	if len(ev.newValue) == 0 {
		t.Errorf("event newValue is empty")
	}
	if ev.mvccHLC == "" {
		t.Errorf("event mvccHLC is empty")
	}
}

func TestChangefeed_SeesUpdate(t *testing.T) {
	s, cf := setupTestChangefeed(t)
	sub := cf.Subscribe("/registry/testobjs/", 128)
	t.Cleanup(func() { cf.Unsubscribe(sub) })
	drainStale(sub, 200*time.Millisecond)

	seedObjects(t, s, []string{"/testobjs/default/cf-upd"})
	waitForKey(t, sub, "/registry/testobjs/default/cf-upd", 5*time.Second)

	out := &testObj{}
	if err := s.GuaranteedUpdate(context.Background(), "/testobjs/default/cf-upd", out, false, nil,
		mutateData("changed"), nil,
	); err != nil {
		t.Fatalf("update: %v", err)
	}
	ev := waitForKey(t, sub, "/registry/testobjs/default/cf-upd", 5*time.Second)
	if ev.isCreate || ev.isDelete {
		t.Errorf("expected update (before+after both set), got %+v", ev)
	}
	if len(ev.oldValue) == 0 || len(ev.newValue) == 0 {
		t.Errorf("update event missing before/after: %+v", ev)
	}
}

func TestChangefeed_SeesDelete(t *testing.T) {
	s, cf := setupTestChangefeed(t)
	sub := cf.Subscribe("/registry/testobjs/", 128)
	t.Cleanup(func() { cf.Unsubscribe(sub) })
	drainStale(sub, 200*time.Millisecond)

	seedObjects(t, s, []string{"/testobjs/default/cf-del"})
	waitForKey(t, sub, "/registry/testobjs/default/cf-del", 5*time.Second)

	if err := deleteRow(s, "/registry/testobjs/default/cf-del"); err != nil {
		t.Fatalf("delete: %v", err)
	}
	ev := waitForKey(t, sub, "/registry/testobjs/default/cf-del", 5*time.Second)
	if !ev.isDelete {
		t.Errorf("event isDelete = false; want true")
	}
	if len(ev.oldValue) == 0 {
		t.Errorf("event oldValue is empty (need diff for prevValue)")
	}
}

func TestChangefeed_ResolvedAdvancesWatermark(t *testing.T) {
	_, cf := setupTestChangefeed(t)
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if cf.ResolvedHLC() != "" {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Errorf("no resolved HLC received within 5s")
}

func TestChangefeed_ReconnectResumesFromCursor(t *testing.T) {
	s, cf := setupTestChangefeed(t)
	sub := cf.Subscribe("/registry/testobjs/", 128)
	t.Cleanup(func() { cf.Unsubscribe(sub) })
	drainStale(sub, 200*time.Millisecond)

	// Baseline event to prime a resolved timestamp we can resume from.
	seedObjects(t, s, []string{"/testobjs/default/before-disconnect"})
	waitForKey(t, sub, "/registry/testobjs/default/before-disconnect", 5*time.Second)

	// Kill the server-side changefeed query; the reader loop should see
	// the connection drop, back off, and reopen WITH cursor=<lastResolved>.
	if err := killChangefeedQueries(context.Background(), s.pool); err != nil {
		t.Fatalf("cancel changefeed: %v", err)
	}

	// Give the reader a beat to notice the drop and reopen. If reconnect
	// is broken this loop times out on the next Create.
	waitForReconnect(t, cf, sub, 10*time.Second)

	// Post-reconnect write must arrive on the same subscriber.
	seedObjects(t, s, []string{"/testobjs/default/after-reconnect"})
	ev := waitForKey(t, sub, "/registry/testobjs/default/after-reconnect", 10*time.Second)
	if !ev.isCreate {
		t.Errorf("post-reconnect event isCreate=false; want true")
	}
}

// killChangefeedQueries cancels every active CREATE CHANGEFEED on the
// cluster, forcing an EOF on the reader's pgx conn. The filter anchors on
// the query prefix so this CANCEL doesn't match itself.
func killChangefeedQueries(ctx context.Context, pool *pgxpool.Pool) error {
	_, err := pool.Exec(ctx,
		`CANCEL QUERIES (SELECT query_id FROM [SHOW CLUSTER QUERIES] WHERE query ILIKE 'CREATE CHANGEFEED%')`,
	)
	return err
}

// waitForReconnect blocks until the subscription's ResolvedHLC advances
// past the moment killChangefeedQueries fired. Proves the reader loop
// re-established the changefeed and started receiving resolved rows.
func waitForReconnect(t *testing.T, cf *ChangefeedSubscription, sub *changefeedSubscriber, d time.Duration) {
	t.Helper()
	before := cf.ResolvedHLC()
	deadline := time.After(d)
	for {
		if cur := cf.ResolvedHLC(); cur != "" && cur > before {
			return
		}
		select {
		case _, ok := <-sub.events():
			if !ok {
				t.Fatalf("subscription closed before reconnect")
			}
		case <-time.After(200 * time.Millisecond):
		case <-deadline:
			t.Fatalf("reconnect did not advance resolved HLC within %v (before=%q, still=%q)", d, before, cf.ResolvedHLC())
		}
	}
}

func TestChangefeed_SubscriberFilterByPrefix(t *testing.T) {
	s, cf := setupTestChangefeed(t)
	sub := cf.Subscribe("/registry/testobjs/only/", 128)
	t.Cleanup(func() { cf.Unsubscribe(sub) })
	drainStale(sub, 200*time.Millisecond)

	// One matching, one not.
	seedObjects(t, s, []string{"/testobjs/other/keep-out"})
	seedObjects(t, s, []string{"/testobjs/only/kept"})

	ev := waitForKey(t, sub, "/registry/testobjs/only/kept", 5*time.Second)
	if !ev.isCreate {
		t.Errorf("expected create, got %+v", ev)
	}
	// Ensure the "keep-out" key never arrives in the subscriber.
	timeout := time.After(500 * time.Millisecond)
	for {
		select {
		case e, ok := <-sub.events():
			if !ok {
				return
			}
			if e.key == "/registry/testobjs/other/keep-out" {
				t.Errorf("subscriber received unfiltered key: %s", e.key)
				return
			}
		case <-timeout:
			return
		}
	}
}

func TestParseChangefeedRow_Resolved(t *testing.T) {
	body := []byte(`{"resolved":"1782867115401424000.0000000000"}`)
	ev, err := parseChangefeedRow(nil, body)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if !ev.isProgress {
		t.Errorf("isProgress = false; want true")
	}
	if ev.mvccHLC != "1782867115401424000.0000000000" {
		t.Errorf("mvcc = %q", ev.mvccHLC)
	}
}

func TestParseChangefeedRow_WrappedCreate(t *testing.T) {
	hexed := "5c78" + hex.EncodeToString([]byte("hello"))[len("5c78"):]
	_ = hexed
	body := fmt.Sprintf(`{
		"after":  {"key":"/foo","value":"\\x%s"},
		"before": null,
		"mvcc_timestamp":"1782867115401424000.0000000000"
	}`, hex.EncodeToString([]byte("hello")))
	ev, err := parseChangefeedRow([]byte(`["/foo"]`), []byte(body))
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if !ev.isCreate {
		t.Errorf("isCreate = false")
	}
	if string(ev.newValue) != "hello" {
		t.Errorf("newValue = %q, want hello", string(ev.newValue))
	}
}

func TestParseChangefeedRow_WrappedDelete(t *testing.T) {
	body := fmt.Sprintf(`{
		"after":  null,
		"before": {"key":"/foo","value":"\\x%s"},
		"mvcc_timestamp":"1782867115401424000.0000000000"
	}`, hex.EncodeToString([]byte("gone")))
	ev, err := parseChangefeedRow([]byte(`["/foo"]`), []byte(body))
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if !ev.isDelete {
		t.Errorf("isDelete = false")
	}
	if string(ev.oldValue) != "gone" {
		t.Errorf("oldValue = %q, want gone", string(ev.oldValue))
	}
}

// deleteRow issues a raw DELETE for the given prepared key. Used by tests
// that want to trigger a delete without going through the storage.Delete
// path (which decodes and needs a valid object).
func deleteRow(s *store, preparedKey string) error {
	_, err := s.pool.Exec(context.Background(), `DELETE FROM kv WHERE key = $1`, preparedKey)
	return err
}

// drainStale discards any events available within the given window. Used
// to skip past historical rows from the initial changefeed scan.
func drainStale(sub *changefeedSubscriber, window time.Duration) {
	deadline := time.After(window)
	for {
		select {
		case _, ok := <-sub.events():
			if !ok {
				return
			}
		case <-deadline:
			return
		}
	}
}

// waitForKey blocks until an event for the exact key arrives or times out.
// Progress events are skipped.
func waitForKey(t *testing.T, sub *changefeedSubscriber, key string, d time.Duration) changefeedEvent {
	t.Helper()
	deadline := time.After(d)
	for {
		select {
		case ev, ok := <-sub.events():
			if !ok {
				t.Fatalf("subscription closed before seeing %s", key)
			}
			if ev.isProgress {
				continue
			}
			if ev.key == key {
				return ev
			}
		case <-deadline:
			t.Fatalf("timed out waiting for key %s", key)
		}
	}
}

// mutateData returns a tryUpdate that overwrites the object's Data field.
func mutateData(next string) func(runtime.Object, storage.ResponseMeta) (runtime.Object, *uint64, error) {
	return func(in runtime.Object, _ storage.ResponseMeta) (runtime.Object, *uint64, error) {
		cur := in.(*testObj)
		cur.Data = next
		return cur, nil, nil
	}
}
