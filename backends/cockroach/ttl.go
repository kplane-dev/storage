package cockroach

import (
	"context"
	"sync"
	"time"

	"k8s.io/klog/v2"
)

// defaultTTLScanCadence balances etcd's 500ms lease-check cadence with
// avoiding needless work in steady state. One indexed range scan / second
// per apiserver replica is negligible for the kv table's expected
// cardinality (dozens of TTL'd rows for internal leases).
const defaultTTLScanCadence = time.Second

// defaultTTLDeleteBatch caps rows deleted per tick. Chosen so a burst of
// simultaneous expirations gets drained across a small number of ticks
// without a single statement holding write intents for too long.
const defaultTTLDeleteBatch = 100

// TTLScanner deletes expired rows on a periodic cadence. The changefeed
// picks up the resulting DELETEs and emits real watch events; no
// synthetic events are produced here. Cockroach's built-in row-level TTL
// job is left enabled as a backup for anything the scanner misses
// (e.g. across a process crash), but the scanner is the fast path.
type TTLScanner struct {
	store   *store
	cadence time.Duration
	batch   int

	cancel context.CancelFunc
	done   chan struct{}
	once   sync.Once
}

// NewTTLScanner returns a scanner for the given store. Start begins the
// loop; Stop cancels it. Cadence <=0 uses defaultTTLScanCadence.
func NewTTLScanner(s *store, cadence time.Duration) *TTLScanner {
	if cadence <= 0 {
		cadence = defaultTTLScanCadence
	}
	return &TTLScanner{
		store:   s,
		cadence: cadence,
		batch:   defaultTTLDeleteBatch,
		done:    make(chan struct{}),
	}
}

// Start launches the scanner goroutine. Idempotent.
func (t *TTLScanner) Start(ctx context.Context) {
	t.once.Do(func() {
		runCtx, cancel := context.WithCancel(ctx)
		t.cancel = cancel
		go t.run(runCtx)
	})
}

// Stop cancels the scanner and waits for the goroutine to exit.
func (t *TTLScanner) Stop() {
	if t.cancel != nil {
		t.cancel()
	}
	<-t.done
}

func (t *TTLScanner) run(ctx context.Context) {
	defer close(t.done)
	tick := time.NewTicker(t.cadence)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
			t.sweep(ctx)
		}
	}
}

// sweep deletes up to batch expired rows. Serializable isolation resolves
// the refresh race atomically: if a client's UPDATE(expire_at=future)
// commits between our SELECT and DELETE, our DELETE's WHERE misses the
// row and it survives. No CAS logic required client-side.
func (t *TTLScanner) sweep(ctx context.Context) {
	rows, err := t.store.pool.Query(ctx, `
		DELETE FROM kv
		WHERE expire_at IS NOT NULL AND expire_at <= now()
		LIMIT $1
		RETURNING key`, t.batch)
	if err != nil {
		klog.V(4).Infof("cockroach ttl scanner: delete failed: %v", err)
		return
	}
	defer rows.Close()

	var count int
	for rows.Next() {
		var key string
		if err := rows.Scan(&key); err != nil {
			klog.V(4).Infof("cockroach ttl scanner: scan failed: %v", err)
			continue
		}
		count++
	}
	if err := rows.Err(); err != nil {
		klog.V(4).Infof("cockroach ttl scanner: iter: %v", err)
	}
	if count > 0 {
		klog.V(4).Infof("cockroach ttl scanner: deleted %d expired rows", count)
	}
}
