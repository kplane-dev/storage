package postgres

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"k8s.io/klog/v2"
)

// defaultTTLScanCadence approximates etcd's sub-second lease-expiry latency.
// One indexed range scan per second per replica is negligible for the kv
// log's expected TTL cardinality (a handful of internal leases).
const defaultTTLScanCadence = time.Second

// defaultTTLDeleteBatch caps tombstones written per tick so a burst of
// simultaneous expirations drains across a few ticks rather than holding one
// transaction open over many rows.
const defaultTTLDeleteBatch = 100

// TTLScanner appends tombstone rows for expired keys on a periodic cadence.
// PostgreSQL has no built-in row-level TTL, so this scanner is the only
// expiry mechanism — but it fits the append-only model cleanly: it INSERTs a
// tombstone (it never issues a physical DELETE), the feed picks that up on
// its next poll, and watchers see an ordinary DELETED event. Reads are
// already protected independently by the `expire_at > now()` filter on every
// Get/GetList, so an expired row is invisible to reads even before its
// tombstone lands.
type TTLScanner struct {
	pool    *pgxpool.Pool
	cadence time.Duration
	batch   int

	cancel context.CancelFunc
	done   chan struct{}
	once   sync.Once
}

// NewTTLScanner returns a scanner bound to the pool. Cadence <= 0 uses the
// default.
func NewTTLScanner(pool *pgxpool.Pool, cadence time.Duration) *TTLScanner {
	if cadence <= 0 {
		cadence = defaultTTLScanCadence
	}
	return &TTLScanner{
		pool:    pool,
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

// candidate is an expired-but-still-current key awaiting a tombstone.
type candidate struct {
	id   int64
	name string
}

// sweep finds expired keys that are still the current row for their name and
// tombstones each one. The NOT EXISTS clause excludes keys already
// superseded by a newer write (a lease refresh or an explicit delete), so we
// never tombstone a key that's since been renewed.
func (t *TTLScanner) sweep(ctx context.Context) {
	rows, err := t.pool.Query(ctx, `
		SELECT k.id, k.name FROM kv k
		WHERE k.expire_at IS NOT NULL AND k.deleted = false AND k.expire_at <= now()
		  AND NOT EXISTS (SELECT 1 FROM kv n WHERE n.name = k.name AND n.id > k.id)
		LIMIT $1`, t.batch)
	if err != nil {
		if ctx.Err() == nil {
			klog.V(4).Infof("postgres ttl scanner: select: %v", err)
		}
		return
	}
	var candidates []candidate
	for rows.Next() {
		var c candidate
		if err := rows.Scan(&c.id, &c.name); err != nil {
			rows.Close()
			klog.V(4).Infof("postgres ttl scanner: scan: %v", err)
			return
		}
		candidates = append(candidates, c)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		klog.V(4).Infof("postgres ttl scanner: iter: %v", err)
		return
	}

	var deleted int
	for _, c := range candidates {
		if t.tombstone(ctx, c) {
			deleted++
		}
	}
	if deleted > 0 {
		klog.V(4).Infof("postgres ttl scanner: tombstoned %d expired rows", deleted)
	}
}

// tombstone appends a tombstone for one candidate inside a SERIALIZABLE
// transaction. The re-read under the transaction is a CAS against a
// concurrent refresh: if a newer row for the name landed, the NOT EXISTS
// re-check yields no row and we skip; if the refresh commits after our read,
// serializable isolation aborts the transaction and execTx retries, at which
// point the candidate no longer qualifies. Either way a renewed lease is
// never wrongly deleted.
func (t *TTLScanner) tombstone(ctx context.Context, c candidate) bool {
	var wrote bool
	err := execTx(ctx, t.pool, func(tx pgx.Tx) error {
		var val []byte
		err := tx.QueryRow(ctx, `
			SELECT value FROM kv k
			WHERE k.id = $1 AND k.name = $2 AND k.deleted = false
			  AND k.expire_at IS NOT NULL AND k.expire_at <= now()
			  AND NOT EXISTS (SELECT 1 FROM kv n WHERE n.name = k.name AND n.id > k.id)`,
			c.id, c.name,
		).Scan(&val)
		if errors.Is(err, pgx.ErrNoRows) {
			return nil // superseded or refreshed; nothing to do
		}
		if err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `
			INSERT INTO kv (name, value, prev_value, created, deleted)
			VALUES ($1, NULL, $2, false, true)`,
			c.name, val,
		); err != nil {
			return err
		}
		wrote = true
		return nil
	})
	if err != nil {
		klog.V(4).Infof("postgres ttl scanner: tombstone %q: %v", c.name, err)
		return false
	}
	return wrote
}
