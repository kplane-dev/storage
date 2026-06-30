package spanner

import (
	"container/heap"
	"context"
	"sync"
	"time"

	"cloud.google.com/go/spanner"
	"google.golang.org/api/iterator"
	"k8s.io/klog/v2"
)

// watchEvent is the internal representation of a storage mutation,
// published through the broadcaster to all active watchers.
type watchEvent struct {
	key       string
	value     []byte
	prevValue []byte
	rev       int64

	isCreated  bool
	isDeleted  bool
	isProgress bool
}

// subscription is a channel-based subscription to broadcast events.
type subscription struct {
	ch     chan watchEvent
	id     uint64
	closed bool
}

// Broadcaster fans out storage mutation events to all subscribed watchers
// in monotonically non-decreasing revision order — matching the contract
// the upstream cacher's watchCache relies on for its binary-search lookup
// (staging/src/k8s.io/apiserver/pkg/storage/cacher/watch_cache.go:982).
//
// Why this matters: Spanner assigns each commit a unique TrueTime
// timestamp, and TrueTime guarantees timestamps are globally monotonic
// across commits. But the *goroutines* that call client.Apply return to
// their callers in OS-scheduling order, not commit-timestamp order. When
// the apiserver bootstrap creates ~50 things in parallel, publishing
// inline from each goroutine produces an out-of-order event stream;
// the watchCache then mis-indexes events and downstream watches silently
// miss them — manifesting as "namespace not found" admission failures
// that never recover.
//
// The fix preserves Spanner's high write throughput (no global write
// serialization) while restoring etcd's ordered-watch contract: writes
// acquire a Ticket BEFORE calling Apply, then either Publish (on success)
// or Cancel (on failure) the Ticket. A single dispatcher goroutine drains
// a min-heap of pending events in revision order, but only flushes once
// every outstanding Ticket has been resolved — at which point TrueTime
// guarantees no future Apply can commit at a revision below anything in
// the heap, so popping in heap order produces a monotonic stream.
//
// The Ticket abstraction (acquire → defer Cancel → Publish-on-success)
// makes correct pairing the path of least resistance; misuse would
// stall the dispatcher, which fails loudly via watch starvation rather
// than corrupting event order.
type Broadcaster struct {
	mu          sync.Mutex
	cond        *sync.Cond
	subscribers map[uint64]*subscription
	nextID      uint64

	// heap holds events queued by Ticket.Publish, ordered by ascending rev.
	heap eventHeap

	// pendingTickets counts outstanding write Tickets — Apply calls in
	// flight whose Publish or Cancel hasn't been called yet. The
	// dispatcher waits until pendingTickets == 0 before flushing the heap,
	// because at that point TrueTime guarantees no future Apply can
	// produce an RV below anything we've buffered.
	pendingTickets int

	highWaterMark int64

	stop chan struct{}
	done chan struct{}

	// ttlScannerDone is closed when the TTL scanner goroutine exits (only
	// started if StartTTLScanner was called).
	ttlScannerDone chan struct{}
}

// NewBroadcaster creates a Broadcaster and starts its dispatcher goroutine.
func NewBroadcaster() *Broadcaster {
	b := &Broadcaster{
		subscribers: make(map[uint64]*subscription),
		stop:        make(chan struct{}),
		done:        make(chan struct{}),
	}
	b.cond = sync.NewCond(&b.mu)
	go b.dispatch()
	return b
}

// Close stops the dispatcher. Provided for tests; production broadcasters
// run for the lifetime of the process.
func (b *Broadcaster) Close() {
	b.mu.Lock()
	select {
	case <-b.stop:
		b.mu.Unlock()
		return
	default:
		close(b.stop)
	}
	b.cond.Broadcast()
	b.mu.Unlock()
	<-b.done
	// Wait for the TTL scanner to exit too, if it was started.
	if b.ttlScannerDone != nil {
		<-b.ttlScannerDone
	}
}

// StartTTLScanner starts the periodic TTL eviction loop. Idempotent — a
// second call is a no-op. Safe to call once per broadcaster, after the
// store is fully constructed.
//
// The scanner is the watch-side half of the TTL eviction design: every
// scanCadence the loop SELECTs rows whose expire_at falls in
// (watermark, CURRENT_TIMESTAMP()], then for each one acquires a write
// Ticket, re-reads the row under that ticket as a CAS check (to rule out
// a concurrent refresh that re-set expire_at into the future), and on
// confirmation publishes a synthetic Delete event into the broadcaster.
// Watch consumers see this event identically to a real Delete — same
// shape, same path through processEvent.
//
// The CAS check inside the ticket serializes against concurrent Updates:
//   - if a refresh committed before our AcquireWrite, our re-read sees
//     the new expire_at and we Cancel the ticket
//   - if a refresh is queued behind our ticket, our Delete publishes
//     first; the refresh follows with a later rev and the watcher sees
//     Delete-then-Added, which is the correct sequence for "row expired,
//     then was recreated"
//
// Physical row cleanup is handled out-of-band by Spanner's ROW DELETION
// POLICY (declared on the kv table). The scanner only emits watch events;
// it never issues DELETEs. Reads are protected independently by the
// `expire_at IS NULL OR expire_at > CURRENT_TIMESTAMP()` filter on every
// Get/GetList.
func (b *Broadcaster) StartTTLScanner(s *store, scanCadence time.Duration) {
	b.mu.Lock()
	if b.ttlScannerDone != nil {
		b.mu.Unlock()
		return
	}
	b.ttlScannerDone = make(chan struct{})
	b.mu.Unlock()
	go b.ttlScanLoop(s, scanCadence)
}

func (b *Broadcaster) ttlScanLoop(s *store, cadence time.Duration) {
	defer close(b.ttlScannerDone)

	// watermark advances strictly forward — once we've handled
	// expirations up to T, we never re-scan the window before T.
	// In-memory only; restart rescans from the zero value, which means
	// already-published Deletes for already-physically-deleted rows
	// may re-fire. That's harmless: processEvent emits a watch.Deleted
	// with prevValue decoded, the cacher's watchCache treats it as a
	// duplicate Delete (idempotent — removing a key already gone is a
	// no-op).
	var watermark time.Time

	tick := time.NewTicker(cadence)
	defer tick.Stop()

	for {
		select {
		case <-b.stop:
			return
		case <-tick.C:
			b.ttlScanOnce(s, &watermark)
		}
	}
}

// ttlScanOnce queries for rows whose expire_at falls in (watermark, now]
// and emits synthetic Delete events for each one that's still genuinely
// expired (CAS-verified under a write Ticket).
func (b *Broadcaster) ttlScanOnce(s *store, watermark *time.Time) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Use a Read-Only snapshot so concurrent writes don't race with the
	// initial scan. The CAS check (below) re-reads each candidate
	// under a fresh transaction; that's where ordering guarantees come
	// from.
	stmt := spanner.Statement{
		SQL: `SELECT key, value, expire_at FROM kv@{FORCE_INDEX=kv_by_expire_at}
              WHERE expire_at IS NOT NULL
                AND expire_at > @watermark
                AND expire_at <= CURRENT_TIMESTAMP()
              ORDER BY expire_at`,
		Params: map[string]interface{}{"watermark": *watermark},
	}
	iter := s.client.Single().Query(ctx, stmt)
	defer iter.Stop()

	type candidate struct {
		key      string
		value    []byte
		expireAt time.Time
	}
	var candidates []candidate
	for {
		row, err := iter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			klog.V(4).Infof("ttlScan: SELECT failed: %v", err)
			return
		}
		var c candidate
		if err := row.Columns(&c.key, &c.value, &c.expireAt); err != nil {
			klog.V(4).Infof("ttlScan: row decode failed: %v", err)
			continue
		}
		candidates = append(candidates, c)
	}

	if len(candidates) == 0 {
		// Advance watermark to the candidate-list cutoff (scan-time "now").
		// Approximating with client time is fine — watermark monotonicity
		// is the only invariant we need, and a few-ms client/server skew
		// is well below the scan cadence.
		now := time.Now()
		if now.After(*watermark) {
			*watermark = now
		}
		return
	}

	// CAS-verify and emit one at a time. Per-candidate is the right
	// granularity: each AcquireWrite serializes against writes for that
	// key only at the dispatcher level (not at the Spanner level), so
	// holding a ticket is cheap.
	for _, c := range candidates {
		b.emitTTLDelete(ctx, s, c.key, c.value, c.expireAt)
		if c.expireAt.After(*watermark) {
			*watermark = c.expireAt
		}
	}
}

// emitTTLDelete CAS-verifies the candidate is still expired and publishes
// a synthetic Delete event. See StartTTLScanner for the correctness
// argument.
func (b *Broadcaster) emitTTLDelete(ctx context.Context, s *store, key string, value []byte, expireAt time.Time) {
	ticket := b.AcquireWrite()
	defer ticket.Cancel()

	// Re-read under the ticket. If the row was refreshed after our scan
	// (expire_at moved into the future) or physically removed (NOT_FOUND
	// from row-deletion-policy), skip the emit.
	iter := s.client.Single().Query(ctx, spanner.Statement{
		SQL:    `SELECT expire_at FROM kv WHERE key = @key`,
		Params: map[string]interface{}{"key": key},
	})
	defer iter.Stop()
	row, err := iter.Next()
	if err == iterator.Done {
		return // already physically deleted
	}
	if err != nil {
		klog.V(4).Infof("ttlScan emit: CAS read failed key=%q: %v", key, err)
		return
	}
	var curExpire spanner.NullTime
	if err := row.Column(0, &curExpire); err != nil {
		return
	}
	if !curExpire.Valid || curExpire.Time.After(time.Now()) {
		return // refreshed; not expired anymore
	}

	// Use scanner-now as the synthetic Delete's rev, matching etcd's
	// "lease expiration commit revision" semantics — the rev reflects
	// when we processed the expiration, not when it nominally fired.
	// This preserves the broadcaster's monotonic-rev invariant against
	// any subsequent real commit on this key.
	rev := time.Now().UnixNano()
	ticket.Publish(watchEvent{
		key:       key,
		prevValue: value,
		rev:       rev,
		isDeleted: true,
	})
}

// Ticket represents one outstanding write. The store acquires a Ticket
// BEFORE calling Apply and resolves it after — either via Publish (when
// the write committed and produced an event) or Cancel (on error or
// no-op). The dispatcher cannot flush its event heap until every
// outstanding Ticket has been resolved.
//
// Idiomatic use is `defer t.Cancel()` immediately after Acquire — Cancel
// on an already-Published Ticket is a no-op, so the defer is harmless on
// the success path and guarantees correctness on every error path.
type Ticket struct {
	b        *Broadcaster
	resolved bool
}

// AcquireWrite reserves a slot for an upcoming write. Must be called
// BEFORE s.client.Apply (or s.client.ReadWriteTransaction). The returned
// Ticket MUST be resolved exactly once via Publish or Cancel — defer
// Cancel immediately to handle every error path.
func (b *Broadcaster) AcquireWrite() *Ticket {
	b.mu.Lock()
	b.pendingTickets++
	b.mu.Unlock()
	return &Ticket{b: b}
}

// Publish releases the Ticket and enqueues e for ordered delivery. Safe
// to call only once; subsequent Publish/Cancel calls on the same Ticket
// are no-ops.
func (t *Ticket) Publish(e watchEvent) {
	t.b.mu.Lock()
	if t.resolved {
		t.b.mu.Unlock()
		return
	}
	t.resolved = true
	if e.rev > t.b.highWaterMark {
		t.b.highWaterMark = e.rev
	}
	heap.Push(&t.b.heap, e)
	t.b.pendingTickets--
	t.b.cond.Broadcast()
	t.b.mu.Unlock()
}

// Cancel releases the Ticket without publishing an event. Idempotent.
func (t *Ticket) Cancel() {
	t.b.mu.Lock()
	if t.resolved {
		t.b.mu.Unlock()
		return
	}
	t.resolved = true
	t.b.pendingTickets--
	t.b.cond.Broadcast()
	t.b.mu.Unlock()
}

// PublishProgress emits a synthetic bookmark/progress event without
// participating in the write-ordering protocol. Progress events carry the
// current Spanner read timestamp; since that read timestamp comes from
// the same TrueTime clock as Apply's commit timestamps, the dispatcher
// orders these correctly alongside real writes.
func (b *Broadcaster) PublishProgress(e watchEvent) {
	b.mu.Lock()
	if e.rev > b.highWaterMark {
		b.highWaterMark = e.rev
	}
	heap.Push(&b.heap, e)
	b.cond.Broadcast()
	b.mu.Unlock()
}

func (b *Broadcaster) dispatch() {
	defer close(b.done)
	b.mu.Lock()
	defer b.mu.Unlock()

	for {
		// Drain everything we can — but only when no Ticket is in flight,
		// which is what makes ordering correct (no future Publish can
		// produce an RV below anything currently in the heap).
		for b.heap.Len() > 0 && b.pendingTickets == 0 {
			ev := heap.Pop(&b.heap).(watchEvent)
			b.deliverLocked(ev)
		}

		select {
		case <-b.stop:
			return
		default:
		}

		b.cond.Wait()
	}
}

// deliverLocked fans an event out to all subscribers. Caller holds b.mu.
// Subscribers whose buffer is full are closed (same behavior etcd uses
// for slow watches: drop the connection, force the client to re-list).
func (b *Broadcaster) deliverLocked(e watchEvent) {
	if klog.V(4).Enabled() {
		klog.V(4).Infof("broadcaster.deliver: key=%q rev=%d isCreated=%v isDeleted=%v isProgress=%v subscribers=%d",
			e.key, e.rev, e.isCreated, e.isDeleted, e.isProgress, len(b.subscribers))
	}
	for id, sub := range b.subscribers {
		if sub.closed {
			continue
		}
		select {
		case sub.ch <- e:
		default:
			sub.closed = true
			close(sub.ch)
			delete(b.subscribers, id)
		}
	}
}

// Subscribe creates a new subscription. The returned channel receives all
// events delivered after the subscription is created. bufSize controls
// the channel buffer depth.
func (b *Broadcaster) Subscribe(bufSize int) *subscription {
	b.mu.Lock()
	defer b.mu.Unlock()

	if bufSize < 64 {
		bufSize = 64
	}

	sub := &subscription{
		ch: make(chan watchEvent, bufSize),
		id: b.nextID,
	}
	b.nextID++
	b.subscribers[sub.id] = sub
	return sub
}

// Unsubscribe removes a subscription and closes its channel.
func (b *Broadcaster) Unsubscribe(sub *subscription) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if sub.closed {
		return
	}
	sub.closed = true
	close(sub.ch)
	delete(b.subscribers, sub.id)
}

// HighWaterMark returns the highest revision the broadcaster has seen.
func (b *Broadcaster) HighWaterMark() int64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.highWaterMark
}

// eventHeap is a min-heap of watchEvents ordered by rev. heap.Interface.
type eventHeap []watchEvent

func (h eventHeap) Len() int            { return len(h) }
func (h eventHeap) Less(i, j int) bool  { return h[i].rev < h[j].rev }
func (h eventHeap) Swap(i, j int)       { h[i], h[j] = h[j], h[i] }
func (h *eventHeap) Push(x interface{}) { *h = append(*h, x.(watchEvent)) }
func (h *eventHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[:n-1]
	return x
}
