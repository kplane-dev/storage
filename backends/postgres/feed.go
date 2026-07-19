package postgres

import (
	"context"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"k8s.io/klog/v2"
)

// feedEvent is a normalized change from the log, fanned out to subscribers.
// Delete rows carry newValue=nil and oldValue=the pre-image; create rows
// carry oldValue=nil. Progress events carry no key and exist only to
// advance watchers' resource versions.
type feedEvent struct {
	key        string
	newValue   []byte // storage-encoded bytes; nil on delete/progress
	oldValue   []byte // storage-encoded bytes; nil on create/progress
	rv         uint64
	isCreate   bool
	isDelete   bool
	isProgress bool
}

// feedSubscriber is one per Watch call. Events whose key starts with
// keyPrefix (and all progress events) are forwarded onto ch. Overflow
// closes the subscription so the Watch owner re-lists — same convention as
// etcd's slow-consumer policy.
type feedSubscriber struct {
	keyPrefix string
	ch        chan feedEvent
	id        uint64

	mu     sync.Mutex
	closed bool
}

// Feed tails the append-only kv log and fans changes out to subscribers. A
// single Feed runs per process. It advances a gap-safe cursor: each poll
// delivers rows with id in (emitted, safe], where `safe` is the highest id
// all of whose predecessors have committed (see currentSafeRV). Delivering
// only up to `safe` — never up to a raw max(id) — is what prevents a
// concurrently-committed lower id from being skipped.
//
// Polling is the authoritative, durably-resumable path (the cursor lives in
// the log's own ids, so a dropped notification loses nothing). LISTEN/NOTIFY
// is only a low-latency wakeup; a periodic timer is the backstop.
type Feed struct {
	pool    *pgxpool.Pool
	connCfg *pgx.ConnConfig

	mu          sync.RWMutex
	subscribers map[uint64]*feedSubscriber
	nextID      uint64

	emitted int64        // highest id dispatched; written only by pollLoop
	safe    atomic.Int64 // highest fully-committed id; read by GetCurrentResourceVersion

	wake   chan struct{}
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// pollBackstop is the maximum time the feed waits between polls when no
// NOTIFY arrives. Notifications drive sub-100ms latency in the common case;
// this bound only matters if a notification is missed (e.g. across a listen
// reconnect).
const pollBackstop = time.Second

// NewFeed constructs a Feed. pool is used for polling; connCfg opens the
// dedicated LISTEN connection. Nothing runs until Start.
func NewFeed(pool *pgxpool.Pool, connCfg *pgx.ConnConfig) *Feed {
	return &Feed{
		pool:        pool,
		connCfg:     connCfg,
		subscribers: make(map[uint64]*feedSubscriber),
		wake:        make(chan struct{}, 1),
	}
}

// Start launches the poll and listen goroutines. They run until Stop or ctx
// cancellation.
func (f *Feed) Start(ctx context.Context) {
	runCtx, cancel := context.WithCancel(ctx)
	f.cancel = cancel
	f.wg.Add(2)
	go f.pollLoop(runCtx)
	go f.listenLoop(runCtx)
}

// Stop cancels the goroutines and closes all subscriber channels.
func (f *Feed) Stop() {
	if f.cancel != nil {
		f.cancel()
	}
	f.wg.Wait()
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, sub := range f.subscribers {
		sub.close()
	}
	f.subscribers = nil
}

// SafeRV returns the highest fully-committed log id the feed has observed.
// Used by GetCurrentResourceVersion as a scan lower bound.
func (f *Feed) SafeRV() int64 { return f.safe.Load() }

// Subscribe returns a subscriber receiving every change whose key starts
// with keyPrefix (plus all progress events). The caller must Unsubscribe.
func (f *Feed) Subscribe(keyPrefix string, bufSize int) *feedSubscriber {
	if bufSize < 64 {
		bufSize = 64
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.nextID++
	sub := &feedSubscriber{
		keyPrefix: keyPrefix,
		ch:        make(chan feedEvent, bufSize),
		id:        f.nextID,
	}
	f.subscribers[sub.id] = sub
	return sub
}

// Unsubscribe removes the subscriber and closes its channel. Idempotent.
func (f *Feed) Unsubscribe(sub *feedSubscriber) {
	f.mu.Lock()
	delete(f.subscribers, sub.id)
	f.mu.Unlock()
	sub.close()
}

// PublishProgress advances the feed to at least rv and fans a progress event
// out to every subscriber. Called by store.RequestWatchProgress when the
// cacher needs the watchCache advanced sooner than the backstop poll would.
func (f *Feed) PublishProgress(rv uint64) {
	if int64(rv) > f.safe.Load() {
		f.safe.Store(int64(rv))
	}
	f.dispatch(feedEvent{isProgress: true, rv: rv})
}

// pollLoop polls the log on every wakeup and on a periodic backstop.
func (f *Feed) pollLoop(ctx context.Context) {
	defer f.wg.Done()
	tick := time.NewTicker(pollBackstop)
	defer tick.Stop()
	f.poll(ctx) // prime the cursor immediately
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
		case <-f.wake:
		}
		f.poll(ctx)
	}
}

// poll delivers every committed row with id in (emitted, safe], then a
// progress event carrying the new safe watermark.
func (f *Feed) poll(ctx context.Context) {
	safe, err := currentSafeRV(ctx, f.pool, f.emitted)
	if err != nil {
		if ctx.Err() == nil {
			klog.V(4).Infof("postgres feed: compute watermark: %v", err)
		}
		return
	}
	if safe <= f.emitted {
		return
	}
	rows, err := f.pool.Query(ctx, `
		SELECT name, value, prev_value, created, deleted, id
		FROM kv WHERE id > $1 AND id <= $2 ORDER BY id`, f.emitted, safe)
	if err != nil {
		if ctx.Err() == nil {
			klog.V(4).Infof("postgres feed: poll query: %v", err)
		}
		return
	}
	defer rows.Close()
	for rows.Next() {
		var (
			name    string
			value   []byte
			prev    []byte
			created bool
			deleted bool
			id      int64
		)
		if err := rows.Scan(&name, &value, &prev, &created, &deleted, &id); err != nil {
			klog.V(4).Infof("postgres feed: scan: %v", err)
			return
		}
		f.dispatch(feedEvent{
			key:      name,
			newValue: value,
			oldValue: prev,
			rv:       idToRV(id),
			isCreate: created,
			isDelete: deleted,
		})
	}
	if err := rows.Err(); err != nil {
		klog.V(4).Infof("postgres feed: poll iter: %v", err)
		return
	}
	f.emitted = safe
	f.safe.Store(safe)
	// A progress event lets watchers whose prefix matched none of the rows
	// above still advance their resource version to the new head — the
	// append-only analog of a resolved-timestamp bookmark.
	f.dispatch(feedEvent{isProgress: true, rv: idToRV(safe)})
}

// listenLoop maintains a dedicated LISTEN connection and pokes the poll loop
// on every notification. On disconnect it reconnects with backoff; because
// the poll cursor is authoritative, notifications missed during a reconnect
// are recovered by the next backstop poll.
func (f *Feed) listenLoop(ctx context.Context) {
	defer f.wg.Done()
	backoff := 200 * time.Millisecond
	for {
		if ctx.Err() != nil {
			return
		}
		if err := f.listenOnce(ctx); err != nil {
			if ctx.Err() != nil {
				return
			}
			klog.V(2).Infof("postgres feed: listener exited: %v; reconnecting after %v", err, backoff)
			select {
			case <-ctx.Done():
				return
			case <-time.After(backoff):
			}
			if backoff < 5*time.Second {
				backoff *= 2
			}
			continue
		}
		backoff = 200 * time.Millisecond
	}
}

func (f *Feed) listenOnce(ctx context.Context) error {
	conn, err := pgx.ConnectConfig(ctx, f.connCfg)
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close(context.Background()) }()
	if _, err := conn.Exec(ctx, "LISTEN "+notifyChannel); err != nil {
		return err
	}
	// A notification may have raced in before LISTEN completed; poke once so
	// we don't wait for the backstop.
	f.poke()
	for {
		if _, err := conn.WaitForNotification(ctx); err != nil {
			return err
		}
		f.poke()
	}
}

// poke requests a poll without blocking. The wake channel is depth-1, so
// bursts of notifications coalesce into a single poll.
func (f *Feed) poke() {
	select {
	case f.wake <- struct{}{}:
	default:
	}
}

func (f *Feed) dispatch(ev feedEvent) {
	f.mu.RLock()
	subs := make([]*feedSubscriber, 0, len(f.subscribers))
	for _, sub := range f.subscribers {
		subs = append(subs, sub)
	}
	f.mu.RUnlock()

	for _, sub := range subs {
		if !ev.isProgress && !strings.HasPrefix(ev.key, sub.keyPrefix) {
			continue
		}
		sub.send(ev)
	}
}

func (s *feedSubscriber) send(ev feedEvent) {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return
	}
	s.mu.Unlock()
	select {
	case s.ch <- ev:
	default:
		// Slow consumer: close it. The Watch owner sees its channel close
		// and re-establishes through the cacher.
		s.close()
	}
}

func (s *feedSubscriber) close() {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return
	}
	s.closed = true
	close(s.ch)
	s.mu.Unlock()
}

// events returns the subscriber's receive channel.
func (s *feedSubscriber) events() <-chan feedEvent { return s.ch }
