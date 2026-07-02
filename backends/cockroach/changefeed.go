package cockroach

import (
	"context"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5"
	"k8s.io/klog/v2"
)

// changefeedEvent is a normalized row emitted by the changefeed. Delete
// events carry after=nil; the diff option gives us before for prevValue.
type changefeedEvent struct {
	key        string
	newValue   []byte // storage-encoded bytes; nil on delete
	oldValue   []byte // storage-encoded bytes; nil on create
	mvccHLC    string
	isCreate   bool
	isDelete   bool
	isProgress bool // resolved-timestamp bookmark row
}

// changefeedSubscriber is one per Watch call. Events matching keyPrefix are
// forwarded onto ch. Overflow closes the subscription so upstream can
// re-open its watch — same convention as etcd's slow-consumer behavior.
type changefeedSubscriber struct {
	keyPrefix string
	ch        chan changefeedEvent
	id        uint64

	mu     sync.Mutex
	closed bool
}

// ChangefeedSubscription runs a single CockroachDB changefeed on the kv
// table and fans events out to registered subscribers. Reconnects on
// disconnect using WITH cursor=<lastResolved> so no events are lost.
type ChangefeedSubscription struct {
	connCfg *pgx.ConnConfig

	mu          sync.RWMutex
	subscribers map[uint64]*changefeedSubscriber
	nextID      uint64
	resolvedHLC atomic.Value // stores string

	cancel context.CancelFunc
	done   chan struct{}
}

// defaultApplicationName tags the reader's SQL session so operators (and
// tests) can identify it via SHOW SESSIONS / SHOW CLUSTER QUERIES.
const defaultApplicationName = "kplane-cockroach-changefeed"

// NewChangefeedSubscription constructs a subscription bound to connCfg. It
// doesn't dial until Start is called. If connCfg has no application_name
// runtime param, we set a stable default so the changefeed session is
// identifiable in cluster observability views.
func NewChangefeedSubscription(connCfg *pgx.ConnConfig) *ChangefeedSubscription {
	if connCfg.RuntimeParams == nil {
		connCfg.RuntimeParams = make(map[string]string)
	}
	if _, ok := connCfg.RuntimeParams["application_name"]; !ok {
		connCfg.RuntimeParams["application_name"] = defaultApplicationName
	}
	c := &ChangefeedSubscription{
		connCfg:     connCfg,
		subscribers: make(map[uint64]*changefeedSubscriber),
		done:        make(chan struct{}),
	}
	c.resolvedHLC.Store("")
	return c
}

// Start launches the reader goroutine. It runs until Stop or ctx is
// cancelled; on disconnect it backoffs and reconnects from the last
// resolved timestamp.
func (c *ChangefeedSubscription) Start(ctx context.Context) {
	runCtx, cancel := context.WithCancel(ctx)
	c.cancel = cancel
	go c.run(runCtx)
}

// Stop cancels the reader's context and closes all subscriber channels.
func (c *ChangefeedSubscription) Stop() {
	if c.cancel != nil {
		c.cancel()
	}
	<-c.done

	c.mu.Lock()
	defer c.mu.Unlock()
	for _, sub := range c.subscribers {
		sub.close()
	}
	c.subscribers = nil
}

// Subscribe returns a subscriber that receives every change whose key
// starts with keyPrefix. The caller must call Unsubscribe when done.
func (c *ChangefeedSubscription) Subscribe(keyPrefix string, bufSize int) *changefeedSubscriber {
	if bufSize < 64 {
		bufSize = 64
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.nextID++
	sub := &changefeedSubscriber{
		keyPrefix: keyPrefix,
		ch:        make(chan changefeedEvent, bufSize),
		id:        c.nextID,
	}
	c.subscribers[sub.id] = sub
	return sub
}

// Unsubscribe removes the subscriber and closes its channel. Idempotent.
func (c *ChangefeedSubscription) Unsubscribe(sub *changefeedSubscriber) {
	c.mu.Lock()
	delete(c.subscribers, sub.id)
	c.mu.Unlock()
	sub.close()
}

// ResolvedHLC returns the most recent resolved timestamp seen from the
// changefeed. Empty if none observed yet.
func (c *ChangefeedSubscription) ResolvedHLC() string {
	if v, _ := c.resolvedHLC.Load().(string); v != "" {
		return v
	}
	return ""
}

// PublishProgress synthesizes an out-of-band progress event and fans it
// out to every subscriber. Called by store.RequestWatchProgress when the
// cacher needs the watchCache advanced sooner than the changefeed's
// natural resolved-timestamp cadence would deliver.
func (c *ChangefeedSubscription) PublishProgress(hlc string) {
	if hlc == "" {
		return
	}
	if cur, _ := c.resolvedHLC.Load().(string); cur < hlc {
		c.resolvedHLC.Store(hlc)
	}
	c.dispatch(changefeedEvent{isProgress: true, mvccHLC: hlc})
}

func (c *ChangefeedSubscription) run(ctx context.Context) {
	defer close(c.done)
	backoff := 500 * time.Millisecond
	for {
		if ctx.Err() != nil {
			return
		}
		if err := c.readOnce(ctx); err != nil {
			if ctx.Err() != nil {
				return
			}
			klog.V(2).Infof("cockroach changefeed: reader exited: %v; reconnecting after %v", err, backoff)
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
		backoff = 500 * time.Millisecond
	}
}

// readOnce opens a dedicated pgx.Conn, issues CREATE CHANGEFEED, and reads
// rows until disconnect. Cursor is set to the last resolved HLC so events
// aren't replayed on reconnect.
func (c *ChangefeedSubscription) readOnce(ctx context.Context) error {
	conn, err := pgx.ConnectConfig(ctx, c.connCfg)
	if err != nil {
		return fmt.Errorf("dial: %w", err)
	}
	defer func() { _ = conn.Close(context.Background()) }()

	stmt := c.buildChangefeedSQL()
	rows, err := conn.Query(ctx, stmt)
	if err != nil {
		return fmt.Errorf("changefeed query: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		if ctx.Err() != nil {
			return nil
		}
		var (
			table sql.NullString
			key   []byte
			value []byte
		)
		if err := rows.Scan(&table, &key, &value); err != nil {
			return fmt.Errorf("scan: %w", err)
		}
		ev, err := parseChangefeedRow(key, value)
		if err != nil {
			klog.V(4).Infof("cockroach changefeed: skipping unparseable row: %v", err)
			continue
		}
		if ev.isProgress {
			c.resolvedHLC.Store(ev.mvccHLC)
		}
		c.dispatch(ev)
	}
	return rows.Err()
}

func (c *ChangefeedSubscription) buildChangefeedSQL() string {
	base := `CREATE CHANGEFEED FOR TABLE kv WITH updated, mvcc_timestamp, diff, resolved='1s', min_checkpoint_frequency='1s', envelope='wrapped', format='json'`
	if cursor := c.ResolvedHLC(); cursor != "" {
		base += fmt.Sprintf(", cursor='%s', no_initial_scan", cursor)
	}
	return base
}

func (c *ChangefeedSubscription) dispatch(ev changefeedEvent) {
	c.mu.RLock()
	subs := make([]*changefeedSubscriber, 0, len(c.subscribers))
	for _, sub := range c.subscribers {
		subs = append(subs, sub)
	}
	c.mu.RUnlock()

	for _, sub := range subs {
		if !ev.isProgress && !strings.HasPrefix(ev.key, sub.keyPrefix) {
			continue
		}
		sub.send(ev)
	}
}

func (s *changefeedSubscriber) send(ev changefeedEvent) {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return
	}
	s.mu.Unlock()
	select {
	case s.ch <- ev:
	default:
		// Slow consumer: close the subscription. The Watch owner sees
		// its channel closed and re-establishes with the cacher.
		s.close()
	}
}

func (s *changefeedSubscriber) close() {
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
func (s *changefeedSubscriber) events() <-chan changefeedEvent { return s.ch }

// parseChangefeedRow decodes the pgwire row Cockroach emits. Resolved
// rows have key=NULL; regular rows have a JSON array key like
// `["/registry/foo"]` and a JSON wrapped-envelope value.
func parseChangefeedRow(rawKey, rawValue []byte) (changefeedEvent, error) {
	if rawKey == nil {
		return parseResolvedRow(rawValue)
	}
	var keys []string
	if err := json.Unmarshal(rawKey, &keys); err != nil || len(keys) == 0 {
		return changefeedEvent{}, fmt.Errorf("bad key %q", string(rawKey))
	}
	env, err := parseEnvelope(rawValue)
	if err != nil {
		return changefeedEvent{}, err
	}
	return changefeedEvent{
		key:      keys[0],
		newValue: env.after,
		oldValue: env.before,
		mvccHLC:  env.mvcc,
		isCreate: env.before == nil && env.after != nil,
		isDelete: env.after == nil,
	}, nil
}

func parseResolvedRow(rawValue []byte) (changefeedEvent, error) {
	var v struct {
		Resolved string `json:"resolved"`
	}
	if err := json.Unmarshal(rawValue, &v); err != nil {
		return changefeedEvent{}, err
	}
	if v.Resolved == "" {
		return changefeedEvent{}, errors.New("resolved row missing hlc")
	}
	return changefeedEvent{isProgress: true, mvccHLC: v.Resolved}, nil
}

// wrappedEnvelope models the envelope='wrapped' + diff JSON message shape.
// after is the new row (null on delete); before is the prior row (null on
// create); mvcc_timestamp is the write HLC.
type wrappedEnvelope struct {
	after  []byte
	before []byte
	mvcc   string
}

func parseEnvelope(rawValue []byte) (wrappedEnvelope, error) {
	var raw struct {
		After         *cfRow `json:"after"`
		Before        *cfRow `json:"before"`
		MVCCTimestamp string `json:"mvcc_timestamp"`
		Updated       string `json:"updated"`
	}
	if err := json.Unmarshal(rawValue, &raw); err != nil {
		return wrappedEnvelope{}, err
	}
	env := wrappedEnvelope{mvcc: firstNonEmpty(raw.MVCCTimestamp, raw.Updated)}
	if raw.After != nil {
		v, err := raw.After.decodeValue()
		if err != nil {
			return env, err
		}
		env.after = v
	}
	if raw.Before != nil {
		v, err := raw.Before.decodeValue()
		if err != nil {
			return env, err
		}
		env.before = v
	}
	return env, nil
}

// cfRow captures the columns we care about from a changefeed row. Cockroach
// emits BYTES as a hex-escaped string with leading `\x`.
type cfRow struct {
	Value    string `json:"value"`
	LeaseTTL *int64 `json:"lease_ttl,omitempty"`
	ExpireAt string `json:"expire_at,omitempty"`
}

func (r *cfRow) decodeValue() ([]byte, error) {
	if !strings.HasPrefix(r.Value, `\x`) {
		return nil, fmt.Errorf("value not hex-encoded: %q", r.Value)
	}
	return hex.DecodeString(r.Value[2:])
}

func firstNonEmpty(a, b string) string {
	if a != "" {
		return a
	}
	return b
}
