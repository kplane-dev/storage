package spanner

import (
	"sync"

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

// Broadcaster fans out storage mutation events to all subscribed watchers.
// The write path (Create/Delete/GuaranteedUpdate) calls Publish() after each
// successful Spanner commit. This gives ~microsecond notification latency
// for in-process consumers (the cacher), avoiding the need for Change Streams
// on the hot path.
type Broadcaster struct {
	mu          sync.RWMutex
	subscribers map[uint64]*subscription
	nextID      uint64

	// highWaterMark tracks the highest revision seen, used for
	// progress/bookmark events.
	highWaterMark int64
}

// NewBroadcaster creates a new Broadcaster.
func NewBroadcaster() *Broadcaster {
	return &Broadcaster{
		subscribers: make(map[uint64]*subscription),
	}
}

// Publish sends an event to all subscribers. Non-blocking: if a subscriber's
// channel is full, the subscription is closed so the watcher detects the gap
// and the cacher can re-list (same behavior as etcd closing a slow watch).
func (b *Broadcaster) Publish(e watchEvent) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if e.rev > b.highWaterMark {
		b.highWaterMark = e.rev
	}

	klog.V(4).Infof("broadcaster.Publish: key=%q rev=%d isCreated=%v isDeleted=%v isProgress=%v subscribers=%d",
		e.key, e.rev, e.isCreated, e.isDeleted, e.isProgress, len(b.subscribers))

	for id, sub := range b.subscribers {
		if sub.closed {
			continue
		}
		select {
		case sub.ch <- e:
		default:
			// Subscriber can't keep up — close it so the watcher
			// exits cleanly and the cacher re-establishes the watch.
			sub.closed = true
			close(sub.ch)
			delete(b.subscribers, id)
		}
	}
}

// Subscribe creates a new subscription. The returned channel receives all
// events published after the subscription is created. bufSize controls the
// channel buffer depth.
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

// HighWaterMark returns the highest revision seen by the broadcaster.
func (b *Broadcaster) HighWaterMark() int64 {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.highWaterMark
}
