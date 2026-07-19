package postgres

import (
	"context"
	"strings"
	"sync"

	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/apiserver/pkg/storage"
	"k8s.io/klog/v2"
)

// watcher implements watch.Interface backed by a feed subscriber.
type watcher struct {
	store    *store
	prefix   string
	opts     storage.ListOptions
	startRV  uint64
	sub      *feedSubscriber
	result   chan watch.Event
	done     chan struct{}
	stopOnce sync.Once
	ctx      context.Context
}

var _ watch.Interface = (*watcher)(nil)

// ResultChan returns the channel events are delivered on.
func (w *watcher) ResultChan() <-chan watch.Event { return w.result }

// Stop unsubscribes from the feed and closes the result channel.
func (w *watcher) Stop() {
	w.stopOnce.Do(func() {
		if w.store.feed != nil {
			w.store.feed.Unsubscribe(w.sub)
		}
		close(w.done)
	})
}

// Watch returns a watch.Interface streaming events for keys under key.
// Semantics match etcd3:
//   - opts.ResourceVersion == "" or "0" (SendInitialEvents nil): first
//     replay current state as ADDED events, then stream live
//   - opts.ResourceVersion > 0: stream events strictly after that RV
//   - opts.SendInitialEvents *bool: honored per the WatchList contract
//   - opts.Predicate: applied per-event (filter transitions synthesize
//     ADDED/DELETED to match etcd3)
func (s *store) Watch(ctx context.Context, key string, opts storage.ListOptions) (watch.Interface, error) {
	if s.feed == nil {
		return nil, storage.NewInternalError(errFeedNotStarted)
	}
	preparedKey, err := s.prepareKey(key)
	if err != nil {
		return nil, err
	}
	if opts.Recursive && !strings.HasSuffix(preparedKey, "/") {
		preparedKey += "/"
	}

	startRV := uint64(0)
	if opts.ResourceVersion != "" {
		if parsed, perr := s.versioner.ParseResourceVersion(opts.ResourceVersion); perr == nil {
			startRV = parsed
		}
	}

	// Subscribe before starting the goroutine so events that land during
	// sendInitialEvents are buffered, not lost; the startRV filter drops any
	// that duplicate the initial replay.
	sub := s.feed.Subscribe(preparedKey, 256)
	w := &watcher{
		store:   s,
		prefix:  preparedKey,
		opts:    opts,
		startRV: startRV,
		sub:     sub,
		result:  make(chan watch.Event, 100),
		done:    make(chan struct{}),
		ctx:     ctx,
	}
	go w.run()
	return w, nil
}

func (w *watcher) run() {
	defer close(w.result)

	if w.ctx != nil {
		if err := w.ctx.Err(); err != nil {
			return
		}
	}

	if w.wantInitial() {
		if err := w.sendInitialEvents(); err != nil {
			klog.V(4).Infof("postgres watcher: initial events failed: %v", err)
			return
		}
	}

	var ctxDone <-chan struct{}
	if w.ctx != nil {
		ctxDone = w.ctx.Done()
	}
	for {
		select {
		case <-w.done:
			return
		case <-ctxDone:
			return
		case ev, ok := <-w.sub.events():
			if !ok {
				return
			}
			if ev.isProgress {
				w.emitProgressBookmark(ev.rv)
				continue
			}
			w.dispatch(ev)
		}
	}
}

// emitProgressBookmark forwards a feed progress event as a watch.Bookmark.
// The cacher's watchCache advances its resourceVersion on any received
// event (including bookmarks); without this, waitlist requests that call
// GetCurrentResourceVersion block forever waiting for the watchCache to
// reach a version no real write will produce.
func (w *watcher) emitProgressBookmark(rv uint64) {
	if !w.opts.Predicate.AllowWatchBookmarks {
		return
	}
	if rv <= w.startRV {
		return
	}
	obj := w.store.newObject(nil)
	if err := w.store.versioner.UpdateObject(obj, rv); err != nil {
		return
	}
	_ = w.deliver(watch.Event{Type: watch.Bookmark, Object: obj})
}

// wantInitial mirrors etcd3's areInitialEventsRequired: RV=0 with
// SendInitialEvents unset means "send me the current state as ADDED".
func (w *watcher) wantInitial() bool {
	if w.opts.SendInitialEvents != nil && *w.opts.SendInitialEvents {
		return true
	}
	return w.opts.SendInitialEvents == nil && w.startRV == 0
}

// sendInitialEvents replays current state as ADDED events. Uses Get for
// single-key watches (prefix has no trailing slash) and GetList for prefix
// watches — matching the caller's opts.Recursive. Advances startRV to the
// highest emitted item's RV so the streaming loop doesn't re-deliver rows.
func (w *watcher) sendInitialEvents() error {
	if w.opts.Recursive {
		return w.sendInitialEventsList()
	}
	return w.sendInitialEventsSingle()
}

func (w *watcher) sendInitialEventsList() error {
	listObj := w.store.newListFunc()
	if err := w.store.GetList(w.ctx, w.prefix, storage.ListOptions{
		Predicate: w.opts.Predicate,
		Recursive: true,
	}, listObj); err != nil {
		return err
	}
	items, err := meta.ExtractList(listObj)
	if err != nil {
		return err
	}
	for _, item := range items {
		if err := w.deliver(watch.Event{Type: watch.Added, Object: item}); err != nil {
			return err
		}
		if acc, aerr := meta.Accessor(item); aerr == nil {
			if rv, perr := w.store.versioner.ParseResourceVersion(acc.GetResourceVersion()); perr == nil && rv > w.startRV {
				w.startRV = rv
			}
		}
	}
	return w.emitInitialEventsEndBookmark()
}

func (w *watcher) sendInitialEventsSingle() error {
	obj := w.store.newObject(nil)
	err := w.store.Get(w.ctx, w.prefix, storage.GetOptions{IgnoreNotFound: true}, obj)
	if err != nil {
		return err
	}
	acc, aerr := meta.Accessor(obj)
	if aerr == nil && acc.GetResourceVersion() != "" {
		if err := w.deliver(watch.Event{Type: watch.Added, Object: obj}); err != nil {
			return err
		}
		if rv, perr := w.store.versioner.ParseResourceVersion(acc.GetResourceVersion()); perr == nil && rv > w.startRV {
			w.startRV = rv
		}
	}
	return w.emitInitialEventsEndBookmark()
}

// emitInitialEventsEndBookmark completes the WatchList contract: when the
// caller explicitly asked for SendInitialEvents AND allows bookmarks, emit a
// Bookmark carrying the initial-events-end annotation so the upstream
// reflector knows the replay is complete. Skipped for the legacy RV=0 path.
func (w *watcher) emitInitialEventsEndBookmark() error {
	if w.opts.SendInitialEvents == nil || !*w.opts.SendInitialEvents {
		return nil
	}
	if !w.opts.Predicate.AllowWatchBookmarks {
		return nil
	}
	bookmark := w.store.newObject(nil)
	rv := w.startRV
	if rv == 0 {
		cur, err := w.store.GetCurrentResourceVersion(w.ctx)
		if err != nil {
			return err
		}
		rv = cur
	}
	if err := w.store.versioner.UpdateObject(bookmark, rv); err != nil {
		return err
	}
	if err := storage.AnnotateInitialEventsEndBookmark(bookmark); err != nil {
		return err
	}
	return w.deliver(watch.Event{Type: watch.Bookmark, Object: bookmark})
}

// dispatch translates a feed event into a watch.Event and applies the
// caller's predicate + RV filter.
func (w *watcher) dispatch(ev feedEvent) {
	rv := ev.rv
	if w.startRV > 0 && rv <= w.startRV {
		return
	}
	preparedKey := ev.key

	var curObj runtime.Object
	if ev.newValue != nil {
		obj := w.store.newObject(nil)
		if err := decode(w.store.codec, w.store.versioner, ev.newValue, obj, rv, w.store.transformer, preparedKey, w.ctx); err != nil {
			klog.V(4).Infof("postgres watcher: decode new failed: %v", err)
			return
		}
		curObj = obj
	}
	var oldObj runtime.Object
	if ev.oldValue != nil {
		obj := w.store.newObject(nil)
		if err := decode(w.store.codec, w.store.versioner, ev.oldValue, obj, rv, w.store.transformer, preparedKey, w.ctx); err != nil {
			klog.V(4).Infof("postgres watcher: decode old failed: %v", err)
			return
		}
		oldObj = obj
	}
	eventType, out := w.classify(ev, curObj, oldObj)
	if out == nil {
		return
	}
	_ = w.deliver(watch.Event{Type: eventType, Object: out})
}

// classify picks the watch.EventType and output object, applying predicate
// transitions (etcd3 semantics: a Modify leaving the predicate window
// becomes a Deleted; entering it becomes an Added).
func (w *watcher) classify(ev feedEvent, cur, old runtime.Object) (watch.EventType, runtime.Object) {
	if ev.isDelete {
		if w.matches(old) {
			return watch.Deleted, old
		}
		return "", nil
	}
	if ev.isCreate {
		if w.matches(cur) {
			return watch.Added, cur
		}
		return "", nil
	}
	curPasses := w.matches(cur)
	oldPasses := w.matches(old)
	switch {
	case curPasses && oldPasses:
		return watch.Modified, cur
	case curPasses && !oldPasses:
		return watch.Added, cur
	case !curPasses && oldPasses:
		return watch.Deleted, old
	default:
		return "", nil
	}
}

func (w *watcher) matches(obj runtime.Object) bool {
	if obj == nil {
		return false
	}
	if w.opts.Predicate.Empty() {
		return true
	}
	matched, err := w.opts.Predicate.Matches(obj)
	return err == nil && matched
}

func (w *watcher) deliver(ev watch.Event) error {
	select {
	case <-w.done:
		return nil
	case w.result <- ev:
		return nil
	}
}
