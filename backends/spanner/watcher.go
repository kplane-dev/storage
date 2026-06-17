package spanner

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync"

	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/apiserver/pkg/storage"
	"k8s.io/klog/v2"
)

// spannerWatcher implements watch.Interface backed by a broadcast subscription.
type spannerWatcher struct {
	store    *store
	prefix   string
	opts     storage.ListOptions
	startRev int64

	result chan watch.Event
	done   chan struct{}
	once   sync.Once

	sub *subscription
}

var _ watch.Interface = (*spannerWatcher)(nil)

func newWatcher(s *store, prefix string, opts storage.ListOptions, startRev int64) *spannerWatcher {
	w := &spannerWatcher{
		store:    s,
		prefix:   prefix,
		opts:     opts,
		startRev: startRev,
		result:   make(chan watch.Event, 100),
		done:     make(chan struct{}),
	}

	w.sub = s.broadcaster.Subscribe(256)
	go w.run()
	return w
}

func (w *spannerWatcher) ResultChan() <-chan watch.Event {
	return w.result
}

func (w *spannerWatcher) Stop() {
	w.once.Do(func() {
		w.store.broadcaster.Unsubscribe(w.sub)
		close(w.done)
	})
}

func (w *spannerWatcher) run() {
	defer close(w.result)

	klog.V(4).Infof("spannerWatcher.run: prefix=%q startRev=%d sendInitialEvents=%v", w.prefix, w.startRev, w.opts.SendInitialEvents)

	// If SendInitialEvents is requested, send a snapshot first,
	// then a bookmark, then switch to streaming.
	if w.opts.SendInitialEvents != nil && *w.opts.SendInitialEvents {
		if err := w.sendInitialEvents(); err != nil {
			klog.Errorf("failed to send initial events: %v", err)
			return
		}
	}

	klog.V(4).Infof("spannerWatcher.run: streaming from broadcaster, prefix=%q startRev=%d", w.prefix, w.startRev)

	// Stream events from the broadcaster.
	for {
		select {
		case <-w.done:
			return
		case e, ok := <-w.sub.ch:
			if !ok {
				klog.V(4).Infof("spannerWatcher.run: subscription closed, prefix=%q", w.prefix)
				return
			}
			if err := w.processEvent(e); err != nil {
				klog.V(4).Infof("watcher: failed to process event: %v", err)
			}
		}
	}
}

// sendInitialEvents lists all existing objects and sends them as ADDED events,
// followed by a bookmark at the current resource version. It advances startRev
// so that the subsequent broadcast stream doesn't re-emit events already sent.
func (w *spannerWatcher) sendInitialEvents() error {
	ctx := context.Background()

	// Capture storage keys via decode callback so we can wrap each item
	// with wrapDecodedObject (same identity annotation as processEvent).
	var keyMap map[string]string
	if w.store.wrapDecodedObject != nil {
		keyMap = make(map[string]string)
		ctx = storage.WithDecodeCallback(ctx, func(obj runtime.Object, storageKey string, modRev int64) {
			accessor, err := meta.Accessor(obj)
			if err != nil {
				return
			}
			id := strconv.FormatInt(modRev, 10) + "|" + accessor.GetNamespace() + "|" + accessor.GetName()
			keyMap[id] = storageKey
		})
	}

	listObj := w.store.newListFunc()

	listOpts := storage.ListOptions{
		Predicate: w.opts.Predicate,
		Recursive: w.opts.Recursive,
	}
	if w.startRev > 0 {
		listOpts.ResourceVersion = strconv.FormatUint(uint64(w.startRev), 10)
	}

	if err := w.store.GetList(ctx, w.prefix, listOpts, listObj); err != nil {
		klog.Errorf("spannerWatcher.sendInitialEvents: GetList failed: %v (prefix=%q)", err, w.prefix)
		return err
	}

	items, err := extractListItems(listObj)
	if err != nil {
		return err
	}

	klog.V(4).Infof("spannerWatcher.sendInitialEvents: prefix=%q items=%d", w.prefix, len(items))

	for _, item := range items {
		obj := item
		if w.store.wrapDecodedObject != nil && keyMap != nil {
			if accessor, aErr := meta.Accessor(item); aErr == nil {
				id := accessor.GetResourceVersion() + "|" + accessor.GetNamespace() + "|" + accessor.GetName()
				if storageKey, ok := keyMap[id]; ok {
					obj = w.store.wrapDecodedObject(item, storageKey)
				}
			}
		}
		select {
		case <-w.done:
			return nil
		case w.result <- watch.Event{Type: watch.Added, Object: obj}:
		}
	}

	// Get the current RV to use as the bookmark and the dedup watermark.
	bookmarkRV, rvErr := w.store.GetCurrentResourceVersion(ctx)
	if rvErr != nil {
		klog.Errorf("spannerWatcher.sendInitialEvents: GetCurrentResourceVersion failed: %v (prefix=%q)", rvErr, w.prefix)
	}
	klog.V(4).Infof("spannerWatcher.sendInitialEvents: bookmarkRV=%d allowBookmarks=%v prefix=%q", bookmarkRV, w.opts.Predicate.AllowWatchBookmarks, w.prefix)

	// Send a bookmark to signal the end of initial events.
	// The annotation is required by the WatchList contract so that the
	// reflector knows the initial snapshot is complete.
	if w.opts.Predicate.AllowWatchBookmarks && bookmarkRV > 0 {
		bookmarkObj := w.store.newFunc()
		_ = w.store.versioner.UpdateObject(bookmarkObj, bookmarkRV)
		if err := storage.AnnotateInitialEventsEndBookmark(bookmarkObj); err != nil {
			return fmt.Errorf("failed to annotate initial events end bookmark: %w", err)
		}
		select {
		case <-w.done:
			return nil
		case w.result <- watch.Event{Type: watch.Bookmark, Object: bookmarkObj}:
		}
	}

	// Advance startRev so the broadcast stream doesn't re-emit events
	// that were already covered by the initial list.
	if int64(bookmarkRV) > w.startRev {
		w.startRev = int64(bookmarkRV)
	}

	return nil
}

func (w *spannerWatcher) processEvent(e watchEvent) error {
	// Handle progress/bookmark events before key filtering —
	// progress events have no key and apply to all watchers.
	if e.isProgress {
		if w.opts.ProgressNotify {
			bookmarkObj := w.store.newFunc()
			if e.rev > 0 {
				_ = w.store.versioner.UpdateObject(bookmarkObj, uint64(e.rev))
			}
			select {
			case <-w.done:
			case w.result <- watch.Event{Type: watch.Bookmark, Object: bookmarkObj}:
			}
		}
		return nil
	}

	// Filter by key prefix for recursive watches.
	if w.opts.Recursive {
		if !strings.HasPrefix(e.key, w.prefix) {
			klog.V(5).Infof("spannerWatcher.processEvent: key prefix mismatch key=%q prefix=%q", e.key, w.prefix)
			return nil
		}
	} else {
		// Non-recursive: exact key match (strip trailing /).
		trimmed := strings.TrimSuffix(w.prefix, "/")
		if e.key != trimmed {
			return nil
		}
	}

	// Filter by revision.
	if w.startRev > 0 && e.rev <= w.startRev {
		klog.V(5).Infof("spannerWatcher.processEvent: rev filtered key=%q rev=%d startRev=%d", e.key, e.rev, w.startRev)
		return nil
	}

	klog.V(4).Infof("spannerWatcher.processEvent: DELIVERING key=%q rev=%d isCreated=%v isDeleted=%v prefix=%q", e.key, e.rev, e.isCreated, e.isDeleted, w.prefix)

	// Decode current and previous objects as needed.
	ctx := context.Background()
	var curObj, oldObj runtime.Object

	if !e.isDeleted && e.value != nil {
		data, _, err := w.store.transformer.TransformFromStorage(ctx, e.value, authenticatedDataString(e.key))
		if err != nil {
			return err
		}
		curObj = w.store.newFunc()
		if err := decode(w.store.codec, w.store.versioner, data, curObj, uint64(e.rev)); err != nil {
			return err
		}
	}

	// Decode prevValue for deletes, and for modifications when a predicate
	// filter is present (needed to detect predicate window transitions).
	if len(e.prevValue) > 0 && (e.isDeleted || !w.opts.Predicate.Empty()) {
		data, _, err := w.store.transformer.TransformFromStorage(ctx, e.prevValue, authenticatedDataString(e.key))
		if err != nil {
			return err
		}
		oldObj = w.store.newFunc()
		if err := decode(w.store.codec, w.store.versioner, data, oldObj, uint64(e.rev)); err != nil {
			return err
		}
	}

	// Determine event type, applying predicate filter transitions.
	// Matches etcd3 behavior: when a predicate is present, modifications
	// that cause an object to enter/leave the predicate window emit
	// synthetic Added/Deleted events.
	var eventType watch.EventType
	var obj runtime.Object

	switch {
	case e.isCreated:
		eventType = watch.Added
		obj = curObj
	case e.isDeleted:
		eventType = watch.Deleted
		obj = oldObj
	default:
		// Modification — check predicate transitions.
		if w.opts.Predicate.Empty() {
			eventType = watch.Modified
			obj = curObj
		} else {
			curPasses := curObj != nil && w.matchesPredicate(curObj)
			oldPasses := oldObj != nil && w.matchesPredicate(oldObj)
			switch {
			case curPasses && oldPasses:
				eventType = watch.Modified
				obj = curObj
			case curPasses && !oldPasses:
				// Object entered predicate window.
				eventType = watch.Added
				obj = curObj
			case !curPasses && oldPasses:
				// Object left predicate window.
				eventType = watch.Deleted
				obj = oldObj
			default:
				// Neither matches — skip entirely.
				return nil
			}
		}
	}

	if obj == nil {
		return nil
	}

	// Apply wrapDecodedObject hook if configured.
	// Strip the storage pathPrefix so the key matches the cacher's
	// namespace (same as etcd3's storageKeyFromPreparedKey).
	if w.store.wrapDecodedObject != nil {
		storageKey := w.store.storageKeyFromSpannerKey(e.key)
		obj = w.store.wrapDecodedObject(obj, storageKey)
	}

	// Apply predicate filter for create/delete events.
	if !w.opts.Predicate.Empty() && (e.isCreated || e.isDeleted) {
		if !w.matchesPredicate(obj) {
			return nil
		}
	}

	select {
	case <-w.done:
	case w.result <- watch.Event{Type: eventType, Object: obj}:
	}

	return nil
}

// matchesPredicate returns true if the object matches the watcher's predicate.
func (w *spannerWatcher) matchesPredicate(obj runtime.Object) bool {
	matched, err := w.opts.Predicate.Matches(obj)
	return err == nil && matched
}

// extractListItems extracts individual items from a list object.
func extractListItems(listObj runtime.Object) ([]runtime.Object, error) {
	items, err := meta.ExtractList(listObj)
	if err != nil {
		return nil, err
	}
	result := make([]runtime.Object, 0, len(items))
	for _, item := range items {
		result = append(result, item)
	}
	return result, nil
}
