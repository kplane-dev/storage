package spanner

import (
	"bytes"
	"context"
	"fmt"
	"path"
	"reflect"
	"strings"
	"time"

	"cloud.google.com/go/spanner"
	"google.golang.org/api/iterator"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/conversion"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/apiserver/pkg/storage"
	"k8s.io/apiserver/pkg/storage/value"
	"k8s.io/klog/v2"
)

// authenticatedDataString satisfies the value.Context interface.
// Uses the storage key to authenticate the stored data, same pattern as etcd3.
type authenticatedDataString string

func (d authenticatedDataString) AuthenticatedData() []byte {
	return []byte(string(d))
}

var _ value.Context = authenticatedDataString("")

// store implements storage.Interface backed by Cloud Spanner.
type store struct {
	client      *spanner.Client
	codec       runtime.Codec
	versioner   storage.Versioner
	transformer value.Transformer

	pathPrefix     string
	resourcePrefix string
	groupResource  string
	newFunc        func() runtime.Object
	newListFunc    func() runtime.Object

	broadcaster *Broadcaster

	// wrapDecodedObject wraps decoded objects with their storage key.
	// Used by multicluster caching to carry key identity through the
	// watch.Event boundary. nil for single-cluster deployments.
	wrapDecodedObject func(obj runtime.Object, key string) runtime.Object
}

var _ storage.Interface = (*store)(nil)

// NewStore creates a new Spanner-backed storage.Interface.
func NewStore(
	client *spanner.Client,
	codec runtime.Codec,
	newFunc, newListFunc func() runtime.Object,
	prefix, resourcePrefix string,
	transformer value.Transformer,
	wrapDecodedObject func(obj runtime.Object, key string) runtime.Object,
) *store {
	pathPrefix := path.Join("/", prefix)
	if !strings.HasSuffix(pathPrefix, "/") {
		pathPrefix += "/"
	}

	s := &store{
		client:            client,
		codec:             codec,
		versioner:         storage.APIObjectVersioner{},
		transformer:       transformer,
		pathPrefix:        pathPrefix,
		resourcePrefix:    resourcePrefix,
		groupResource:     resourcePrefix,
		newFunc:           newFunc,
		newListFunc:       newListFunc,
		broadcaster:       NewBroadcaster(),
		wrapDecodedObject: wrapDecodedObject,
	}
	// Start the TTL eviction scanner. 1s cadence matches etcd's
	// lease-expiration detection latency; the scanner only does work
	// when rows actually expire in the window so steady-state cost is
	// one tiny indexed SELECT/second.
	s.broadcaster.StartTTLScanner(s, time.Second)
	return s
}

func (s *store) Versioner() storage.Versioner {
	return s.versioner
}

// newObject returns a fresh instance of the type the store decodes into.
// Prefers s.newFunc (set by callers that supply a generator: CR storage via
// RESTOptionsGetter), and falls back to reflecting on hint's type for
// callers that pass nil — service IP / NodePort allocators and master/peer
// endpoint leases call factory.Create with nil newFunc and rely on the
// destination argument as the type prototype, the same way upstream's
// etcd3 store does.
func (s *store) newObject(hint runtime.Object) runtime.Object {
	if s.newFunc != nil {
		return s.newFunc()
	}
	if u, ok := hint.(runtime.Unstructured); ok {
		return u.NewEmptyInstance()
	}
	return reflect.New(reflect.TypeOf(hint).Elem()).Interface().(runtime.Object)
}

// newObjectOfType is the List-side analog: GetList walks a slice and needs
// fresh instances of the element type, not the list type. The element type
// is derivable from the listObj via reflect.
func (s *store) newObjectOfType(t reflect.Type) runtime.Object {
	if s.newFunc != nil {
		return s.newFunc()
	}
	return reflect.New(t).Interface().(runtime.Object)
}

// prepareKey validates and normalizes the storage key.
// Rejects path traversal attacks (.. and .), empty keys, and keys that
// don't start with the expected path prefix.
func (s *store) prepareKey(key string) (string, error) {
	if key == ".." ||
		strings.HasPrefix(key, "../") ||
		strings.HasSuffix(key, "/..") ||
		strings.Contains(key, "/../") {
		return "", fmt.Errorf("invalid key: %q", key)
	}
	if key == "." ||
		strings.HasPrefix(key, "./") ||
		strings.HasSuffix(key, "/.") ||
		strings.Contains(key, "/./") {
		return "", fmt.Errorf("invalid key: %q", key)
	}
	if key == "" || key == "/" {
		return "", fmt.Errorf("empty key: %q", key)
	}
	if strings.HasPrefix(key, s.pathPrefix) {
		return key, nil
	}
	// Trim a leading "/" off key before joining: pathPrefix always has a
	// trailing "/", and incoming keys from the multicluster decorator's
	// rewriteKey already start with "/" (upstream resourcePrefix carries
	// its own leading slash). Without the trim we'd produce
	// "/registry//apiregistration.k8s.io/..." — functionally harmless
	// because reads and writes both go through prepareKey, but ugly and
	// fragile for any future prefix range query.
	return s.pathPrefix + strings.TrimPrefix(key, "/"), nil
}

// storageKeyFromSpannerKey strips the apiserver's etcd-prefix (`s.pathPrefix`,
// typically `/registry/`) from a stored key, restoring the storage-relative
// path that callers (cacher's keyFunc, watch event consumers, decode
// callback) expect. The leading slash MUST be preserved: etcd stores keys
// as e.g. `/registry/apiregistration.k8s.io/...` and exposes them to the
// cacher as `/apiregistration.k8s.io/...` — the cacher's watchCache then
// looks them up by prefix `/apiregistration.k8s.io/clusters/`. Without the
// leading slash here, the cluster-aware cacher's `ListPrefix` misses every
// item it stored, and LIST returns empty even though watch events were
// successfully delivered and indexed.
func (s *store) storageKeyFromSpannerKey(spannerKey string) string {
	stripped := strings.TrimPrefix(spannerKey, s.pathPrefix)
	if !strings.HasPrefix(stripped, "/") {
		stripped = "/" + stripped
	}
	return stripped
}

// Create adds a new object at the given key.
func (s *store) Create(ctx context.Context, key string, obj, out runtime.Object, ttl uint64) error {
	preparedKey, err := s.prepareKey(key)
	if err != nil {
		return err
	}

	if version, err := s.versioner.ObjectResourceVersion(obj); err == nil && version != 0 {
		return storage.ErrResourceVersionSetOnCreate
	}
	if err := s.versioner.PrepareObjectForStorage(obj); err != nil {
		return fmt.Errorf("PrepareObjectForStorage failed: %v", err)
	}

	data, err := runtime.Encode(s.codec, obj)
	if err != nil {
		return err
	}

	newData, err := s.transformer.TransformToStorage(ctx, data, authenticatedDataString(preparedKey))
	if err != nil {
		return storage.NewInternalError(err)
	}

	cols := map[string]interface{}{
		"key":       preparedKey,
		"value":     newData,
		"mod_ts":    spanner.CommitTimestamp,
		"create_ts": spanner.CommitTimestamp,
	}
	if ttl != 0 {
		cols["lease_ttl"] = int64(ttl)
		// expire_at drives TTL eviction (read filter + scanner emit +
		// Spanner's ROW DELETION POLICY). Computed client-side because
		// spanner.CommitTimestamp is a server-side sentinel — we can't
		// arithmetic on it from the client. The few-ms skew between
		// client_now and commit_ts is well below TTL granularity.
		cols["expire_at"] = time.Now().Add(time.Duration(ttl) * time.Second)
	}
	// AcquireWrite must bracket Apply so the broadcaster's dispatcher
	// knows not to flush newer events ahead of this one. The defer Cancel
	// covers every error path; the success path calls Publish which
	// resolves the ticket (Cancel becomes a no-op).
	ticket := s.broadcaster.AcquireWrite()
	defer ticket.Cancel()

	// Use Apply with Insert (not InsertOrUpdate) so Spanner rejects
	// duplicates via PRIMARY KEY constraint — avoids a ReadRow round trip.
	commitTs, err := s.client.Apply(ctx, []*spanner.Mutation{spanner.InsertMap("kv", cols)})
	if err != nil {
		if status.Code(err) == codes.AlreadyExists {
			return storage.NewKeyExistsError(preparedKey, 0)
		}
		return err
	}

	if out != nil {
		if err := decode(s.codec, s.versioner, data, out, revisionFromCommitTimestamp(commitTs)); err != nil {
			return err
		}
	}

	ticket.Publish(watchEvent{
		key:       preparedKey,
		value:     newData,
		rev:       int64(revisionFromCommitTimestamp(commitTs)),
		isCreated: true,
	})

	return nil
}

// Get retrieves the object at the given key.
func (s *store) Get(ctx context.Context, key string, opts storage.GetOptions, out runtime.Object) error {
	preparedKey, err := s.prepareKey(key)
	if err != nil {
		return err
	}

	// Honor ResourceVersion: stale read at exact timestamp, strong read otherwise.
	txn := s.client.Single()
	if opts.ResourceVersion != "" {
		if parsed, err := s.versioner.ParseResourceVersion(opts.ResourceVersion); err == nil && parsed > 0 {
			// Guard against future-dated RVs: Spanner refuses reads bounded
			// at a timestamp more than ~1h in the future with
			// DeadlineExceeded. The apiserver contract is to return a
			// typed TooLargeResourceVersionError instead so the caller can
			// retry with backoff.
			if cur, curErr := s.GetCurrentResourceVersion(ctx); curErr == nil && parsed > cur {
				return storage.NewTooLargeResourceVersionError(parsed, cur, 1)
			}
			txn = s.client.Single().WithTimestampBound(spanner.ReadTimestamp(timestampFromRevision(int64(parsed))))
		}
	}
	defer txn.Close()

	// SQL (not ReadRow) so we can apply the TTL read filter atomically.
	// Rows past expire_at are invisible to Get even if Spanner's row
	// deletion policy hasn't yet physically removed them.
	iter := txn.Query(ctx, spanner.Statement{
		SQL: `SELECT value, mod_ts FROM kv WHERE key = @key
              AND (expire_at IS NULL OR expire_at > CURRENT_TIMESTAMP())`,
		Params: map[string]interface{}{"key": preparedKey},
	})
	defer iter.Stop()
	row, err := iter.Next()
	if err == iterator.Done {
		if opts.IgnoreNotFound {
			return runtime.SetZeroValue(out)
		}
		return storage.NewKeyNotFoundError(preparedKey, 0)
	}
	if err != nil {
		return err
	}

	var val []byte
	var modTs time.Time
	if err := row.Columns(&val, &modTs); err != nil {
		return err
	}

	data, _, err := s.transformer.TransformFromStorage(ctx, val, authenticatedDataString(preparedKey))
	if err != nil {
		return storage.NewInternalError(err)
	}

	return decode(s.codec, s.versioner, data, out, revisionFromCommitTimestamp(modTs))
}

// Delete removes the object at the given key.
func (s *store) Delete(
	ctx context.Context, key string, out runtime.Object, preconditions *storage.Preconditions,
	validateDeletion storage.ValidateObjectFunc, cachedExistingObject runtime.Object, opts storage.DeleteOptions) error {
	preparedKey, err := s.prepareKey(key)
	if err != nil {
		return err
	}

	v, err := conversion.EnforcePtr(out)
	if err != nil {
		return fmt.Errorf("unable to convert output object to pointer: %v", err)
	}
	_ = v

	var oldData []byte    // decrypted, for decoding into out
	var oldEncData []byte // encrypted, for watch event prevValue

	ticket := s.broadcaster.AcquireWrite()
	defer ticket.Cancel()

	commitTs, err := s.client.ReadWriteTransaction(ctx, func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
		row, err := txn.ReadRow(ctx, "kv", spanner.Key{preparedKey}, []string{"value", "mod_ts"})
		if err != nil {
			if spanner.ErrCode(err) == 5 { // NOT_FOUND
				return storage.NewKeyNotFoundError(preparedKey, 0)
			}
			return err
		}

		var val []byte
		var modTs time.Time
		if err := row.Columns(&val, &modTs); err != nil {
			return err
		}

		// Use cachedExistingObject to skip decrypt+decode when the RV matches.
		var existing runtime.Object
		var data []byte
		if cachedExistingObject != nil {
			if cachedRV, rvErr := s.versioner.ObjectResourceVersion(cachedExistingObject); rvErr == nil && cachedRV == revisionFromCommitTimestamp(modTs) {
				existing = cachedExistingObject
			}
		}
		if existing == nil {
			data, _, err = s.transformer.TransformFromStorage(ctx, val, authenticatedDataString(preparedKey))
			if err != nil {
				return storage.NewInternalError(err)
			}
			existing = s.newObject(out)
			if err := decode(s.codec, s.versioner, data, existing, revisionFromCommitTimestamp(modTs)); err != nil {
				return err
			}
		}

		if preconditions != nil {
			if err := preconditions.Check(preparedKey, existing); err != nil {
				return err
			}
		}
		if err := validateDeletion(ctx, existing); err != nil {
			return err
		}

		// We always need the decrypted data for the out object.
		if data == nil {
			data, _, err = s.transformer.TransformFromStorage(ctx, val, authenticatedDataString(preparedKey))
			if err != nil {
				return storage.NewInternalError(err)
			}
		}
		oldData = data
		oldEncData = val
		txn.BufferWrite([]*spanner.Mutation{spanner.Delete("kv", spanner.Key{preparedKey})})
		return nil
	})
	if err != nil {
		return err
	}

	rv := revisionFromCommitTimestamp(commitTs)

	// Decode the deleted object into out.
	if err := decode(s.codec, s.versioner, oldData, out, rv); err != nil {
		return err
	}

	ticket.Publish(watchEvent{
		key:       preparedKey,
		prevValue: oldEncData,
		rev:       int64(rv),
		isDeleted: true,
	})

	return nil
}

// Watch starts watching at the given key.
func (s *store) Watch(ctx context.Context, key string, opts storage.ListOptions) (watch.Interface, error) {
	preparedKey, err := s.prepareKey(key)
	if err != nil {
		return nil, err
	}
	if opts.Recursive && !strings.HasSuffix(preparedKey, "/") {
		preparedKey += "/"
	}

	rev := int64(0)
	if opts.ResourceVersion != "" {
		parsed, err := s.versioner.ParseResourceVersion(opts.ResourceVersion)
		if err != nil {
			return nil, err
		}
		rev = int64(parsed)
	}

	w := newWatcher(s, ctx, preparedKey, opts, rev)
	return w, nil
}

// GetList retrieves a list of objects matching the key prefix.
func (s *store) GetList(ctx context.Context, key string, opts storage.ListOptions, listObj runtime.Object) error {
	preparedKey, err := s.prepareKey(key)
	if err != nil {
		return err
	}
	if opts.Recursive && !strings.HasSuffix(preparedKey, "/") {
		preparedKey += "/"
	}

	listPtr, err := meta.GetItemsPtr(listObj)
	if err != nil {
		return err
	}
	v, err := conversion.EnforcePtr(listPtr)
	if err != nil || v.Kind() != reflect.Slice {
		return fmt.Errorf("need ptr to slice: %v", err)
	}

	withRev, continueKey, err := storage.ValidateListOptions(preparedKey, s.versioner, opts)
	if err != nil {
		return err
	}

	// Guard against future-dated RVs (Spanner refuses with DeadlineExceeded;
	// the apiserver expects TooLargeResourceVersionError). We check the
	// raw opts.ResourceVersion — ValidateListOptions returns withRev==0
	// for legacy ResourceVersionMatch="" + non-recursive queries, even
	// when the caller specified a too-high RV.
	if opts.ResourceVersion != "" && opts.ResourceVersion != "0" {
		if parsedRV, perr := s.versioner.ParseResourceVersion(opts.ResourceVersion); perr == nil && parsedRV > 0 {
			if cur, curErr := s.GetCurrentResourceVersion(ctx); curErr == nil && parsedRV > cur {
				return storage.NewTooLargeResourceVersionError(parsedRV, cur, 1)
			}
		}
	}

	// Build the read transaction (strong or stale).
	var txn *spanner.ReadOnlyTransaction
	if withRev > 0 {
		txn = s.client.Single().WithTimestampBound(spanner.ReadTimestamp(timestampFromRevision(withRev)))
	} else {
		txn = s.client.Single()
	}
	defer txn.Close()

	// Build key range for prefix scan.
	var keyRange spanner.KeyRange
	if continueKey != "" {
		// Continue from the last key (exclusive).
		keyRange = spanner.KeyRange{
			Start: spanner.Key{continueKey},
			End:   spanner.Key{prefixEnd(preparedKey)},
			Kind:  spanner.ClosedOpen,
		}
	} else if opts.Recursive {
		keyRange = spanner.KeyRange{
			Start: spanner.Key{preparedKey},
			End:   spanner.Key{prefixEnd(preparedKey)},
			Kind:  spanner.ClosedOpen,
		}
	} else {
		// Non-recursive: exact key match. SQL (not ReadRow) so we can
		// apply the TTL read filter atomically.
		iter := txn.Query(ctx, spanner.Statement{
			SQL: `SELECT key, value, mod_ts FROM kv WHERE key = @key
                  AND (expire_at IS NULL OR expire_at > CURRENT_TIMESTAMP())`,
			Params: map[string]interface{}{"key": preparedKey},
		})
		defer iter.Stop()
		row, err := iter.Next()
		if err == iterator.Done {
			if v.IsNil() {
				v.Set(reflect.MakeSlice(v.Type(), 0, 0))
			}
			rv, err := s.resolveListRV(ctx, txn, withRev)
			if err != nil {
				return err
			}
			return s.versioner.UpdateList(listObj, rv, "", nil)
		}
		if err != nil {
			return err
		}
		var rowKey string
		var val []byte
		var modTs time.Time
		if err := row.Columns(&rowKey, &val, &modTs); err != nil {
			return err
		}
		data, _, err := s.transformer.TransformFromStorage(ctx, val, authenticatedDataString(rowKey))
		if err != nil {
			return storage.NewInternalError(err)
		}
		obj := s.newObjectOfType(v.Type().Elem())
		if err := decode(s.codec, s.versioner, data, obj, revisionFromCommitTimestamp(modTs)); err != nil {
			return err
		}

		// Notify decode callback for identity resolution.
		if cb := storage.DecodeCallbackFromContext(ctx); cb != nil {
			cb(obj, s.storageKeyFromSpannerKey(rowKey), int64(revisionFromCommitTimestamp(modTs)))
		}

		if matched, err := opts.Predicate.Matches(obj); err == nil && matched {
			v.Set(reflect.Append(v, reflect.ValueOf(obj).Elem()))
		}
		// Ensure the slice is non-nil even when the predicate didn't match
		// — upstream conformance compares `[]T{}` (empty slice) against
		// our return and rejects nil-underlying slices.
		if v.IsNil() {
			v.Set(reflect.MakeSlice(v.Type(), 0, 0))
		}
		rv, err := s.resolveListRV(ctx, txn, withRev)
		if err != nil {
			return err
		}
		return s.versioner.UpdateList(listObj, rv, "", nil)
	}

	limit := opts.Predicate.Limit
	paging := limit > 0

	// SQL with the TTL read filter — rows past expire_at are invisible to
	// LIST even if Spanner's row deletion policy hasn't physically removed
	// them yet. Equivalent to the Read above but with the WHERE predicate.
	iter := txn.Query(ctx, spanner.Statement{
		SQL: `SELECT key, value, mod_ts FROM kv
              WHERE key >= @start AND key < @end
                AND (expire_at IS NULL OR expire_at > CURRENT_TIMESTAMP())
              ORDER BY key`,
		Params: map[string]interface{}{
			"start": keyRange.Start[0],
			"end":   keyRange.End[0],
		},
	})
	defer iter.Stop()

	var lastKey string
	var count int64

	for {
		// Check if the request context has been cancelled or timed out.
		select {
		case <-ctx.Done():
			return storage.NewTimeoutError(preparedKey, "request did not complete within requested timeout")
		default:
		}

		row, err := iter.Next()
		if err != nil {
			if err == iterator.Done {
				break
			}
			return err
		}

		var rowKey string
		var val []byte
		var modTs time.Time
		if err := row.Columns(&rowKey, &val, &modTs); err != nil {
			return err
		}

		rv := revisionFromCommitTimestamp(modTs)

		data, _, err := s.transformer.TransformFromStorage(ctx, val, authenticatedDataString(rowKey))
		if err != nil {
			return storage.NewInternalError(err)
		}

		obj := s.newObjectOfType(v.Type().Elem())
		if err := decode(s.codec, s.versioner, data, obj, rv); err != nil {
			klog.Errorf("failed to decode object at key %s: %v", rowKey, err)
			continue
		}

		// Notify decode callback for identity resolution.
		if cb := storage.DecodeCallbackFromContext(ctx); cb != nil {
			cb(obj, s.storageKeyFromSpannerKey(rowKey), int64(rv))
		}

		if matched, err := opts.Predicate.Matches(obj); err == nil && matched {
			v.Set(reflect.Append(v, reflect.ValueOf(obj).Elem()))
		}

		count++
		lastKey = rowKey

		if paging && int64(v.Len()) >= limit {
			break
		}
	}

	if v.IsNil() {
		v.Set(reflect.MakeSlice(v.Type(), 0, 0))
	}

	// Use the transaction's read timestamp as the list RV. This ensures
	// paginated continuations read at the same snapshot, even if some items
	// have later mod_ts than the ones returned on this page.
	listRV, err := s.resolveListRV(ctx, txn, withRev)
	if err != nil {
		return err
	}

	var continueValue string
	var remainingItemCount *int64
	if paging && int64(v.Len()) >= limit && lastKey != "" {
		hasMore := true
		continueValue, remainingItemCount, err = storage.PrepareContinueToken(lastKey, preparedKey, int64(listRV), count, hasMore, opts)
		if err != nil {
			return err
		}
	}

	return s.versioner.UpdateList(listObj, listRV, continueValue, remainingItemCount)
}

// GuaranteedUpdate implements optimistic concurrency control via Spanner transactions.
func (s *store) GuaranteedUpdate(
	ctx context.Context, key string, destination runtime.Object, ignoreNotFound bool,
	preconditions *storage.Preconditions, tryUpdate storage.UpdateFunc, cachedExistingObject runtime.Object) error {
	preparedKey, err := s.prepareKey(key)
	if err != nil {
		return err
	}

	v, err := conversion.EnforcePtr(destination)
	if err != nil {
		return fmt.Errorf("unable to convert output object to pointer: %v", err)
	}
	_ = v

	transformContext := authenticatedDataString(preparedKey)

	for {
		var newData []byte     // codec-encoded (unencrypted), for decoding into destination
		var newEncData []byte  // encrypted, for watch event value
		var origEncData []byte // encrypted, for watch event prevValue
		var noopRev int64      // set when data is unchanged (no-op), holds existing mod_ts
		var created bool       // true when object didn't exist (upsert)
		var conflict bool      // set when tryUpdate returns a retriable conflict

		// Per-attempt ticket: retries acquire a fresh one. defer-style
		// cleanup wrapped inside the iteration via an immediately-invoked
		// func so each loop iteration's ticket is released on return,
		// whether it Publishes or Cancels.
		ticket := s.broadcaster.AcquireWrite()

		commitTs, err := s.client.ReadWriteTransaction(ctx, func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
			row, readErr := txn.ReadRow(ctx, "kv", spanner.Key{preparedKey}, []string{"value", "mod_ts"})

			var origObj runtime.Object
			var origRev int64
			var origData []byte // decrypted, for no-op comparison

			if readErr != nil {
				if spanner.ErrCode(readErr) != 5 { // NOT_FOUND
					return readErr
				}
				if !ignoreNotFound {
					return storage.NewKeyNotFoundError(preparedKey, 0)
				}
				origObj = s.newObject(destination)
			} else {
				var val []byte
				var modTs time.Time
				if err := row.Columns(&val, &modTs); err != nil {
					return err
				}
				origRev = int64(revisionFromCommitTimestamp(modTs))
				origEncData = val

				// Use cachedExistingObject to skip decrypt+decode when RV matches.
				// origData stays nil, which disables no-op detection for this attempt —
				// acceptable tradeoff since we already saved the decrypt+decode cost.
				if cachedExistingObject != nil {
					if cachedRV, rvErr := s.versioner.ObjectResourceVersion(cachedExistingObject); rvErr == nil && cachedRV == uint64(origRev) {
						origObj = cachedExistingObject
					}
				}
				if origObj == nil {
					data, stale, err := s.transformer.TransformFromStorage(ctx, val, transformContext)
					if err != nil {
						return storage.NewInternalError(err)
					}
					origObj = s.newObject(destination)
					if err := decode(s.codec, s.versioner, data, origObj, revisionFromCommitTimestamp(modTs)); err != nil {
						return err
					}
					// Only enable no-op detection when the stored data is fresh.
					// If stale (e.g. key rotation), force a re-write even if
					// the decoded content is identical.
					if !stale {
						origData = data
					}
				}
			}

			if preconditions != nil {
				if err := preconditions.Check(preparedKey, origObj); err != nil {
					return err
				}
			}

			ret, ttl, err := tryUpdate(origObj, storage.ResponseMeta{ResourceVersion: uint64(origRev)})
			if err != nil {
				if apierrors.IsConflict(err) {
					// Signal the outer loop to retry with a fresh read.
					conflict = true
					return err
				}
				return err
			}

			if err := s.versioner.PrepareObjectForStorage(ret); err != nil {
				return fmt.Errorf("PrepareObjectForStorage failed: %v", err)
			}

			data, err := runtime.Encode(s.codec, ret)
			if err != nil {
				return err
			}

			// No-op detection: if data is identical, skip write.
			if origData != nil && bytes.Equal(data, origData) {
				newData = data
				noopRev = origRev
				return nil
			}

			encrypted, err := s.transformer.TransformToStorage(ctx, data, transformContext)
			if err != nil {
				return storage.NewInternalError(err)
			}

			cols := map[string]interface{}{
				"key":    preparedKey,
				"value":  encrypted,
				"mod_ts": spanner.CommitTimestamp,
			}
			if origRev == 0 {
				// Object didn't exist — insert with create_ts.
				cols["create_ts"] = spanner.CommitTimestamp
			}
			if ttl != nil && *ttl != 0 {
				cols["lease_ttl"] = int64(*ttl)
				cols["expire_at"] = time.Now().Add(time.Duration(*ttl) * time.Second)
			} else if ttl != nil {
				// ttl explicitly cleared — null out expire_at so the row
				// stops being a TTL candidate.
				cols["lease_ttl"] = nil
				cols["expire_at"] = nil
			}

			var m *spanner.Mutation
			if origRev == 0 {
				m = spanner.InsertOrUpdateMap("kv", cols)
			} else {
				m = spanner.UpdateMap("kv", cols)
			}
			txn.BufferWrite([]*spanner.Mutation{m})

			newData = data
			newEncData = encrypted
			created = origRev == 0
			return nil
		})
		if err != nil {
			ticket.Cancel()
			if conflict {
				// Retry: re-read current state and call tryUpdate again.
				conflict = false
				continue
			}
			return err
		}

		// Use existing mod_ts for no-op, commit timestamp for actual writes.
		var rv uint64
		if noopRev > 0 {
			rv = uint64(noopRev)
		} else {
			rv = revisionFromCommitTimestamp(commitTs)
		}

		if err := decode(s.codec, s.versioner, newData, destination, rv); err != nil {
			ticket.Cancel()
			return err
		}

		// Only publish watch event if data actually changed. No-op writes
		// release the ticket without enqueuing an event.
		if noopRev == 0 {
			ticket.Publish(watchEvent{
				key:       preparedKey,
				value:     newEncData,
				prevValue: origEncData,
				rev:       int64(rv),
				isCreated: created,
			})
		} else {
			ticket.Cancel()
		}

		return nil
	}
}

// Stats returns storage statistics.
func (s *store) Stats(ctx context.Context) (storage.Stats, error) {
	stmt := spanner.Statement{
		SQL:    "SELECT COUNT(*) as cnt FROM kv WHERE STARTS_WITH(key, @prefix)",
		Params: map[string]interface{}{"prefix": s.pathPrefix + strings.TrimPrefix(s.resourcePrefix, "/")},
	}
	iter := s.client.Single().Query(ctx, stmt)
	defer iter.Stop()

	row, err := iter.Next()
	if err != nil {
		return storage.Stats{}, err
	}
	var count int64
	if err := row.Columns(&count); err != nil {
		return storage.Stats{}, err
	}
	return storage.Stats{ObjectCount: count}, nil
}

// ReadinessCheck verifies Spanner connectivity.
func (s *store) ReadinessCheck() error {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	iter := s.client.Single().Query(ctx, spanner.Statement{SQL: "SELECT 1"})
	defer iter.Stop()
	_, err := iter.Next()
	return err
}

// RequestWatchProgress publishes a synthetic bookmark event.
func (s *store) RequestWatchProgress(ctx context.Context) error {
	ts, err := s.getCurrentTimestamp(ctx)
	if err != nil {
		return err
	}
	s.broadcaster.PublishProgress(watchEvent{
		rev:        int64(revisionFromCommitTimestamp(ts)),
		isProgress: true,
	})
	return nil
}

// GetCurrentResourceVersion returns the latest resource version.
func (s *store) GetCurrentResourceVersion(ctx context.Context) (uint64, error) {
	ts, err := s.getCurrentTimestamp(ctx)
	if err != nil {
		return 0, err
	}
	rv := revisionFromCommitTimestamp(ts)
	if rv == 0 {
		return 0, fmt.Errorf("the current resource version must be greater than 0")
	}
	return rv, nil
}

// EnableResourceSizeEstimation is a no-op for Spanner.
func (s *store) EnableResourceSizeEstimation(storage.KeysFunc) error {
	return nil
}

// CompactRevision returns 0 — Spanner handles data lifecycle via row deletion policies.
func (s *store) CompactRevision() int64 {
	return 0
}

// getModTimestamp reads the current mod_ts for a key.
func (s *store) getModTimestamp(ctx context.Context, key string) (time.Time, error) {
	row, err := s.client.Single().ReadRow(ctx, "kv", spanner.Key{key}, []string{"mod_ts"})
	if err != nil {
		return time.Time{}, err
	}
	var ts time.Time
	if err := row.Column(0, &ts); err != nil {
		return time.Time{}, err
	}
	return ts, nil
}

// getCurrentTimestamp gets the current Spanner timestamp.
func (s *store) getCurrentTimestamp(ctx context.Context) (time.Time, error) {
	iter := s.client.Single().Query(ctx, spanner.Statement{SQL: "SELECT CURRENT_TIMESTAMP()"})
	defer iter.Stop()
	row, err := iter.Next()
	if err != nil {
		return time.Time{}, err
	}
	var ts time.Time
	if err := row.Column(0, &ts); err != nil {
		return time.Time{}, err
	}
	return ts, nil
}

// resolveListRV picks the resource version GetList should attach to a
// returned list. The invariant the upstream cacher relies on: every
// non-error List response carries a RV > 0. Returning 0 trips the
// `illegal resource version from storage: 0` check and breaks the
// watchCache.
//
// Precedence:
//  1. If the caller specified a snapshot RV (withRev > 0), echo it back
//     — the txn was bound to it, so it's the authoritative read RV.
//  2. Otherwise prefer the transaction's read timestamp. After any
//     successful Query/Read on a Spanner ReadOnly txn (including one
//     that returned zero rows), the server-picked timestamp is
//     available; we use that so the list RV matches what the txn
//     actually read at.
//  3. As a last resort — only meaningful if the caller never executed
//     a read on the txn — fall back to a fresh CURRENT_TIMESTAMP()
//     round trip.
//
// Any failure to produce a usable RV is returned as an error rather
// than silently substituting 0.
func (s *store) resolveListRV(ctx context.Context, txn *spanner.ReadOnlyTransaction, withRev int64) (uint64, error) {
	if withRev > 0 {
		return uint64(withRev), nil
	}
	if ts, err := txn.Timestamp(); err == nil {
		if rv := revisionFromCommitTimestamp(ts); rv > 0 {
			return rv, nil
		}
	}
	ts, err := s.getCurrentTimestamp(ctx)
	if err != nil {
		return 0, fmt.Errorf("resolveListRV: txn read timestamp unavailable and CURRENT_TIMESTAMP() failed: %w", err)
	}
	rv := revisionFromCommitTimestamp(ts)
	if rv == 0 {
		return 0, fmt.Errorf("resolveListRV: CURRENT_TIMESTAMP() returned zero-valued revision")
	}
	return rv, nil
}

// decode decodes data into out and sets the resource version.
func decode(codec runtime.Codec, versioner storage.Versioner, data []byte, out runtime.Object, rv uint64) error {
	if _, _, err := codec.Decode(data, nil, out); err != nil {
		return err
	}
	if rv != 0 {
		if err := versioner.UpdateObject(out, rv); err != nil {
			return err
		}
	}
	return nil
}

// prefixEnd returns the key that is just past all keys with the given prefix.
func prefixEnd(prefix string) string {
	if len(prefix) == 0 {
		return ""
	}
	end := []byte(prefix)
	for i := len(end) - 1; i >= 0; i-- {
		if end[i] < 0xff {
			end[i]++
			return string(end[:i+1])
		}
	}
	return "" // prefix is all 0xff
}
