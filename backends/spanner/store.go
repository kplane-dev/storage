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

	return &store{
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
}

func (s *store) Versioner() storage.Versioner {
	return s.versioner
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
	return s.pathPrefix + key, nil
}

func (s *store) storageKeyFromSpannerKey(spannerKey string) string {
	return strings.TrimPrefix(spannerKey, s.pathPrefix)
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
	}
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

	// Publish watch event.
	s.broadcaster.Publish(watchEvent{
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
			txn = s.client.Single().WithTimestampBound(spanner.ReadTimestamp(timestampFromRevision(int64(parsed))))
		}
	}
	defer txn.Close()

	row, err := txn.ReadRow(ctx, "kv", spanner.Key{preparedKey}, []string{"value", "mod_ts"})
	if err != nil {
		if spanner.ErrCode(err) == 5 { // NOT_FOUND
			if opts.IgnoreNotFound {
				return runtime.SetZeroValue(out)
			}
			return storage.NewKeyNotFoundError(preparedKey, 0)
		}
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
			existing = s.newFunc()
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

	s.broadcaster.Publish(watchEvent{
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

	w := newWatcher(s, preparedKey, opts, rev)
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
		// Non-recursive: exact key match.
		row, err := txn.ReadRow(ctx, "kv", spanner.Key{preparedKey}, []string{"key", "value", "mod_ts"})
		if err != nil {
			if spanner.ErrCode(err) == 5 { // NOT_FOUND
				if v.IsNil() {
					v.Set(reflect.MakeSlice(v.Type(), 0, 0))
				}
				return s.versioner.UpdateList(listObj, uint64(withRev), "", nil)
			}
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
		obj := s.newFunc()
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
		// Use transaction read timestamp for consistency with recursive path.
		readRV := revisionFromCommitTimestamp(modTs)
		if ts, tsErr := txn.Timestamp(); tsErr == nil {
			readRV = revisionFromCommitTimestamp(ts)
		}
		return s.versioner.UpdateList(listObj, readRV, "", nil)
	}

	limit := opts.Predicate.Limit
	paging := limit > 0

	iter := txn.Read(ctx, "kv", spanner.KeySets(keyRange), []string{"key", "value", "mod_ts"})
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

		obj := s.newFunc()
		if err := decode(s.codec, s.versioner, data, obj, rv); err != nil {
			klog.Errorf("failed to decode object at key %s: %v", rowKey, err)
			continue
		}

		// Notify decode callback for identity resolution.
		if cb := storage.DecodeCallbackFromContext(ctx); cb != nil {
			cb(obj, s.storageKeyFromSpannerKey(rowKey), int64(rv))
		}

		if matched, matchErr := opts.Predicate.Matches(obj); matchErr == nil && matched {
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
	var listRV uint64
	if withRev > 0 {
		listRV = uint64(withRev)
	} else {
		readTs, err := txn.Timestamp()
		if err == nil {
			listRV = revisionFromCommitTimestamp(readTs)
		}
		// Fallback: if no rows were read, txn.Timestamp() may fail.
		// Use current timestamp to ensure a non-zero RV.
		if listRV == 0 {
			if ts, tsErr := s.getCurrentTimestamp(ctx); tsErr == nil {
				listRV = revisionFromCommitTimestamp(ts)
			}
		}
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
				origObj = s.newFunc()
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
					origObj = s.newFunc()
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
			return err
		}

		// Only publish watch event if data actually changed.
		if noopRev == 0 {
			s.broadcaster.Publish(watchEvent{
				key:       preparedKey,
				value:     newEncData,
				prevValue: origEncData,
				rev:       int64(rv),
				isCreated: created,
			})
		}

		return nil
	}
}

// Stats returns storage statistics.
func (s *store) Stats(ctx context.Context) (storage.Stats, error) {
	stmt := spanner.Statement{
		SQL:    "SELECT COUNT(*) as cnt FROM kv WHERE STARTS_WITH(key, @prefix)",
		Params: map[string]interface{}{"prefix": s.pathPrefix + s.resourcePrefix},
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
	s.broadcaster.Publish(watchEvent{
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
