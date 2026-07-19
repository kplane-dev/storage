package postgres

import (
	"context"
	"errors"
	"fmt"
	"math"
	"path"
	"reflect"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apiserver/pkg/storage"
	"k8s.io/apiserver/pkg/storage/value"
)

// errFeedNotStarted is returned by Watch when SetFeed wasn't called. Wiring
// is the constructor's job; a Watch arriving before that is a programmer
// error, not a runtime condition.
var errFeedNotStarted = errors.New("postgres: watch feed not started (call SetFeed)")

// unboundedID is the id ceiling for a strong (non-snapshot) read — every
// committed row has a smaller id, so `id <= unboundedID` imposes no bound.
const unboundedID = int64(math.MaxInt64)

// authenticatedDataString satisfies value.Context so the transformer can
// authenticate the encrypted payload against the storage key.
type authenticatedDataString string

func (d authenticatedDataString) AuthenticatedData() []byte { return []byte(string(d)) }

var _ value.Context = authenticatedDataString("")

// store implements storage.Interface against the append-only kv log.
type store struct {
	pool        *pgxpool.Pool
	codec       runtime.Codec
	versioner   storage.Versioner
	transformer value.Transformer

	pathPrefix     string
	resourcePrefix string
	groupResource  string
	newFunc        func() runtime.Object
	newListFunc    func() runtime.Object

	// feed backs Watch: a single log poller per process, fanned out to
	// per-Watch subscribers by key prefix. May be nil if the store was
	// constructed without SetFeed (Watch then returns InternalError).
	feed *Feed

	// wrapDecodedObject wraps a decoded object with its storage key so the
	// cacher's multicluster keyFunc can extract cluster identity. nil for
	// single-cluster backends.
	wrapDecodedObject func(obj runtime.Object, key string) runtime.Object
}

var _ storage.Interface = (*store)(nil)

// NewStore returns a storage.Interface backed by the given pool. The pool
// must be connected to a database whose schema was applied by EnsureSchema.
func NewStore(
	pool *pgxpool.Pool,
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
		pool:              pool,
		codec:             codec,
		versioner:         storage.APIObjectVersioner{},
		transformer:       transformer,
		pathPrefix:        pathPrefix,
		resourcePrefix:    resourcePrefix,
		groupResource:     resourcePrefix,
		newFunc:           newFunc,
		newListFunc:       newListFunc,
		wrapDecodedObject: wrapDecodedObject,
	}
}

// SetFeed installs the process-wide watch feed. Must be called before the
// first Watch. Passing nil leaves Watch broken.
func (s *store) SetFeed(f *Feed) { s.feed = f }

// Versioner returns the resource-version scheme this store uses.
func (s *store) Versioner() storage.Versioner { return s.versioner }

// newObject returns a fresh instance of the decoded type. Prefers the
// generator supplied to NewStore; falls back to reflecting on hint's type
// for callers (allocators, endpoint leases) that pass nil newFunc.
func (s *store) newObject(hint runtime.Object) runtime.Object {
	if s.newFunc != nil {
		return s.newFunc()
	}
	if u, ok := hint.(runtime.Unstructured); ok {
		return u.NewEmptyInstance()
	}
	return reflect.New(reflect.TypeOf(hint).Elem()).Interface().(runtime.Object)
}

// newObjectOfType is the list-side newObject: GetList walks a slice and
// needs fresh instances of the element type derived from the list object.
func (s *store) newObjectOfType(t reflect.Type) runtime.Object {
	if s.newFunc != nil {
		return s.newFunc()
	}
	return reflect.New(t).Interface().(runtime.Object)
}

// prepareKey validates the caller's key and prefixes it with s.pathPrefix
// (typically "/registry/"). Rejects path traversal, empty keys, and keys
// with .. or . segments.
func (s *store) prepareKey(key string) (string, error) {
	if key == ".." || strings.HasPrefix(key, "../") || strings.HasSuffix(key, "/..") || strings.Contains(key, "/../") {
		return "", fmt.Errorf("invalid key: %q", key)
	}
	if key == "." || strings.HasPrefix(key, "./") || strings.HasSuffix(key, "/.") || strings.Contains(key, "/./") {
		return "", fmt.Errorf("invalid key: %q", key)
	}
	if key == "" || key == "/" {
		return "", fmt.Errorf("empty key: %q", key)
	}
	if strings.HasPrefix(key, s.pathPrefix) {
		return key, nil
	}
	return s.pathPrefix + strings.TrimPrefix(key, "/"), nil
}

// storageKeyFromDBKey undoes prepareKey for callers that need the
// storage-relative key back — the cacher's keyFunc and watch consumers.
// Preserves the leading "/" (the multicluster cacher's ListPrefix depends
// on it).
func (s *store) storageKeyFromDBKey(dbKey string) string {
	stripped := strings.TrimPrefix(dbKey, s.pathPrefix)
	if !strings.HasPrefix(stripped, "/") {
		stripped = "/" + stripped
	}
	return stripped
}

// ttlColumns turns a ttl (seconds) into the lease_ttl / expire_at column
// values. A zero ttl leaves both nil (no expiry).
func ttlColumns(ttl uint64) (leaseTTL any, expireAt any) {
	if ttl == 0 {
		return nil, nil
	}
	return int64(ttl), time.Now().Add(time.Duration(ttl) * time.Second)
}

// Create appends a create row for key. Fails with KeyExistsError if a live,
// unexpired row already exists. The existence check and the insert share one
// SERIALIZABLE transaction so two racing Creates can't both observe "no live
// row" — the loser serialization-fails, retries, and then sees KeyExists.
func (s *store) Create(ctx context.Context, key string, obj, out runtime.Object, ttl uint64) error {
	preparedKey, err := s.prepareKey(key)
	if err != nil {
		return err
	}
	if version, verr := s.versioner.ObjectResourceVersion(obj); verr == nil && version != 0 {
		return storage.ErrResourceVersionSetOnCreate
	}
	if err := s.versioner.PrepareObjectForStorage(obj); err != nil {
		return fmt.Errorf("PrepareObjectForStorage: %w", err)
	}
	encoded, err := runtime.Encode(s.codec, obj)
	if err != nil {
		return err
	}
	stored, err := s.transformer.TransformToStorage(ctx, encoded, authenticatedDataString(preparedKey))
	if err != nil {
		return storage.NewInternalError(err)
	}
	leaseTTL, expireAt := ttlColumns(ttl)

	var commitID int64
	err = execTx(ctx, s.pool, func(tx pgx.Tx) error {
		var deleted, expired bool
		row := tx.QueryRow(ctx, `
			SELECT deleted, (expire_at IS NOT NULL AND expire_at <= now())
			FROM kv WHERE name = $1 ORDER BY id DESC LIMIT 1`, preparedKey)
		switch err := row.Scan(&deleted, &expired); {
		case err == nil:
			if !deleted && !expired {
				return storage.NewKeyExistsError(preparedKey, 0)
			}
		case errors.Is(err, pgx.ErrNoRows):
			// no prior row — fall through to insert
		default:
			return err
		}
		return tx.QueryRow(ctx, `
			INSERT INTO kv (name, value, prev_value, created, deleted, lease_ttl, expire_at)
			VALUES ($1, $2, NULL, true, false, $3, $4)
			RETURNING id`,
			preparedKey, stored, leaseTTL, expireAt,
		).Scan(&commitID)
	})
	if err != nil {
		return err
	}

	if out != nil {
		if err := decode(s.codec, s.versioner, stored, out, idToRV(commitID), s.transformer, preparedKey, ctx); err != nil {
			return err
		}
	}
	return nil
}

// Get retrieves the object at key. Honors opts.ResourceVersion as a
// snapshot bound (newest row with id <= rv); RV=0 or empty is a strong read.
func (s *store) Get(ctx context.Context, key string, opts storage.GetOptions, out runtime.Object) error {
	preparedKey, err := s.prepareKey(key)
	if err != nil {
		return err
	}
	upperID := unboundedID
	if opts.ResourceVersion != "" && opts.ResourceVersion != "0" {
		if parsed, perr := s.versioner.ParseResourceVersion(opts.ResourceVersion); perr == nil && parsed > 0 {
			if cur, cerr := s.GetCurrentResourceVersion(ctx); cerr == nil && parsed > cur {
				return storage.NewTooLargeResourceVersionError(parsed, cur, 1)
			}
			upperID = rvToID(parsed)
		}
	}

	// Take the newest row at or below the snapshot bound, then keep it only
	// if it's a live, unexpired object. If the newest row is a tombstone or
	// expired, the outer filter yields nothing → not found (we must not fall
	// through to an older, superseded row).
	var stored []byte
	var id int64
	err = s.pool.QueryRow(ctx, `
		SELECT value, id FROM (
			SELECT value, id, deleted, expire_at FROM kv
			WHERE name = $1 AND id <= $2
			ORDER BY id DESC LIMIT 1
		) t
		WHERE deleted = false AND (expire_at IS NULL OR expire_at > now())`,
		preparedKey, upperID,
	).Scan(&stored, &id)
	if errors.Is(err, pgx.ErrNoRows) {
		if opts.IgnoreNotFound {
			return runtime.SetZeroValue(out)
		}
		return storage.NewKeyNotFoundError(preparedKey, 0)
	}
	if err != nil {
		return err
	}
	return decode(s.codec, s.versioner, stored, out, idToRV(id), s.transformer, preparedKey, ctx)
}

// Delete appends a tombstone row for key. Honors preconditions and runs
// validateDeletion against the stored object before writing the tombstone.
func (s *store) Delete(
	ctx context.Context,
	key string,
	out runtime.Object,
	preconditions *storage.Preconditions,
	validateDeletion storage.ValidateObjectFunc,
	cachedExistingObject runtime.Object,
	opts storage.DeleteOptions,
) error {
	preparedKey, err := s.prepareKey(key)
	if err != nil {
		return err
	}

	var (
		storedBytes []byte
		commitID    int64
	)
	err = execTx(ctx, s.pool, func(tx pgx.Tx) error {
		var val []byte
		var liveID int64
		row := tx.QueryRow(ctx, `
			SELECT value, id FROM (
				SELECT value, id, deleted, expire_at FROM kv
				WHERE name = $1 ORDER BY id DESC LIMIT 1
			) t
			WHERE deleted = false AND (expire_at IS NULL OR expire_at > now())`,
			preparedKey,
		)
		if err := row.Scan(&val, &liveID); err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return storage.NewKeyNotFoundError(preparedKey, 0)
			}
			return err
		}

		existing := s.newObject(out)
		if err := decode(s.codec, s.versioner, val, existing, idToRV(liveID), s.transformer, preparedKey, ctx); err != nil {
			return err
		}
		if preconditions != nil {
			if err := preconditions.Check(preparedKey, existing); err != nil {
				return err
			}
		}
		if err := validateDeletion(ctx, existing); err != nil {
			return err
		}

		storedBytes = val
		return tx.QueryRow(ctx, `
			INSERT INTO kv (name, value, prev_value, created, deleted)
			VALUES ($1, NULL, $2, false, true)
			RETURNING id`,
			preparedKey, val,
		).Scan(&commitID)
	})
	if err != nil {
		return err
	}
	return decode(s.codec, s.versioner, storedBytes, out, idToRV(commitID), s.transformer, preparedKey, ctx)
}
