package cockroach

import (
	"context"
	"errors"
	"fmt"
	"path"
	"reflect"
	"strings"
	"time"

	crdbpgx "github.com/cockroachdb/cockroach-go/v2/crdb/crdbpgxv5"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apiserver/pkg/storage"
	"k8s.io/apiserver/pkg/storage/value"
)

// authenticatedDataString satisfies value.Context so the value transformer
// can authenticate the encrypted payload against the storage key.
type authenticatedDataString string

func (d authenticatedDataString) AuthenticatedData() []byte { return []byte(string(d)) }

var _ value.Context = authenticatedDataString("")

// store implements storage.Interface against a CockroachDB kv table.
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

	// wrapDecodedObject wraps a decoded object with its storage key so
	// the cacher's multicluster keyFunc can extract cluster identity.
	// nil for single-cluster backends.
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
// Preserves the leading "/" (the multicluster cacher's ListPrefix
// depends on it).
func (s *store) storageKeyFromDBKey(dbKey string) string {
	stripped := strings.TrimPrefix(dbKey, s.pathPrefix)
	if !strings.HasPrefix(stripped, "/") {
		stripped = "/" + stripped
	}
	return stripped
}

// Create inserts a new row. Fails with storage.NewKeyExistsError if the
// key is already present (23505 unique_violation).
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

	var (
		leaseTTL  any
		expireAt  any
		commitHLC string
	)
	if ttl != 0 {
		leaseTTL = int64(ttl)
		expireAt = time.Now().Add(time.Duration(ttl) * time.Second)
	}
	err = crdbpgx.ExecuteTx(ctx, s.pool, pgx.TxOptions{}, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `
			INSERT INTO kv (key, value, lease_ttl, expire_at)
			VALUES ($1, $2, $3, $4)`,
			preparedKey, stored, leaseTTL, expireAt,
		); err != nil {
			return err
		}
		// crdb_internal_mvcc_timestamp is only visible on SELECT — not in
		// RETURNING. Inside the same serializable transaction the row we
		// just wrote is visible; the read returns the write timestamp.
		return tx.QueryRow(ctx,
			`SELECT (crdb_internal_mvcc_timestamp)::STRING FROM kv WHERE key = $1`,
			preparedKey,
		).Scan(&commitHLC)
	})
	if err != nil {
		if isUniqueViolation(err) {
			return storage.NewKeyExistsError(preparedKey, 0)
		}
		return err
	}

	rv, err := hlcToRV(commitHLC)
	if err != nil {
		return storage.NewInternalError(err)
	}
	if out != nil {
		if err := decode(s.codec, s.versioner, stored, out, rv, s.transformer, preparedKey, ctx); err != nil {
			return err
		}
	}
	return nil
}

// Get retrieves the object at key. Honors opts.ResourceVersion as an
// AS OF SYSTEM TIME snapshot; RV=0 (or empty) is a strong read.
func (s *store) Get(ctx context.Context, key string, opts storage.GetOptions, out runtime.Object) error {
	preparedKey, err := s.prepareKey(key)
	if err != nil {
		return err
	}
	aost, err := s.aostClause(ctx, opts.ResourceVersion)
	if err != nil {
		return err
	}
	q := `SELECT value, (crdb_internal_mvcc_timestamp)::STRING
	      FROM kv ` + aost + `
	      WHERE key = $1 AND (expire_at IS NULL OR expire_at > now())`

	var stored []byte
	var mvcc string
	err = s.pool.QueryRow(ctx, q, preparedKey).Scan(&stored, &mvcc)
	if errors.Is(err, pgx.ErrNoRows) {
		if opts.IgnoreNotFound {
			return runtime.SetZeroValue(out)
		}
		return storage.NewKeyNotFoundError(preparedKey, 0)
	}
	if err != nil {
		return err
	}
	rv, err := hlcToRV(mvcc)
	if err != nil {
		return storage.NewInternalError(err)
	}
	return decode(s.codec, s.versioner, stored, out, rv, s.transformer, preparedKey, ctx)
}

// Delete removes the row at key. Honors preconditions; runs
// validateDeletion against the stored object before deleting.
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
		mvccStr     string
	)
	err = crdbpgx.ExecuteTx(ctx, s.pool, pgx.TxOptions{}, func(tx pgx.Tx) error {
		var val []byte
		if err := tx.QueryRow(ctx,
			`SELECT value FROM kv WHERE key = $1 AND (expire_at IS NULL OR expire_at > now())`,
			preparedKey,
		).Scan(&val); err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return storage.NewKeyNotFoundError(preparedKey, 0)
			}
			return err
		}
		existing := s.newObject(out)
		if err := decode(s.codec, s.versioner, val, existing, 0, s.transformer, preparedKey, ctx); err != nil {
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
		// Capture the transaction's commit timestamp via
		// cluster_logical_timestamp() BEFORE the DELETE — the row-based
		// mvcc column isn't accessible after removal. Under serializable
		// isolation the returned HLC equals the tx's write timestamp.
		if err := tx.QueryRow(ctx, `SELECT (cluster_logical_timestamp())::STRING`).Scan(&mvccStr); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `DELETE FROM kv WHERE key = $1`, preparedKey); err != nil {
			return err
		}
		storedBytes = val
		return nil
	})
	if err != nil {
		return err
	}

	rv, err := hlcToRV(mvccStr)
	if err != nil {
		return storage.NewInternalError(err)
	}
	return decode(s.codec, s.versioner, storedBytes, out, rv, s.transformer, preparedKey, ctx)
}

// Watch, GetList, GuaranteedUpdate, GetCurrentResourceVersion, and
// RequestWatchProgress are implemented in dedicated files.

// isUniqueViolation matches pgx-wrapped Cockroach unique_violation
// (SQLSTATE 23505) which INSERT surfaces on primary-key collision.
func isUniqueViolation(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "23505"
}
