package postgres

import (
	"context"
	"fmt"
	"reflect"
	"strings"

	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/conversion"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apiserver/pkg/storage"
)

// GetList returns objects whose key starts with the given prefix. Honors
// opts.Recursive (true = prefix scan, false = exact match) and an explicit
// opts.ResourceVersion, which bounds the append-only log to `id <= rv` so a
// paginated list reads one consistent snapshot even as newer rows land.
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
		return fmt.Errorf("need pointer to slice: %v", err)
	}

	withRev, continueKey, err := storage.ValidateListOptions(preparedKey, s.versioner, opts)
	if err != nil {
		return err
	}
	if opts.ResourceVersion != "" && opts.ResourceVersion != "0" {
		if parsed, perr := s.versioner.ParseResourceVersion(opts.ResourceVersion); perr == nil && parsed > 0 {
			if cur, cerr := s.GetCurrentResourceVersion(ctx); cerr == nil && parsed > cur {
				return storage.NewTooLargeResourceVersionError(parsed, cur, 1)
			}
		}
	}

	// The snapshot bound. A caller-specified RV pins the id ceiling (and is
	// carried across pages by the continue token); an unset/"0" RV reads the
	// live head of the log.
	upperID := unboundedID
	if withRev > 0 {
		upperID = rvToID(uint64(withRev))
	}

	build := func() (string, []any) {
		// DISTINCT ON (name) ... ORDER BY name, id DESC collapses the log to
		// the newest row per key at or below the snapshot; the outer filter
		// then drops tombstoned or expired keys. This is the append-only
		// equivalent of a point-in-time table scan.
		if opts.Recursive {
			startKey := preparedKey
			if continueKey != "" {
				startKey = continueKey
			}
			endKey := prefixEnd(preparedKey)
			q := `SELECT name, value, id FROM (
			          SELECT DISTINCT ON (name) name, value, id, deleted, expire_at
			          FROM kv
			          WHERE name >= $1 AND name < $2 AND id <= $3
			          ORDER BY name, id DESC
			      ) t
			      WHERE deleted = false AND (expire_at IS NULL OR expire_at > now())
			      ORDER BY name`
			args := []any{startKey, endKey, upperID}
			if opts.Predicate.Limit > 0 {
				q += fmt.Sprintf(" LIMIT %d", opts.Predicate.Limit+1)
			}
			return q, args
		}
		return `SELECT name, value, id FROM (
		            SELECT name, value, id, deleted, expire_at FROM kv
		            WHERE name = $1 AND id <= $2 ORDER BY id DESC LIMIT 1
		        ) t
		        WHERE deleted = false AND (expire_at IS NULL OR expire_at > now())`,
			[]any{preparedKey, upperID}
	}

	q, args := build()
	rows, err := s.pool.Query(ctx, q, args...)
	if err != nil {
		return err
	}
	defer rows.Close()

	var lastEmittedKey string
	var emitted int64
	limit := opts.Predicate.Limit
	hasMore := false
	for rows.Next() {
		var rowKey string
		var stored []byte
		var id int64
		if err := rows.Scan(&rowKey, &stored, &id); err != nil {
			return err
		}
		if limit > 0 && emitted == limit {
			// The extra row (we asked for LIMIT limit+1) only signals that a
			// next page exists — don't emit it.
			hasMore = true
			continue
		}
		obj := s.newObjectOfType(v.Type().Elem())
		if err := decode(s.codec, s.versioner, stored, obj, idToRV(id), s.transformer, rowKey, ctx); err != nil {
			return err
		}
		if cb := storage.DecodeCallbackFromContext(ctx); cb != nil {
			cb(obj, s.storageKeyFromDBKey(rowKey), id)
		}
		if matched, merr := opts.Predicate.Matches(obj); merr == nil && matched {
			v.Set(reflect.Append(v, reflect.ValueOf(obj).Elem()))
		}
		emitted++
		lastEmittedKey = rowKey
	}
	if err := rows.Err(); err != nil {
		return err
	}
	if v.IsNil() {
		v.Set(reflect.MakeSlice(v.Type(), 0, 0))
	}

	listRV, err := s.resolveListRV(ctx, withRev)
	if err != nil {
		return err
	}
	var continueValue string
	var remainingItemCount *int64
	if hasMore && lastEmittedKey != "" {
		continueValue, remainingItemCount, err = storage.PrepareContinueToken(lastEmittedKey, preparedKey, int64(listRV), emitted, hasMore, opts)
		if err != nil {
			return err
		}
	}
	return s.versioner.UpdateList(listObj, listRV, continueValue, remainingItemCount)
}

// resolveListRV picks a valid RV for the list response. A caller-specified
// snapshot RV wins; otherwise the current gap-safe head. Never returns 0.
func (s *store) resolveListRV(ctx context.Context, withRev int64) (uint64, error) {
	if withRev > 0 {
		return uint64(withRev), nil
	}
	rv, err := s.GetCurrentResourceVersion(ctx)
	if err != nil {
		return 0, fmt.Errorf("resolveListRV: %w", err)
	}
	return rv, nil
}

// prefixEnd returns the smallest key strictly greater than every key
// starting with prefix. Used to translate a prefix scan into a half-open
// range on the (name, id DESC) index.
func prefixEnd(prefix string) string {
	if prefix == "" {
		return "\xff"
	}
	b := []byte(prefix)
	for i := len(b) - 1; i >= 0; i-- {
		if b[i] < 0xff {
			b[i]++
			return string(b[:i+1])
		}
	}
	return string(b) + "\x00"
}
