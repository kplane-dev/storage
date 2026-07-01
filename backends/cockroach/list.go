package cockroach

import (
	"context"
	"fmt"
	"reflect"
	"strings"

	"github.com/jackc/pgx/v5"

	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/conversion"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apiserver/pkg/storage"
)

// GetList returns objects whose key starts with the given prefix.
// Honors opts.Recursive (true = prefix scan, false = exact match) and
// opts.ResourceVersion for AS OF SYSTEM TIME snapshotting.
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
		parsed, perr := s.versioner.ParseResourceVersion(opts.ResourceVersion)
		if perr == nil && parsed > 0 {
			if cur, curErr := s.GetCurrentResourceVersion(ctx); curErr == nil && parsed > cur {
				return storage.NewTooLargeResourceVersionError(parsed, cur, 1)
			}
		}
	}

	aost, err := s.aostSnapshotClause(withRev, opts.ResourceVersion)
	if err != nil {
		return err
	}

	var rows pgx.Rows
	if opts.Recursive {
		startKey := preparedKey
		if continueKey != "" {
			startKey = continueKey
		}
		endKey := prefixEnd(preparedKey)
		q := `SELECT key, value, (crdb_internal_mvcc_timestamp)::STRING
		      FROM kv ` + aost + `
		      WHERE key >= $1 AND key < $2
		        AND (expire_at IS NULL OR expire_at > now())
		      ORDER BY key`
		limitClause := ""
		if opts.Predicate.Limit > 0 {
			limitClause = fmt.Sprintf(" LIMIT %d", opts.Predicate.Limit+1)
		}
		q += limitClause
		rows, err = s.pool.Query(ctx, q, startKey, endKey)
	} else {
		q := `SELECT key, value, (crdb_internal_mvcc_timestamp)::STRING
		      FROM kv ` + aost + `
		      WHERE key = $1
		        AND (expire_at IS NULL OR expire_at > now())`
		rows, err = s.pool.Query(ctx, q, preparedKey)
	}
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
		var mvcc string
		if err := rows.Scan(&rowKey, &stored, &mvcc); err != nil {
			return err
		}
		if limit > 0 && emitted == limit {
			// The extra row (we asked for LIMIT limit+1) tells us there's
			// a next page — don't emit it, just record the flag.
			hasMore = true
			continue
		}
		rv, err := hlcToRV(mvcc)
		if err != nil {
			return storage.NewInternalError(err)
		}
		obj := s.newObjectOfType(v.Type().Elem())
		if err := decode(s.codec, s.versioner, stored, obj, rv, s.transformer, rowKey, ctx); err != nil {
			return err
		}
		if cb := storage.DecodeCallbackFromContext(ctx); cb != nil {
			cb(obj, s.storageKeyFromDBKey(rowKey), int64(rv))
		}
		if matched, err := opts.Predicate.Matches(obj); err == nil && matched {
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

// aostSnapshotClause returns the AS OF SYSTEM TIME clause for GetList. If
// withRev > 0 the snapshot is exact; RV=0 or unset yields no clause
// (strong read). "-5s" follower reads are only used for opts.ResourceVersion="0"
// on Get; GetList tends to serve consistent reads by default.
func (s *store) aostSnapshotClause(withRev int64, rvOpt string) (string, error) {
	if withRev > 0 {
		return fmt.Sprintf("AS OF SYSTEM TIME '%s'", rvToHLC(uint64(withRev))), nil
	}
	if rvOpt == "0" {
		return "AS OF SYSTEM TIME '-5s'", nil
	}
	return "", nil
}

// resolveListRV picks a valid RV for the list response. Caller-specified
// snapshot RV wins; otherwise the current cluster HLC. Never returns 0.
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
// starting with prefix. Used to translate a range scan into a WHERE clause.
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
