package postgres

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apiserver/pkg/storage"
	"k8s.io/apiserver/pkg/storage/value"
)

// The resource version is simply the append-only log's BIGSERIAL id, so the
// id<->rv conversions are identity casts. They exist as named helpers so the
// intent reads clearly at call sites and so the (uint64) storage contract
// and the (int64) SQL column type are converted in exactly one place.

func idToRV(id int64) uint64 { return uint64(id) }
func rvToID(rv uint64) int64 { return int64(rv) }

// currentSafeRV returns the highest resource version the watch feed is
// guaranteed to eventually deliver: the largest log id such that every id at
// or below it belongs to a committed transaction. Concurrent writers reserve
// ids via the sequence at INSERT time but commit in an unrelated order, so a
// naive max(id) can name an id whose transaction hasn't committed (or will
// abort) — a watcher told to wait for it would hang forever, and a poller
// that jumped past it would skip the row when it finally lands.
//
// The watermark is derived from transaction visibility, the stock-Postgres
// equivalent of Cockroach's resolved timestamp: any row whose writing txid
// is below the current snapshot's xmin is committed and final, and no row
// with a smaller id can still be pending below the smallest still-pending
// id. So the safe boundary is (smallest possibly-pending id) - 1, or the
// max id when nothing is pending.
//
// afterID lower-bounds the scan. Everything at or below a previously
// computed safe watermark is already final, so no pending row can live
// there — passing the feed's last watermark keeps the aggregate scan on the
// small recent tail instead of the whole table.
func currentSafeRV(ctx context.Context, q querier, afterID int64) (int64, error) {
	var minPending, maxID int64
	err := q.QueryRow(ctx, `
		SELECT
			COALESCE(min(id) FILTER (WHERE created_txid >= x.xmin), 0),
			COALESCE(max(id), 0)
		FROM kv, (SELECT txid_snapshot_xmin(txid_current_snapshot()) AS xmin) x
		WHERE id > $1`, afterID).Scan(&minPending, &maxID)
	if err != nil {
		return 0, fmt.Errorf("postgres: compute safe rv: %w", err)
	}
	safe := maxID
	if minPending > 0 {
		safe = minPending - 1
	}
	if safe < afterID {
		safe = afterID
	}
	return safe, nil
}

// querier is the read surface currentSafeRV needs, satisfied by both
// *pgxpool.Pool and pgx.Tx.
type querier interface {
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

var _ querier = (*pgxpool.Pool)(nil)

// GetCurrentResourceVersion returns the current gap-safe resource version.
// When the feed is running it supplies a scan lower bound so the aggregate
// stays cheap; before the feed has polled (or for the fork-level factory
// backends that have no feed) it scans from 0.
func (s *store) GetCurrentResourceVersion(ctx context.Context) (uint64, error) {
	var after int64
	if s.feed != nil {
		after = s.feed.SafeRV()
	}
	safe, err := currentSafeRV(ctx, s.pool, after)
	if err != nil {
		return 0, err
	}
	if safe == 0 {
		// The init sentinel guarantees at least one row, so a zero here means
		// the sentinel's own transaction is still settling on a brand-new
		// database. Treat the log floor as version 1 rather than returning
		// the 0 the cacher rejects.
		return 1, nil
	}
	return idToRV(safe), nil
}

// decode transforms stored bytes back into obj, applies the resource
// version, and runs the codec. Mirrors the etcd3/Cockroach decode contract.
func decode(
	codec runtime.Codec,
	versioner storage.Versioner,
	stored []byte,
	obj runtime.Object,
	rv uint64,
	transformer value.Transformer,
	preparedKey string,
	ctx context.Context,
) error {
	plain, _, err := transformer.TransformFromStorage(ctx, stored, authenticatedDataString(preparedKey))
	if err != nil {
		return storage.NewInternalError(err)
	}
	return decodePlain(codec, versioner, plain, obj, rv)
}

// decodePlain runs the codec decode + versioner UpdateObject steps for
// callers that already hold untransformed bytes.
func decodePlain(codec runtime.Codec, versioner storage.Versioner, plain []byte, obj runtime.Object, rv uint64) error {
	if _, _, err := codec.Decode(plain, nil, obj); err != nil {
		return err
	}
	if rv == 0 {
		return nil
	}
	return versioner.UpdateObject(obj, rv)
}
