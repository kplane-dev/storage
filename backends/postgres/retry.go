package postgres

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// maxSerializationRetries bounds the SERIALIZABLE retry loop. Serialization
// failures under contention normally clear in one or two retries; a run
// this long signals real, sustained conflict on a single key and is better
// surfaced as an error than retried forever.
const maxSerializationRetries = 10

// execTx runs fn inside a SERIALIZABLE transaction, retrying automatically
// on serialization_failure (40001) and deadlock_detected (40P01). It is the
// stock-Postgres analog of cockroach-go's crdbpgx.ExecuteTx: because the
// append-only log relies on SERIALIZABLE isolation for its create/update/
// delete decisions (two racing Creates of one key must not both observe
// "no live row"), every write path funnels through here.
//
// fn must be idempotent across retries — it may run more than once, and any
// state it mutates outside the tx (e.g. captured output vars) must be
// re-derived on each call, not accumulated. On the happy path fn runs once.
//
// A returned application error that is NOT a retryable SQLSTATE aborts the
// loop immediately and rolls back, so typed storage errors (KeyExists,
// Conflict, NotFound) propagate unchanged.
func execTx(ctx context.Context, pool txBeginner, fn func(pgx.Tx) error) error {
	backoff := 2 * time.Millisecond
	var lastErr error
	for attempt := 0; attempt < maxSerializationRetries; attempt++ {
		tx, err := pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.Serializable})
		if err != nil {
			return err
		}
		err = fn(tx)
		if err != nil {
			_ = tx.Rollback(ctx)
			if isRetryable(err) {
				lastErr = err
				if sleepErr := sleepBackoff(ctx, &backoff); sleepErr != nil {
					return sleepErr
				}
				continue
			}
			return err
		}
		if err := tx.Commit(ctx); err != nil {
			// A serialization failure can surface at COMMIT time, not just
			// mid-transaction — retry those too.
			if isRetryable(err) {
				lastErr = err
				if sleepErr := sleepBackoff(ctx, &backoff); sleepErr != nil {
					return sleepErr
				}
				continue
			}
			return err
		}
		return nil
	}
	if lastErr != nil {
		return lastErr
	}
	return errors.New("postgres: exhausted serialization retries")
}

// txBeginner is the subset of *pgxpool.Pool execTx needs, kept as an
// interface so tests can substitute a single-conn beginner.
type txBeginner interface {
	BeginTx(ctx context.Context, opts pgx.TxOptions) (pgx.Tx, error)
}

// isRetryable reports whether err is a Postgres serialization_failure
// (40001) or deadlock_detected (40P01), the two SQLSTATEs a SERIALIZABLE
// transaction is expected to hit under contention.
func isRetryable(err error) bool {
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) {
		return false
	}
	return pgErr.Code == "40001" || pgErr.Code == "40P01"
}

// sleepBackoff waits for the current backoff and doubles it, honoring ctx
// cancellation. Capped so a long retry run doesn't grow the delay without
// bound.
func sleepBackoff(ctx context.Context, backoff *time.Duration) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(*backoff):
	}
	if *backoff < 100*time.Millisecond {
		*backoff *= 2
	}
	return nil
}
