// Package postgres implements storage.Interface backed by a stock
// PostgreSQL server (no extensions, no elevated replication privileges).
//
// Unlike the Cockroach and Spanner backends — which keep a single row per
// key and lean on the engine's own MVCC for resource versions and
// point-in-time reads — PostgreSQL exposes neither a client-visible commit
// clock nor time-travel reads. This backend therefore stores an
// append-only MVCC log: every mutation INSERTs a new row, and the row's
// BIGSERIAL id IS the Kubernetes resource version. That single choice
// covers three storage.Interface obligations at once:
//
//   - Monotonic resource versions come from the id sequence.
//   - Point-in-time reads (Get/GetList at an explicit ResourceVersion) are
//     reconstructed from the retained history via DISTINCT ON (name) with
//     an `id <= rv` bound — no engine time-travel required.
//   - Watch is a resumable tail of the log (`WHERE id > cursor`), woken
//     promptly by LISTEN/NOTIFY and made gap-safe by a transaction-id
//     watermark (see feed.go) so concurrently-committed rows are never
//     skipped.
//
// Writes run in SERIALIZABLE transactions with an automatic retry on
// serialization failure (see retry.go), the stock-Postgres equivalent of
// Cockroach's crdbpgx.ExecuteTx. TTL is enforced by a per-process scanner
// that appends tombstone rows (Postgres has no built-in row-level TTL).
//
// Minimum supported release: PostgreSQL 12 (for txid_snapshot_xmin and
// aggregate FILTER). Primary test target: PostgreSQL 16.
package postgres
