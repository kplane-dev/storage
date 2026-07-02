// Package cockroach implements storage.Interface backed by CockroachDB.
//
// The backend stores every apiserver object in a single kv table and
// exposes watches by subscribing to a table-scoped CockroachDB changefeed.
// Row-level TTL, follower reads via AS OF SYSTEM TIME, and serializable
// transactions come from the database directly; this package is a thin
// adapter that translates storage.Interface calls into SQL.
//
// Minimum supported release: CockroachDB v25.4. Primary test target: v26.2.
package cockroach
