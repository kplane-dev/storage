package cockroach

import (
	"context"
	"fmt"
	"strings"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apiserver/pkg/storage"
	"k8s.io/apiserver/pkg/storage/value"
)

// hlcToRV parses a CockroachDB HLC string (e.g. "1782867115401424000.0000000000")
// into a resource-version uint64 by taking the integer part. HLCs are wall-clock
// nanoseconds with a logical suffix; the logical bit disambiguates commits
// within the same nanosecond but doesn't affect our monotonic-uint64 contract.
func hlcToRV(hlc string) (uint64, error) {
	if i := strings.IndexByte(hlc, '.'); i >= 0 {
		hlc = hlc[:i]
	}
	var rv uint64
	if _, err := fmt.Sscanf(hlc, "%d", &rv); err != nil {
		return 0, fmt.Errorf("parse hlc %q: %w", hlc, err)
	}
	if rv == 0 {
		return 0, fmt.Errorf("hlc parsed to zero: %q", hlc)
	}
	return rv, nil
}

// rvToHLC formats a resource-version uint64 as the HLC string form
// CockroachDB accepts in AS OF SYSTEM TIME and cursor= clauses.
func rvToHLC(rv uint64) string {
	return fmt.Sprintf("%d.0000000000", rv)
}

// GetCurrentResourceVersion returns the current cluster logical timestamp
// as a resource version.
func (s *store) GetCurrentResourceVersion(ctx context.Context) (uint64, error) {
	var hlc string
	if err := s.pool.QueryRow(ctx, `SELECT (cluster_logical_timestamp())::STRING`).Scan(&hlc); err != nil {
		return 0, fmt.Errorf("cluster_logical_timestamp: %w", err)
	}
	return hlcToRV(hlc)
}

// aostClause returns a SQL AS OF SYSTEM TIME fragment appropriate for the
// caller-requested resourceVersion. An empty rv yields no clause (strong
// read). rv=="0" yields "-5s" (follower-read eligible stale read).
// A specific rv yields an exact HLC snapshot.
//
// AOST does not accept placeholder parameters — the value must be
// literal-interpolated. Only inputs we control (numeric strings) reach the
// interpolation branch.
func (s *store) aostClause(ctx context.Context, rv string) (string, error) {
	if rv == "" {
		return "", nil
	}
	if rv == "0" {
		return "AS OF SYSTEM TIME '-5s'", nil
	}
	parsed, err := s.versioner.ParseResourceVersion(rv)
	if err != nil {
		return "", err
	}
	if parsed == 0 {
		return "AS OF SYSTEM TIME '-5s'", nil
	}
	return fmt.Sprintf("AS OF SYSTEM TIME '%s'", rvToHLC(parsed)), nil
}

// decode transforms the stored bytes back into obj, applies the resource
// version, and — when configured — wraps the result for cluster-identity
// carrying watch events. Mirrors etcd3's decode contract.
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
// callers that already hold the untransformed bytes.
func decodePlain(codec runtime.Codec, versioner storage.Versioner, plain []byte, obj runtime.Object, rv uint64) error {
	if _, _, err := codec.Decode(plain, nil, obj); err != nil {
		return err
	}
	if rv == 0 {
		return nil
	}
	return versioner.UpdateObject(obj, rv)
}
