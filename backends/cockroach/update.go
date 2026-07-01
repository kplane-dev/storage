package cockroach

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"time"

	crdbpgx "github.com/cockroachdb/cockroach-go/v2/crdb/crdbpgxv5"
	"github.com/jackc/pgx/v5"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apiserver/pkg/storage"
)

// GuaranteedUpdate reads the current state, invokes tryUpdate to produce a
// new state, and writes it. Retries when tryUpdate returns a Conflict.
// Serialization failures inside the txn are auto-retried by crdbpgx.
func (s *store) GuaranteedUpdate(
	ctx context.Context,
	key string,
	destination runtime.Object,
	ignoreNotFound bool,
	preconditions *storage.Preconditions,
	tryUpdate storage.UpdateFunc,
	cachedExistingObject runtime.Object,
) error {
	preparedKey, err := s.prepareKey(key)
	if err != nil {
		return err
	}

	for {
		var (
			finalHLC  string
			finalData []byte
			noopHLC   string
			noopData  []byte
		)
		txErr := crdbpgx.ExecuteTx(ctx, s.pool, pgx.TxOptions{}, func(tx pgx.Tx) error {
			var (
				origStored []byte
				origMVCC   string
				origRV     uint64
			)
			err := tx.QueryRow(ctx, `
				SELECT value, (crdb_internal_mvcc_timestamp)::STRING
				FROM kv
				WHERE key = $1 AND (expire_at IS NULL OR expire_at > now())`,
				preparedKey,
			).Scan(&origStored, &origMVCC)
			exists := true
			if errors.Is(err, pgx.ErrNoRows) {
				if !ignoreNotFound {
					return storage.NewKeyNotFoundError(preparedKey, 0)
				}
				exists = false
			} else if err != nil {
				return err
			}

			var origObj runtime.Object
			var origPlain []byte
			if exists {
				origRV, err = hlcToRV(origMVCC)
				if err != nil {
					return storage.NewInternalError(err)
				}
				if cachedExistingObject != nil {
					if crv, cerr := s.versioner.ObjectResourceVersion(cachedExistingObject); cerr == nil && crv == origRV {
						origObj = cachedExistingObject
					}
				}
				if origObj == nil {
					plain, stale, terr := s.transformer.TransformFromStorage(ctx, origStored, authenticatedDataString(preparedKey))
					if terr != nil {
						return storage.NewInternalError(terr)
					}
					origObj = s.newObject(destination)
					if derr := decodePlain(s.codec, s.versioner, plain, origObj, origRV); derr != nil {
						return derr
					}
					if !stale {
						origPlain = plain
					}
				}
			} else {
				origObj = s.newObject(destination)
			}

			if preconditions != nil {
				if err := preconditions.Check(preparedKey, origObj); err != nil {
					return err
				}
			}

			out, ttl, uerr := tryUpdate(origObj, storage.ResponseMeta{ResourceVersion: origRV})
			if uerr != nil {
				return uerr
			}
			if err := s.versioner.PrepareObjectForStorage(out); err != nil {
				return fmt.Errorf("PrepareObjectForStorage: %w", err)
			}
			plain, err := runtime.Encode(s.codec, out)
			if err != nil {
				return err
			}
			if origPlain != nil && bytes.Equal(plain, origPlain) {
				noopHLC = origMVCC
				noopData = plain
				return nil
			}
			stored, err := s.transformer.TransformToStorage(ctx, plain, authenticatedDataString(preparedKey))
			if err != nil {
				return storage.NewInternalError(err)
			}
			var (
				leaseTTL any
				expireAt any
			)
			if ttl != nil && *ttl != 0 {
				leaseTTL = int64(*ttl)
				expireAt = time.Now().Add(time.Duration(*ttl) * time.Second)
			}
			if exists {
				if _, err := tx.Exec(ctx, `
					UPDATE kv SET value = $2, lease_ttl = $3, expire_at = $4
					WHERE key = $1`,
					preparedKey, stored, leaseTTL, expireAt,
				); err != nil {
					return err
				}
			} else {
				if _, err := tx.Exec(ctx, `
					INSERT INTO kv (key, value, lease_ttl, expire_at)
					VALUES ($1, $2, $3, $4)`,
					preparedKey, stored, leaseTTL, expireAt,
				); err != nil {
					return err
				}
			}
			// crdb_internal_mvcc_timestamp on the just-written row is
			// visible inside the same serializable txn — same trick as
			// Create. Captures the write timestamp for the reply.
			if err := tx.QueryRow(ctx,
				`SELECT (crdb_internal_mvcc_timestamp)::STRING FROM kv WHERE key = $1`,
				preparedKey,
			).Scan(&finalHLC); err != nil {
				return err
			}
			finalData = plain
			return nil
		})

		if txErr != nil {
			if apierrors.IsConflict(txErr) {
				continue
			}
			return txErr
		}

		var rv uint64
		var data []byte
		if noopHLC != "" {
			rv, err = hlcToRV(noopHLC)
			data = noopData
		} else {
			rv, err = hlcToRV(finalHLC)
			data = finalData
		}
		if err != nil {
			return storage.NewInternalError(err)
		}
		return decodePlain(s.codec, s.versioner, data, destination, rv)
	}
}
