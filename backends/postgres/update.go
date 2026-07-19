package postgres

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apiserver/pkg/storage"
)

// GuaranteedUpdate reads the current state, invokes tryUpdate to produce a
// new state, and appends it as a new log row. Serialization failures inside
// the transaction are auto-retried by execTx; a semantic Conflict returned
// from tryUpdate drives the outer re-read loop.
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
			finalID   int64
			finalData []byte
			noopID    int64
			noopData  []byte
		)
		txErr := execTx(ctx, s.pool, func(tx pgx.Tx) error {
			var (
				origStored   []byte
				origID       int64
				origLeaseTTL *int64
				origExpireAt *time.Time
			)
			row := tx.QueryRow(ctx, `
				SELECT value, id, lease_ttl, expire_at FROM (
					SELECT value, id, deleted, lease_ttl, expire_at FROM kv
					WHERE name = $1 ORDER BY id DESC LIMIT 1
				) t
				WHERE deleted = false AND (expire_at IS NULL OR expire_at > now())`,
				preparedKey,
			)
			err := row.Scan(&origStored, &origID, &origLeaseTTL, &origExpireAt)
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
			origRV := uint64(0)
			if exists {
				origRV = idToRV(origID)
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
				// No-op: identical bytes. Return the existing RV and write
				// nothing — matches etcd's contract (the feed emits no event).
				noopID = origID
				noopData = plain
				return nil
			}
			stored, err := s.transformer.TransformToStorage(ctx, plain, authenticatedDataString(preparedKey))
			if err != nil {
				return storage.NewInternalError(err)
			}

			// TTL columns: an explicit non-zero ttl sets a fresh expiry, an
			// explicit zero clears it, and a nil ttl carries the previous
			// row's expiry forward (an update that doesn't touch the lease
			// must not drop it).
			var leaseTTL, expireAt any
			switch {
			case ttl != nil && *ttl != 0:
				leaseTTL = int64(*ttl)
				expireAt = time.Now().Add(time.Duration(*ttl) * time.Second)
			case ttl != nil && *ttl == 0:
				leaseTTL, expireAt = nil, nil
			default: // ttl == nil: carry forward
				if origLeaseTTL != nil {
					leaseTTL = *origLeaseTTL
				}
				if origExpireAt != nil {
					expireAt = *origExpireAt
				}
			}

			var prevValue any
			if exists {
				prevValue = origStored
			}
			finalData = plain
			return tx.QueryRow(ctx, `
				INSERT INTO kv (name, value, prev_value, created, deleted, lease_ttl, expire_at)
				VALUES ($1, $2, $3, $4, false, $5, $6)
				RETURNING id`,
				preparedKey, stored, prevValue, !exists, leaseTTL, expireAt,
			).Scan(&finalID)
		})

		if txErr != nil {
			if apierrors.IsConflict(txErr) {
				// tryUpdate rejected the read state — re-read and retry.
				continue
			}
			return txErr
		}

		var rv uint64
		var data []byte
		if noopID != 0 {
			rv, data = idToRV(noopID), noopData
		} else {
			rv, data = idToRV(finalID), finalData
		}
		return decodePlain(s.codec, s.versioner, data, destination, rv)
	}
}
