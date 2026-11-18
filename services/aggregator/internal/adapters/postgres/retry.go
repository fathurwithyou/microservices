package postgres

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
)

// serializationFailure is SQLSTATE 40001. CockroachDB returns it as
// "restart transaction: TransactionRetryWithProtoRefreshError" when a
// statement must be re-run, e.g. a long scan that met a write inside its
// clock-uncertainty window right after a node rejoined (P1.4, 17 Nov).
// Single-statement reads are retried server side only until the first row
// has been streamed; a scan over hundreds of thousands of rows is past that
// point, so the client has to run it again.
const serializationFailure = "40001"

// readRetryAttempts bounds the client-side retries of one read.
const readRetryAttempts = 3

// isRetryable reports whether err asks for the statement to be run again.
func isRetryable(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == serializationFailure
}

// retryRead runs a read-only operation again, with a short backoff, while it
// fails with a retryable error and the caller's context is still live.
func retryRead[T any](ctx context.Context, read func(context.Context) (T, error)) (T, error) {
	var (
		result T
		err    error
	)
	for attempt := 1; ; attempt++ {
		result, err = read(ctx)
		if err == nil || attempt >= readRetryAttempts || !isRetryable(err) || ctx.Err() != nil {
			return result, err
		}
		select {
		case <-ctx.Done():
			return result, err
		case <-time.After(time.Duration(attempt) * 50 * time.Millisecond):
		}
	}
}
