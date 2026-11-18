package postgres

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
)

func TestRetryReadRunsAgainOnSerializationFailure(t *testing.T) {
	restart := &pgconn.PgError{Code: serializationFailure, Message: "restart transaction: TransactionRetryWithProtoRefreshError: ReadWithinUncertaintyIntervalError"}
	calls := 0
	ids, err := retryRead(context.Background(), func(context.Context) ([]string, error) {
		calls++
		if calls < 3 {
			return nil, restart
		}
		return []string{"a", "b"}, nil
	})
	if err != nil || len(ids) != 2 || calls != 3 {
		t.Fatalf("ids=%v err=%v calls=%d; want the third attempt's answer", ids, err, calls)
	}
}

func TestRetryReadGivesUpAfterAttemptsAndOnOtherErrors(t *testing.T) {
	restart := &pgconn.PgError{Code: serializationFailure}
	calls := 0
	if _, err := retryRead(context.Background(), func(context.Context) (int, error) { calls++; return 0, restart }); !errors.Is(err, restart) || calls != readRetryAttempts {
		t.Fatalf("err=%v calls=%d; want the restart error after %d attempts", err, calls, readRetryAttempts)
	}
	other := &pgconn.PgError{Code: "42P01", Message: "relation does not exist"}
	calls = 0
	if _, err := retryRead(context.Background(), func(context.Context) (int, error) { calls++; return 0, other }); !errors.Is(err, other) || calls != 1 {
		t.Fatalf("err=%v calls=%d; a non-retryable error must not be retried", err, calls)
	}
}

func TestHLCTimeReadsTheWallClockPart(t *testing.T) {
	got, err := hlcTime("1794959412123456789.0000000002")
	if err != nil {
		t.Fatal(err)
	}
	want := time.Date(2026, 11, 17, 23, 50, 12, 123456789, time.UTC)
	if !got.Equal(want) {
		t.Fatalf("hlcTime = %s, want %s", got, want)
	}
	if _, err := hlcTime("not-a-timestamp"); err == nil {
		t.Fatal("expected an error for a malformed timestamp")
	}
}
