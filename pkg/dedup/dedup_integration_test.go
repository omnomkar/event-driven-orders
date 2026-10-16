//go:build integration

package dedup_test

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/jackc/pgx/v5"

	"github.com/omnomkar/event-driven-orders/pkg/dedup"
	"github.com/omnomkar/event-driven-orders/pkg/testinfra"
)

// Many concurrent transactions claim the same message ID and each does some
// "work" (an insert) in the same transaction. Exactly one of them commits its
// work; the rest see ErrDuplicate. A later claim is also a duplicate.
func TestClaimLetsExactlyOneTransactionWin(t *testing.T) {
	pool := testinfra.Postgres(t, nil)
	ctx := context.Background()
	if _, err := pool.Exec(ctx, `
		CREATE TABLE processed_messages (message_id text PRIMARY KEY, processed_at timestamptz NOT NULL DEFAULT now());
		CREATE TABLE effects (id serial PRIMARY KEY);`); err != nil {
		t.Fatal(err)
	}

	const workers = 10
	var (
		wg              sync.WaitGroup
		mu              sync.Mutex
		won, duplicates int
	)
	for range workers {
		wg.Go(func() {
			err := pgx.BeginFunc(ctx, pool, func(tx pgx.Tx) error {
				if err := dedup.Claim(ctx, tx, "msg-1"); err != nil {
					return err
				}
				_, err := tx.Exec(ctx, `INSERT INTO effects DEFAULT VALUES`)
				return err
			})
			mu.Lock()
			defer mu.Unlock()
			switch {
			case err == nil:
				won++
			case errors.Is(err, dedup.ErrDuplicate):
				duplicates++
			default:
				t.Errorf("unexpected error: %v", err)
			}
		})
	}
	wg.Wait()
	if won != 1 || duplicates != workers-1 {
		t.Errorf("won %d, duplicates %d; want 1 and %d", won, duplicates, workers-1)
	}
	var effects int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM effects`).Scan(&effects); err != nil {
		t.Fatal(err)
	}
	if effects != 1 {
		t.Errorf("effects = %d, want 1", effects)
	}

	// A rolled-back claim does not count: the message can still be processed.
	errBoom := errors.New("boom")
	err := pgx.BeginFunc(ctx, pool, func(tx pgx.Tx) error {
		if err := dedup.Claim(ctx, tx, "msg-2"); err != nil {
			return err
		}
		return errBoom
	})
	if !errors.Is(err, errBoom) {
		t.Fatalf("first attempt: %v, want boom", err)
	}
	err = pgx.BeginFunc(ctx, pool, func(tx pgx.Tx) error { return dedup.Claim(ctx, tx, "msg-2") })
	if err != nil {
		t.Errorf("claim after a rolled-back attempt: %v, want nil", err)
	}
}
