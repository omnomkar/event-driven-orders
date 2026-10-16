// Package postgres runs embedded schema migrations against a service's own
// database.
package postgres

import (
	"context"
	"fmt"
	"io/fs"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/pressly/goose/v3"
	"github.com/pressly/goose/v3/lock"
)

// retryInterval is how long Migrate waits between attempts while Postgres is
// not reachable yet.
const retryInterval = time.Second

// Migrate applies every pending migration in fsys (goose SQL files) and
// returns once the schema is current. It keeps retrying while the database is
// unreachable, so a service can start before Postgres does; ctx bounds the
// wait. A Postgres advisory lock means two replicas starting together cannot
// apply the same migration twice.
func Migrate(ctx context.Context, pool *pgxpool.Pool, fsys fs.FS, logger *slog.Logger) error {
	for {
		err := migrateOnce(ctx, pool, fsys, logger)
		if err == nil {
			return nil
		}
		logger.Warn("migrations not applied yet, retrying", "error", err)
		select {
		case <-ctx.Done():
			return fmt.Errorf("migrate: %w", err)
		case <-time.After(retryInterval):
		}
	}
}

func migrateOnce(ctx context.Context, pool *pgxpool.Pool, fsys fs.FS, logger *slog.Logger) error {
	if err := pool.Ping(ctx); err != nil {
		return fmt.Errorf("ping postgres: %w", err)
	}
	locker, err := lock.NewPostgresSessionLocker()
	if err != nil {
		return fmt.Errorf("create migration lock: %w", err)
	}
	// goose works with database/sql; OpenDBFromPool adapts the pgx pool
	// without opening a second set of connections.
	db := stdlib.OpenDBFromPool(pool)
	defer func() { _ = db.Close() }()

	provider, err := goose.NewProvider(goose.DialectPostgres, db, fsys, goose.WithSessionLocker(locker))
	if err != nil {
		return fmt.Errorf("create migration provider: %w", err)
	}
	results, err := provider.Up(ctx)
	if err != nil {
		return fmt.Errorf("apply migrations: %w", err)
	}
	for _, r := range results {
		logger.Info("applied migration", "version", r.Source.Version, "file", r.Source.Path, "duration", r.Duration.String())
	}
	return nil
}
