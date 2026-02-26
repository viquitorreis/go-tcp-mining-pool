package db

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

type DB struct {
	pool *pgxpool.Pool
}

func New(ctx context.Context) (*DB, error) {
	connStr := "postgres://luxor:luxor@127.0.0.1:5432/luxor?sslmode=disable"

	poolCtx, cancel := context.WithTimeout(ctx, time.Second*15)
	defer cancel()

	pool, err := pgxpool.New(poolCtx, connStr)
	if err != nil {
		return nil, err
	}

	if err := pool.Ping(poolCtx); err != nil {
		return nil, fmt.Errorf("error connecting to database: %w", err)
	}

	db := &DB{pool: pool}

	if err := db.RunMigrations(ctx); err != nil {
		return nil, fmt.Errorf("error running migrations: %w", err)
	}

	slog.Info("Database connection stablished")

	return db, nil
}

func (db *DB) RunMigrations(ctx context.Context) error {
	query := `
		CREATE TABLE IF NOT EXISTS submissions (
			username VARCHAR(255) NOT NULL,
			timestamp TIMESTAMP NOT NULL,
			submission_count INT NOT NULL,
			UNIQUE(username, timestamp)
		);
	`

	if _, err := db.pool.Exec(ctx, query); err != nil {
		return err
	}

	return nil
}
