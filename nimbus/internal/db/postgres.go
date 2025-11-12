package db

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"go.uber.org/zap"
)

// DB wraps the pgx connection pool
type DB struct {
	pool   *pgxpool.Pool
	logger *zap.Logger
}

// Config holds database connection parameters
type Config struct {
	Host     string
	Port     int
	User     string
	Password string
	Database string
	SSLMode  string
}

// New creates a new database connection pool
// Connection pooling is critical for performance:
// - Reuses connections instead of creating new ones
// - Handles concurrency automatically
// - Manages connection lifecycle
func New(ctx context.Context, cfg Config, logger *zap.Logger) (*DB, error) {
	// Build connection string
	dsn := fmt.Sprintf(
		"host=%s port=%d user=%s password=%s dbname=%s sslmode=%s",
		cfg.Host, cfg.Port, cfg.User, cfg.Password, cfg.Database, cfg.SSLMode,
	)

	// Configure connection pool
	poolConfig, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("parse pool config: %w", err)
	}

	// Pool settings - these are important!
	poolConfig.MaxConns = 25                       // Max connections (tune based on load)
	poolConfig.MinConns = 5                        // Keep some connections warm
	poolConfig.MaxConnLifetime = 1 * time.Hour     // Recycle connections periodically
	poolConfig.MaxConnIdleTime = 30 * time.Minute  // Close idle connections
	poolConfig.HealthCheckPeriod = 1 * time.Minute // Check connection health

	// Create the pool
	pool, err := pgxpool.NewWithConfig(ctx, poolConfig)
	if err != nil {
		return nil, fmt.Errorf("create pool: %w", err)
	}

	// Test the connection
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping database: %w", err)
	}

	logger.Info("database connection established",
		zap.String("host", cfg.Host),
		zap.Int("port", cfg.Port),
		zap.String("database", cfg.Database),
		zap.Int32("max_conns", poolConfig.MaxConns),
	)

	return &DB{
		pool:   pool,
		logger: logger,
	}, nil
}

// Close closes the database connection pool
func (db *DB) Close() {
	db.logger.Info("closing database connection pool")
	db.pool.Close()
}

// Pool returns the underlying connection pool
// This is used by the repository layer
func (db *DB) Pool() *pgxpool.Pool {
	return db.pool
}

// Health checks if the database is reachable
func (db *DB) Health(ctx context.Context) error {
	return db.pool.Ping(ctx)
}
