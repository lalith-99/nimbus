package db

import (
	"context"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"go.uber.org/zap"
)

// Repository handles database operations for notifications
type Repository struct {
	db     *DB
	logger *zap.Logger
}

// NewRepository creates a new notification repository
func NewRepository(db *DB, logger *zap.Logger) *Repository {
	return &Repository{
		db:     db,
		logger: logger,
	}
}

// CreateNotification inserts a new notification into the database
func (r *Repository) CreateNotification(ctx context.Context, notif *Notification) error {
	query := `
		INSERT INTO notifications (
			id, tenant_id, user_id, channel, payload, 
			status, attempt, next_retry_at
		) VALUES (
			$1, $2, $3, $4, $5, $6, $7, $8
		)
		RETURNING created_at, updated_at
	`

	err := r.db.Pool().QueryRow(
		ctx,
		query,
		notif.ID,
		notif.TenantID,
		notif.UserID,
		notif.Channel,
		notif.Payload,
		notif.Status,
		notif.Attempt,
		notif.NextRetryAt,
	).Scan(&notif.CreatedAt, &notif.UpdatedAt)

	if err != nil {
		r.logger.Error("failed to create notification",
			zap.Error(err),
			zap.String("notification_id", notif.ID.String()),
		)
		return fmt.Errorf("insert notification: %w", err)
	}

	r.logger.Info("notification created",
		zap.String("notification_id", notif.ID.String()),
		zap.String("tenant_id", notif.TenantID.String()),
		zap.String("channel", notif.Channel),
	)

	return nil
}

// GetNotification retrieves a notification by ID
func (r *Repository) GetNotification(ctx context.Context, id uuid.UUID) (*Notification, error) {
	query := `
		SELECT 
			id, tenant_id, user_id, channel, payload,
			status, attempt, error_message, next_retry_at,
			created_at, updated_at
		FROM notifications
		WHERE id = $1
	`

	var notif Notification
	err := r.db.Pool().QueryRow(ctx, query, id).Scan(
		&notif.ID,
		&notif.TenantID,
		&notif.UserID,
		&notif.Channel,
		&notif.Payload,
		&notif.Status,
		&notif.Attempt,
		&notif.ErrorMessage,
		&notif.NextRetryAt,
		&notif.CreatedAt,
		&notif.UpdatedAt,
	)

	if err == pgx.ErrNoRows {
		return nil, fmt.Errorf("notification not found: %s", id)
	}

	if err != nil {
		r.logger.Error("failed to get notification",
			zap.Error(err),
			zap.String("notification_id", id.String()),
		)
		return nil, fmt.Errorf("query notification: %w", err)
	}

	return &notif, nil
}

// UpdateNotificationStatus updates the status and error message of a notification
func (r *Repository) UpdateNotificationStatus(
	ctx context.Context,
	id uuid.UUID,
	status string,
	attempt int,
	errorMsg *string,
) error {
	query := `
		UPDATE notifications
		SET status = $1, attempt = $2, error_message = $3
		WHERE id = $4
	`

	result, err := r.db.Pool().Exec(ctx, query, status, attempt, errorMsg, id)
	if err != nil {
		r.logger.Error("failed to update notification status",
			zap.Error(err),
			zap.String("notification_id", id.String()),
		)
		return fmt.Errorf("update notification status: %w", err)
	}

	if result.RowsAffected() == 0 {
		return fmt.Errorf("notification not found: %s", id)
	}

	return nil
}

// ListNotificationsByTenant retrieves notifications for a tenant with pagination
func (r *Repository) ListNotificationsByTenant(
	ctx context.Context,
	tenantID uuid.UUID,
	limit int,
	offset int,
) ([]*Notification, error) {
	query := `
		SELECT 
			id, tenant_id, user_id, channel, payload,
			status, attempt, error_message, next_retry_at,
			created_at, updated_at
		FROM notifications
		WHERE tenant_id = $1
		ORDER BY created_at DESC
		LIMIT $2 OFFSET $3
	`

	rows, err := r.db.Pool().Query(ctx, query, tenantID, limit, offset)
	if err != nil {
		return nil, fmt.Errorf("query notifications: %w", err)
	}
	defer rows.Close()

	var notifications []*Notification
	for rows.Next() {
		var notif Notification
		err := rows.Scan(
			&notif.ID,
			&notif.TenantID,
			&notif.UserID,
			&notif.Channel,
			&notif.Payload,
			&notif.Status,
			&notif.Attempt,
			&notif.ErrorMessage,
			&notif.NextRetryAt,
			&notif.CreatedAt,
			&notif.UpdatedAt,
		)
		if err != nil {
			return nil, fmt.Errorf("scan notification: %w", err)
		}
		notifications = append(notifications, &notif)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate rows: %w", err)
	}

	return notifications, nil
}
