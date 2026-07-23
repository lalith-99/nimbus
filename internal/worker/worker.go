package worker

import (
	"context"
	"time"

	"go.uber.org/zap"

	"github.com/google/uuid"
	"github.com/lalithlochan/nimbus/internal/db"
)

type Repository interface {
	// ClaimPendingNotifications atomically claims a batch (FOR UPDATE SKIP LOCKED),
	// marking them 'processing' so no other replica can pick the same rows.
	ClaimPendingNotifications(ctx context.Context, limit int) ([]*db.Notification, error)
	UpdateNotificationStatus(ctx context.Context, id uuid.UUID, status string, attempt int, errorMsg *string, nextRetryAt *time.Time) error
	MoveToDeadLetter(ctx context.Context, notif *db.Notification, lastError string) (*db.DeadLetterNotification, error)
}

// StatusRepository is the narrow subset of persistence operations the Dispatcher
// needs to record the outcome of a send. Kept small on purpose so both the DB
// poller (Worker) and the SQS consumer can share the exact same dispatch logic.
type StatusRepository interface {
	UpdateNotificationStatus(ctx context.Context, id uuid.UUID, status string, attempt int, errorMsg *string, nextRetryAt *time.Time) error
	MoveToDeadLetter(ctx context.Context, notif *db.Notification, lastError string) (*db.DeadLetterNotification, error)
}

type Worker struct {
	repo       Repository
	dispatcher *Dispatcher
	config     Config
	logger     *zap.Logger
}

type Config struct {
	PollInterval time.Duration
	BatchSize    int
	MaxRetries   int
}

// New creates a worker with default config values.
func New(repo Repository, sender Sender, cfg Config, logger *zap.Logger) *Worker {

	if cfg.PollInterval == 0 {
		cfg.PollInterval = 5 * time.Second
	}
	if cfg.BatchSize == 0 {
		cfg.BatchSize = 5
	}
	if cfg.MaxRetries == 0 {
		cfg.MaxRetries = 3
	}

	return &Worker{
		repo:       repo,
		dispatcher: NewDispatcher(repo, sender, cfg.MaxRetries, logger),
		config:     cfg,
		logger:     logger,
	}
}

func (w *Worker) Start(ctx context.Context) {
	ticker := time.NewTicker(w.config.PollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			w.logger.Info("worker stopping")
			return
		case <-ticker.C:
			w.logger.Debug("checking for notifications",
				zap.Int("batch_size", w.config.BatchSize),
			)
			w.processBatch(ctx)
		}
	}
}

func (w *Worker) processBatch(ctx context.Context) {
	// Atomically claim a batch. Each replica gets a disjoint set of rows
	// (FOR UPDATE SKIP LOCKED), so we can scale workers horizontally without
	// double-sending. The claim also reclaims rows stranded by crashed workers.
	notifications, err := w.repo.ClaimPendingNotifications(ctx, w.config.BatchSize)
	if err != nil {
		w.logger.Error("failed to claim pending notifications", zap.Error(err))
		return
	}
	if len(notifications) == 0 {
		return
	}
	// Loop through each notification from the list of notifications
	for _, notif := range notifications {
		// Process each notification
		w.processNotification(ctx, notif)
	}
}

// processNotification is a thin delegate kept for the poller path (and existing
// tests). The real send/retry/DLQ logic lives in Dispatcher.Dispatch so it can be
// shared verbatim with the SQS consumer — one code path, one set of semantics.
func (w *Worker) processNotification(ctx context.Context, notif *db.Notification) {
	w.dispatcher.Dispatch(ctx, notif)
}

// Dispatcher owns the "given an already-claimed notification, send it and record
// the outcome" logic. It is deliberately decoupled from HOW the notification was
// claimed (batch DB poll vs. single SQS-driven claim) so both delivery paths
// reuse identical send/retry/dead-letter behavior.
type Dispatcher struct {
	repo       StatusRepository
	sender     Sender
	maxRetries int
	logger     *zap.Logger
}

// NewDispatcher builds a Dispatcher. maxRetries defaults to 3 when zero.
func NewDispatcher(repo StatusRepository, sender Sender, maxRetries int, logger *zap.Logger) *Dispatcher {
	if maxRetries == 0 {
		maxRetries = 3
	}
	return &Dispatcher{
		repo:       repo,
		sender:     sender,
		maxRetries: maxRetries,
		logger:     logger,
	}
}

// Dispatch sends a notification that has ALREADY been atomically claimed
// ('processing') by the caller, then records the terminal outcome:
//   - success            → status 'sent'
//   - failure, retries left → status back to 'pending' with a future next_retry_at
//   - failure, retries exhausted → moved to the dead-letter queue
//
// On the SQS path, a scheduled retry ('pending' + future next_retry_at) is picked
// up later by the DB poller backstop, since the SQS message is one-shot.
func (d *Dispatcher) Dispatch(ctx context.Context, notif *db.Notification) {
	// The row was already atomically marked 'processing' by the claim, so we go
	// straight to sending — no extra status write needed here.
	err := d.sender.Send(ctx, notif)
	newAttempt := notif.Attempt + 1

	if err != nil {
		d.logger.Error("failed to send notification",
			zap.Error(err),
			zap.String("notification_id", notif.ID.String()),
			zap.String("channel", notif.Channel),
			zap.Int("attempt", newAttempt),
		)

		errMsg := err.Error()

		if newAttempt >= d.maxRetries {
			// Max retries reached, move to dead letter queue
			_, dlqErr := d.repo.MoveToDeadLetter(ctx, notif, errMsg)
			if dlqErr != nil {
				d.logger.Error("failed to move notification to dead letter queue",
					zap.String("id", notif.ID.String()),
					zap.Error(dlqErr),
				)
			} else {
				d.logger.Info("notification moved to dead letter queue",
					zap.String("id", notif.ID.String()),
					zap.Int("attempts", newAttempt),
				)
			}
		} else {
			nextRetry := d.calculateNextRetry(newAttempt)
			_ = d.repo.UpdateNotificationStatus(ctx, notif.ID, "pending", newAttempt, &errMsg, &nextRetry)
		}
	} else {
		d.logger.Info("notification sent",
			zap.String("id", notif.ID.String()),
		)
		_ = d.repo.UpdateNotificationStatus(ctx, notif.ID, "sent", newAttempt, nil, nil)
	}
}

// Calculate next retry time based on attempt
func (d *Dispatcher) calculateNextRetry(attempt int) time.Time {
	delays := []time.Duration{
		1 * time.Minute,  // attempt 1 → wait 1 min
		5 * time.Minute,  // attempt 2 → wait 5 min
		15 * time.Minute, // attempt 3 → w–––
	}

	idx := attempt - 1
	if idx >= len(delays) {
		idx = len(delays) - 1
	}

	return time.Now().Add(delays[idx])
}
