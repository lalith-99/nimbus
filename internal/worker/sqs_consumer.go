package worker

import (
	"context"
	"sync"
	"time"

	"github.com/google/uuid"
	"go.uber.org/zap"

	"github.com/lalithlochan/nimbus/internal/db"
	"github.com/lalithlochan/nimbus/internal/sqs"
)

// SQSReceiver is the transport contract the consumer depends on. Kept as an
// interface so it can be faked in tests without touching AWS.
type SQSReceiver interface {
	Receive(ctx context.Context) ([]sqs.ReceivedMessage, error)
	DeleteBatch(ctx context.Context, receiptHandles []string) error
}

// ClaimByIDRepository is the single persistence operation the SQS hot path needs:
// atomically claim one row by id. The claim is what guarantees the SQS consumer
// and the DB poller never double-send the same notification.
type ClaimByIDRepository interface {
	ClaimNotificationByID(ctx context.Context, id uuid.UUID) (*db.Notification, error)
}

// SQSConsumerConfig tunes throughput. At ~10M/day (~116/s average, higher bursts)
// the defaults below give plenty of headroom and are the knobs you'd scale.
type SQSConsumerConfig struct {
	// Receivers is the number of concurrent long-poll loops. Each loop pulls
	// batches of up to 10 independently, multiplying receive throughput.
	Receivers int
	// Concurrency caps the number of in-flight sends processed per batch, so a
	// slow downstream (SES/SNS/webhook) can't spawn unbounded goroutines.
	Concurrency int
}

// SQSConsumer is the event-driven hot path. It long-polls SQS, atomically claims
// each notification in the DB (the no-double-send guard shared with the poller),
// dispatches it through the SAME Dispatcher the poller uses, then acks the message.
//
// Retry semantics: an SQS message is one-shot. After the first attempt we always
// ack it. If the send failed but has retries left, Dispatch schedules it back to
// 'pending' with a future next_retry_at, and the DB poller backstop picks it up
// when due. This keeps the fast path simple while the poller guarantees eventual
// delivery of retries and anything SQS ever drops.
type SQSConsumer struct {
	receiver   SQSReceiver
	repo       ClaimByIDRepository
	dispatcher *Dispatcher
	cfg        SQSConsumerConfig
	logger     *zap.Logger
}

// NewSQSConsumer wires the consumer. Zero-value config fields get sane defaults.
func NewSQSConsumer(
	receiver SQSReceiver,
	repo ClaimByIDRepository,
	dispatcher *Dispatcher,
	cfg SQSConsumerConfig,
	logger *zap.Logger,
) *SQSConsumer {
	if cfg.Receivers <= 0 {
		cfg.Receivers = 3
	}
	if cfg.Concurrency <= 0 {
		cfg.Concurrency = 10
	}
	return &SQSConsumer{
		receiver:   receiver,
		repo:       repo,
		dispatcher: dispatcher,
		cfg:        cfg,
		logger:     logger,
	}
}

// Start launches cfg.Receivers concurrent receive loops and blocks until ctx is
// cancelled. Run it in a goroutine.
func (s *SQSConsumer) Start(ctx context.Context) {
	s.logger.Info("sqs consumer starting",
		zap.Int("receivers", s.cfg.Receivers),
		zap.Int("concurrency_per_batch", s.cfg.Concurrency),
	)

	var wg sync.WaitGroup
	for i := 0; i < s.cfg.Receivers; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			s.receiveLoop(ctx, id)
		}(i)
	}
	wg.Wait()

	s.logger.Info("sqs consumer stopped")
}

// receiveLoop long-polls for batches until the context is cancelled. On a receive
// error it backs off briefly (via a cancellable timer) to avoid a tight spin.
func (s *SQSConsumer) receiveLoop(ctx context.Context, id int) {
	for {
		if ctx.Err() != nil {
			return
		}

		messages, err := s.receiver.Receive(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			s.logger.Error("sqs receive failed", zap.Int("receiver", id), zap.Error(err))
			select {
			case <-ctx.Done():
				return
			case <-time.After(time.Second):
			}
			continue
		}

		if len(messages) == 0 {
			continue
		}

		s.handleBatch(ctx, messages)
	}
}

// handleBatch processes a batch concurrently (bounded by cfg.Concurrency) and
// batch-deletes every message that was handled to completion.
func (s *SQSConsumer) handleBatch(ctx context.Context, messages []sqs.ReceivedMessage) {
	sem := make(chan struct{}, s.cfg.Concurrency)
	var wg sync.WaitGroup
	var mu sync.Mutex
	acked := make([]string, 0, len(messages))

	for i := range messages {
		msg := messages[i]
		wg.Add(1)
		sem <- struct{}{}
		go func() {
			defer wg.Done()
			defer func() { <-sem }()

			if s.handleMessage(ctx, msg) {
				mu.Lock()
				acked = append(acked, msg.ReceiptHandle)
				mu.Unlock()
			}
		}()
	}
	wg.Wait()

	if len(acked) == 0 {
		return
	}
	if err := s.receiver.DeleteBatch(ctx, acked); err != nil {
		// Not fatal: un-acked messages will reappear after the visibility
		// timeout, and the DB claim guard makes reprocessing a no-op.
		s.logger.Warn("sqs batch delete failed; messages will be redelivered",
			zap.Int("count", len(acked)),
			zap.Error(err),
		)
	}
}

// handleMessage processes one message. It returns true when the message should be
// acked (deleted) and false when it should be left for redelivery.
func (s *SQSConsumer) handleMessage(ctx context.Context, msg sqs.ReceivedMessage) bool {
	id, err := uuid.Parse(msg.Body.NotificationID)
	if err != nil {
		// Malformed id can never succeed — ack to drop it.
		s.logger.Error("invalid notification id in sqs message; dropping",
			zap.String("notification_id", msg.Body.NotificationID),
			zap.Error(err),
		)
		return true
	}

	notif, err := s.repo.ClaimNotificationByID(ctx, id)
	if err != nil {
		// Transient DB error — leave the message for redelivery.
		s.logger.Error("failed to claim notification; will redeliver",
			zap.String("notification_id", id.String()),
			zap.Error(err),
		)
		return false
	}
	if notif == nil {
		// Not claimable: the poller already handled it, it's terminal, or it's a
		// retry that isn't due yet. Ack — the DB is the source of truth.
		s.logger.Debug("notification not claimable via sqs; acking",
			zap.String("notification_id", id.String()),
		)
		return true
	}

	// Row is ours ('processing'). Reuse the exact same dispatch path as the poller.
	s.dispatcher.Dispatch(ctx, notif)
	return true
}
