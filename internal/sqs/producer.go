package sqs

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"go.uber.org/zap"

	"github.com/lalithlochan/nimbus/internal/db"
)

// Config holds SQS configuration.
type Config struct {
	Region   string
	QueueURL string
	DLQURL   string
}

// Message is the payload sent to SQS.
type Message struct {
	NotificationID string          `json:"notification_id"`
	TenantID       string          `json:"tenant_id"`
	UserID         string          `json:"user_id"`
	Channel        string          `json:"channel"`
	Payload        json.RawMessage `json:"payload"`
	Attempt        int             `json:"attempt"`
	EnqueuedAt     int64           `json:"enqueued_at"`
}

// Producer sends notifications to SQS.
type Producer struct {
	client   *sqs.Client
	queueURL string
	logger   *zap.Logger
}

// NewProducer creates a new SQS producer.
func NewProducer(ctx context.Context, cfg Config, logger *zap.Logger) (*Producer, error) {
	awsCfg, err := config.LoadDefaultConfig(ctx, config.WithRegion(cfg.Region))
	if err != nil {
		return nil, fmt.Errorf("failed to load AWS config: %w", err)
	}

	client := sqs.NewFromConfig(awsCfg)

	logger.Info("sqs producer initialized",
		zap.String("queue_url", cfg.QueueURL),
	)

	return &Producer{
		client:   client,
		queueURL: cfg.QueueURL,
		logger:   logger,
	}, nil
}

// Enqueue sends a notification to SQS for asynchronous processing.
// Returns the message ID for tracking.
func (p *Producer) Enqueue(ctx context.Context, notif *db.Notification) (string, error) {
	msg := Message{
		NotificationID: notif.ID.String(),
		TenantID:       notif.TenantID.String(),
		UserID:         notif.UserID.String(),
		Channel:        notif.Channel,
		Payload:        notif.Payload,
		Attempt:        notif.Attempt,
		EnqueuedAt:     time.Now().UnixNano(),
	}

	body, err := json.Marshal(msg)
	if err != nil {
		return "", fmt.Errorf("failed to marshal message: %w", err)
	}

	input := &sqs.SendMessageInput{
		QueueUrl:    aws.String(p.queueURL),
		MessageBody: aws.String(string(body)),
	}

	result, err := p.client.SendMessage(ctx, input)
	if err != nil {
		p.logger.Error("failed to send message to sqs",
			zap.Error(err),
			zap.String("notification_id", notif.ID.String()),
		)
		return "", fmt.Errorf("sqs send failed: %w", err)
	}

	return *result.MessageId, nil
}

// EnqueueBatch sends multiple notifications to SQS efficiently.
func (p *Producer) EnqueueBatch(ctx context.Context, notifications []*db.Notification) ([]string, error) {
	if len(notifications) == 0 {
		return []string{}, nil
	}

	messageIDs := make([]string, 0, len(notifications))
	for _, notif := range notifications {
		msgID, err := p.Enqueue(ctx, notif)
		if err != nil {
			p.logger.Warn("failed to enqueue notification", zap.Error(err))
			continue
		}
		messageIDs = append(messageIDs, msgID)
	}

	return messageIDs, nil
}

// Close closes the SQS producer.
func (p *Producer) Close() {
	// AWS SDK v2 clients don't require explicit Close()
}
