package sqs

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	sqstypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"go.uber.org/zap"
)

// maxReceiveBatch is the largest batch SQS will return in a single ReceiveMessage
// call. Pulling 10 at a time (instead of 1) amortizes the network round-trip and
// is the first lever for throughput at high volume.
const maxReceiveBatch = 10

// ReceivedMessage pairs a decoded notification payload with the SQS receipt handle
// needed to delete (ack) or extend visibility on it.
type ReceivedMessage struct {
	Body          *Message
	ReceiptHandle string
}

// Consumer is the low-level SQS transport: it knows how to long-poll for a batch
// of messages, delete them, and extend visibility. It holds NO business logic —
// the orchestration (claiming rows, sending, retry/DLQ) lives in the worker layer
// so the same dispatch code is shared with the DB poller.
type Consumer struct {
	client   *sqs.Client
	queueURL string
	logger   *zap.Logger
}

// NewConsumer creates a new SQS consumer transport.
func NewConsumer(ctx context.Context, cfg Config, logger *zap.Logger) (*Consumer, error) {
	awsCfg, err := config.LoadDefaultConfig(ctx, config.WithRegion(cfg.Region))
	if err != nil {
		return nil, fmt.Errorf("failed to load AWS config: %w", err)
	}

	client := sqs.NewFromConfig(awsCfg)

	logger.Info("sqs consumer initialized",
		zap.String("queue_url", cfg.QueueURL),
	)

	return &Consumer{
		client:   client,
		queueURL: cfg.QueueURL,
		logger:   logger,
	}, nil
}

// Receive long-polls SQS and returns up to maxReceiveBatch messages. Long polling
// (WaitTimeSeconds=20) keeps idle cost low and avoids busy-spinning empty receives.
// Messages that fail to decode are deleted immediately as poison — they can never
// succeed and would otherwise loop until they hit the DLQ redrive policy.
func (c *Consumer) Receive(ctx context.Context) ([]ReceivedMessage, error) {
	input := &sqs.ReceiveMessageInput{
		QueueUrl:            aws.String(c.queueURL),
		MaxNumberOfMessages: maxReceiveBatch,
		WaitTimeSeconds:     20,
		VisibilityTimeout:   60,
	}

	result, err := c.client.ReceiveMessage(ctx, input)
	if err != nil {
		return nil, fmt.Errorf("sqs receive failed: %w", err)
	}

	if len(result.Messages) == 0 {
		return nil, nil
	}

	messages := make([]ReceivedMessage, 0, len(result.Messages))
	for i := range result.Messages {
		msgData := result.Messages[i]

		var msg Message
		if err := json.Unmarshal([]byte(*msgData.Body), &msg); err != nil {
			c.logger.Error("failed to unmarshal message; deleting poison message",
				zap.Error(err),
			)
			// Poison message: drop it so it doesn't loop forever.
			_ = c.Delete(ctx, *msgData.ReceiptHandle)
			continue
		}

		messages = append(messages, ReceivedMessage{
			Body:          &msg,
			ReceiptHandle: *msgData.ReceiptHandle,
		})
	}

	return messages, nil
}

// Delete removes (acks) a single message from SQS after it has been handled.
func (c *Consumer) Delete(ctx context.Context, receiptHandle string) error {
	input := &sqs.DeleteMessageInput{
		QueueUrl:      aws.String(c.queueURL),
		ReceiptHandle: aws.String(receiptHandle),
	}

	if _, err := c.client.DeleteMessage(ctx, input); err != nil {
		return fmt.Errorf("sqs delete failed: %w", err)
	}

	return nil
}

// DeleteBatch acks up to 10 messages in a single call. Batching deletes cuts the
// SQS API call count by up to 10x on the hot path.
func (c *Consumer) DeleteBatch(ctx context.Context, receiptHandles []string) error {
	if len(receiptHandles) == 0 {
		return nil
	}

	entries := make([]sqstypes.DeleteMessageBatchRequestEntry, 0, len(receiptHandles))
	for i, handle := range receiptHandles {
		entries = append(entries, sqstypes.DeleteMessageBatchRequestEntry{
			Id:            aws.String(fmt.Sprintf("%d", i)),
			ReceiptHandle: aws.String(handle),
		})
	}

	input := &sqs.DeleteMessageBatchInput{
		QueueUrl: aws.String(c.queueURL),
		Entries:  entries,
	}

	if _, err := c.client.DeleteMessageBatch(ctx, input); err != nil {
		return fmt.Errorf("sqs batch delete failed: %w", err)
	}

	return nil
}

// ChangeVisibility extends the visibility timeout for an in-flight message. Used
// as a heartbeat when a send is legitimately taking longer than the initial
// timeout, so SQS doesn't redeliver a message that's still being worked on.
func (c *Consumer) ChangeVisibility(ctx context.Context, receiptHandle string, seconds int32) error {
	input := &sqs.ChangeMessageVisibilityInput{
		QueueUrl:          aws.String(c.queueURL),
		ReceiptHandle:     aws.String(receiptHandle),
		VisibilityTimeout: seconds,
	}

	if _, err := c.client.ChangeMessageVisibility(ctx, input); err != nil {
		return fmt.Errorf("sqs change visibility failed: %w", err)
	}

	return nil
}

// Close closes the SQS consumer.
func (c *Consumer) Close() {
	// AWS SDK v2 clients don't require explicit Close()
}
