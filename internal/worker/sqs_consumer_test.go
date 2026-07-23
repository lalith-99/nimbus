package worker

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"go.uber.org/zap"

	"github.com/lalithlochan/nimbus/internal/db"
	"github.com/lalithlochan/nimbus/internal/sqs"
)

// fakeReceiver serves a single preset batch then returns empty batches, and
// records which receipt handles were acked (deleted).
type fakeReceiver struct {
	mu        sync.Mutex
	batch     []sqs.ReceivedMessage
	served    bool
	deleted   []string
	deleteErr error
}

func (f *fakeReceiver) Receive(ctx context.Context) ([]sqs.ReceivedMessage, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.served {
		return nil, nil
	}
	f.served = true
	return f.batch, nil
}

func (f *fakeReceiver) DeleteBatch(ctx context.Context, receiptHandles []string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.deleted = append(f.deleted, receiptHandles...)
	return f.deleteErr
}

func (f *fakeReceiver) deletedHandles() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]string, len(f.deleted))
	copy(out, f.deleted)
	return out
}

// fakeClaimRepo implements ClaimByIDRepository + StatusRepository (what the
// Dispatcher needs). claimResult controls whether a claim "wins".
type fakeClaimRepo struct {
	mu          sync.Mutex
	claimResult map[uuid.UUID]*db.Notification
	claimErr    map[uuid.UUID]error
	claimCalls  int
	statusCalls int
}

func (r *fakeClaimRepo) ClaimNotificationByID(ctx context.Context, id uuid.UUID) (*db.Notification, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.claimCalls++
	if err, ok := r.claimErr[id]; ok {
		return nil, err
	}
	return r.claimResult[id], nil
}

func (r *fakeClaimRepo) UpdateNotificationStatus(ctx context.Context, id uuid.UUID, status string, attempt int, errorMsg *string, nextRetryAt *time.Time) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.statusCalls++
	return nil
}

func (r *fakeClaimRepo) MoveToDeadLetter(ctx context.Context, notif *db.Notification, lastError string) (*db.DeadLetterNotification, error) {
	return &db.DeadLetterNotification{ID: uuid.New()}, nil
}

func msg(id uuid.UUID, handle string) sqs.ReceivedMessage {
	return sqs.ReceivedMessage{
		Body:          &sqs.Message{NotificationID: id.String()},
		ReceiptHandle: handle,
	}
}

func newTestConsumer(recv SQSReceiver, repo *fakeClaimRepo, sender Sender) *SQSConsumer {
	logger := zap.NewNop()
	dispatcher := NewDispatcher(repo, sender, 3, logger)
	// Receivers=1 keeps the single preset batch deterministic.
	return NewSQSConsumer(recv, repo, dispatcher, SQSConsumerConfig{Receivers: 1, Concurrency: 4}, logger)
}

func TestSQSConsumer_ClaimedNotification_IsSentAndAcked(t *testing.T) {
	id := uuid.New()
	repo := &fakeClaimRepo{
		claimResult: map[uuid.UUID]*db.Notification{
			id: {ID: id, Status: db.StatusProcessing, Channel: db.ChannelEmail},
		},
	}
	recv := &fakeReceiver{batch: []sqs.ReceivedMessage{msg(id, "h1")}}
	sender := &MockSender{}

	c := newTestConsumer(recv, repo, sender)

	c.handleBatch(context.Background(), recv.batch)

	if sender.sendCalls != 1 {
		t.Errorf("expected 1 send, got %d", sender.sendCalls)
	}
	if got := recv.deletedHandles(); len(got) != 1 || got[0] != "h1" {
		t.Errorf("expected message h1 acked, got %v", got)
	}
}

func TestSQSConsumer_NotClaimable_IsAckedWithoutSending(t *testing.T) {
	// Simulates the poller (or a duplicate SQS delivery) already owning the row:
	// ClaimNotificationByID returns nil → no send, but we still ack.
	id := uuid.New()
	repo := &fakeClaimRepo{claimResult: map[uuid.UUID]*db.Notification{}}
	recv := &fakeReceiver{batch: []sqs.ReceivedMessage{msg(id, "h1")}}
	sender := &MockSender{}

	c := newTestConsumer(recv, repo, sender)

	c.handleBatch(context.Background(), recv.batch)

	if sender.sendCalls != 0 {
		t.Errorf("expected 0 sends for unclaimable row (no double-send), got %d", sender.sendCalls)
	}
	if got := recv.deletedHandles(); len(got) != 1 {
		t.Errorf("expected unclaimable message to still be acked, got %v", got)
	}
}

func TestSQSConsumer_ClaimError_IsNotAcked(t *testing.T) {
	// Transient DB error → leave message for redelivery (do NOT ack).
	id := uuid.New()
	repo := &fakeClaimRepo{claimErr: map[uuid.UUID]error{id: errors.New("db down")}}
	recv := &fakeReceiver{batch: []sqs.ReceivedMessage{msg(id, "h1")}}
	sender := &MockSender{}

	c := newTestConsumer(recv, repo, sender)

	c.handleBatch(context.Background(), recv.batch)

	if sender.sendCalls != 0 {
		t.Errorf("expected 0 sends on claim error, got %d", sender.sendCalls)
	}
	if got := recv.deletedHandles(); len(got) != 0 {
		t.Errorf("expected message NOT acked on claim error, got %v", got)
	}
}

func TestSQSConsumer_InvalidID_IsDropped(t *testing.T) {
	repo := &fakeClaimRepo{}
	bad := sqs.ReceivedMessage{Body: &sqs.Message{NotificationID: "not-a-uuid"}, ReceiptHandle: "h1"}
	recv := &fakeReceiver{batch: []sqs.ReceivedMessage{bad}}
	sender := &MockSender{}

	c := newTestConsumer(recv, repo, sender)

	c.handleBatch(context.Background(), recv.batch)

	if repo.claimCalls != 0 {
		t.Errorf("expected no claim attempt for invalid id, got %d", repo.claimCalls)
	}
	if got := recv.deletedHandles(); len(got) != 1 {
		t.Errorf("expected poison message to be dropped (acked), got %v", got)
	}
}
