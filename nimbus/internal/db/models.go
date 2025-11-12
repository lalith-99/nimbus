package db

import (
	"encoding/json"
	"time"

	"github.com/google/uuid"
)

// Notification represents a notification in the database
type Notification struct {
	ID           uuid.UUID       `json:"id"`
	TenantID     uuid.UUID       `json:"tenant_id"`
	UserID       uuid.UUID       `json:"user_id"`
	Channel      string          `json:"channel"`
	Payload      json.RawMessage `json:"payload"`
	Status       string          `json:"status"`
	Attempt      int             `json:"attempt"`
	ErrorMessage *string         `json:"error_message,omitempty"`
	NextRetryAt  *time.Time      `json:"next_retry_at,omitempty"`
	CreatedAt    time.Time       `json:"created_at"`
	UpdatedAt    time.Time       `json:"updated_at"`
}

// Status constants
const (
	StatusPending    = "pending"
	StatusProcessing = "processing"
	StatusSent       = "sent"
	StatusFailed     = "failed"
)

// Channel constants
const (
	ChannelEmail   = "email"
	ChannelSMS     = "sms"
	ChannelWebhook = "webhook"
)
