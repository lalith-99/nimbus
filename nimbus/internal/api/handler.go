package api

import (
	"encoding/json"
	"net/http"

	"go.uber.org/zap"
)

// NotificationRequest represents the incoming request body
type NotificationRequest struct {
	TenantID string          `json:"tenant_id"`
	UserID   string          `json:"user_id"`
	Channel  string          `json:"channel"`
	Payload  json.RawMessage `json:"payload"`
}

// NotificationResponse is returned after creating a notification
type NotificationResponse struct {
	ID string `json:"id"`
}

// ErrorResponse represents an error in problem+json format
type ErrorResponse struct {
	Type   string `json:"type"`
	Title  string `json:"title"`
	Status int    `json:"status"`
	Detail string `json:"detail,omitempty"`
}

// Handler holds dependencies for API handlers
type Handler struct {
	logger *zap.Logger
}

// NewHandler creates a new API handler
func NewHandler(logger *zap.Logger) *Handler {
	return &Handler{
		logger: logger,
	}
}

// CreateNotification handles POST /v1/notifications
func (h *Handler) CreateNotification(w http.ResponseWriter, r *http.Request) {
	var req NotificationRequest

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		h.writeError(w, http.StatusBadRequest, "invalid_request", "Malformed JSON body", err.Error())
		return
	}

	// TODO: Validate required fields
	// TODO: Check idempotency key
	// TODO: Write to database
	// TODO: Enqueue to SQS

	h.logger.Info("notification request received",
		zap.String("tenant_id", req.TenantID),
		zap.String("user_id", req.UserID),
		zap.String("channel", req.Channel),
	)

	// For now, just return a dummy ID
	resp := NotificationResponse{
		ID: "placeholder-id",
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(resp)
}

func (h *Handler) writeError(w http.ResponseWriter, status int, errType, title, detail string) {
	w.Header().Set("Content-Type", "application/problem+json")
	w.WriteHeader(status)

	json.NewEncoder(w).Encode(ErrorResponse{
		Type:   errType,
		Title:  title,
		Status: status,
		Detail: detail,
	})
}
