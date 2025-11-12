-- Create notifications table
CREATE TABLE IF NOT EXISTS notifications (
    -- Identity
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    
    -- Multi-tenancy
    tenant_id UUID NOT NULL,
    user_id UUID NOT NULL,
    
    -- Message details
    channel VARCHAR(20) NOT NULL,
    payload JSONB NOT NULL,
    
    -- State management
    status VARCHAR(20) NOT NULL DEFAULT 'pending',
    attempt INT NOT NULL DEFAULT 0,
    error_message TEXT,
    
    -- Retry logic
    next_retry_at TIMESTAMPTZ,
    
    -- Audit trail
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    
    -- Constraints
    CONSTRAINT chk_channel CHECK (channel IN ('email', 'sms', 'webhook')),
    CONSTRAINT chk_status CHECK (status IN ('pending', 'processing', 'sent', 'failed'))
);

-- Indexes for common query patterns

-- Worker polling for pending notifications ready to retry
CREATE INDEX idx_notifications_retry 
ON notifications(status, next_retry_at, created_at) 
WHERE status IN ('pending', 'processing');

-- Tenant-based queries (list user's notifications)
CREATE INDEX idx_notifications_tenant 
ON notifications(tenant_id, created_at DESC);

-- User-specific queries (more selective)
CREATE INDEX idx_notifications_user 
ON notifications(tenant_id, user_id, created_at DESC);

-- Channel-based analytics (optional, add if needed)
CREATE INDEX idx_notifications_channel 
ON notifications(channel, status);

-- Trigger to auto-update updated_at
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER update_notifications_updated_at
BEFORE UPDATE ON notifications
FOR EACH ROW
EXECUTE FUNCTION update_updated_at_column();
