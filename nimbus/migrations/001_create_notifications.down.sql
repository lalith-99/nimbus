-- Rollback: drop notifications table and related objects
DROP TRIGGER IF EXISTS update_notifications_updated_at ON notifications;
DROP FUNCTION IF EXISTS update_updated_at_column();
DROP TABLE IF EXISTS notifications;
