# Database Setup

## Local Development

### Prerequisites

```bash
# Install PostgreSQL (if not already installed)
brew install postgresql@16
brew services start postgresql@16
```

### Quick Setup

```bash
# Run the setup script
./scripts/setup-db.sh
```

This will:
1. Create the `nimbus` database
2. Run all migrations

### Manual Setup

If you prefer manual setup:

```bash
# Create database
createdb -U postgres nimbus

# Run migrations
psql -U postgres -d nimbus -f migrations/001_create_notifications.up.sql
```

### Connection String

```
postgresql://postgres:postgres@localhost:5432/nimbus?sslmode=disable
```

### Environment Variables

The app reads these env vars (or uses defaults shown):

```bash
DB_HOST=localhost
DB_PORT=5432
DB_USER=postgres
DB_PASSWORD=postgres
DB_NAME=nimbus
DB_SSLMODE=disable
```

## Migrations

Migrations are in `migrations/` directory:
- `*.up.sql` - Apply changes
- `*.down.sql` - Rollback changes

### Rollback Example

```bash
psql -U postgres -d nimbus -f migrations/001_create_notifications.down.sql
```

## Schema

### notifications table

```sql
id            UUID          Primary key
tenant_id     UUID          Multi-tenancy isolation
user_id       UUID          User who owns the notification
channel       VARCHAR(20)   'email' | 'sms' | 'webhook'
payload       JSONB         Channel-specific data
status        VARCHAR(20)   'pending' | 'processing' | 'sent' | 'failed'
attempt       INT           Retry attempt counter
error_message TEXT          Last error (if any)
next_retry_at TIMESTAMPTZ   When to retry (if failed)
created_at    TIMESTAMPTZ   Creation time
updated_at    TIMESTAMPTZ   Auto-updated on changes
```

### Indexes

- `idx_notifications_retry` - Worker polling for pending notifications
- `idx_notifications_tenant` - Tenant-based listing
- `idx_notifications_user` - User-specific queries
- `idx_notifications_channel` - Analytics by channel
