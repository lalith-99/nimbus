# Day 1: Project Bootstrap ✅

**Date**: Nov 8, 2025  
**Focus**: Foundation & basic HTTP gateway

## What I Built

1. **Project Structure**
   - Go module initialization
   - Clean directory layout following Go best practices
   - Makefile for common tasks

2. **Gateway Service**
   - HTTP server using chi router (lightweight, idiomatic)
   - Graceful shutdown handling (SIGTERM/SIGINT)
   - Structured JSON logging with zap
   - Request ID tracking & timing middleware
   - Health check endpoint

3. **Core Packages**
   - `internal/config`: Environment-based configuration
   - `internal/observ`: Logger initialization
   - `internal/api`: HTTP handlers with problem+json error format

## Endpoints

| Method | Path                    | Status | Description                  |
|--------|-------------------------|--------|------------------------------|
| POST   | `/v1/notifications`     | ✅     | Create notification (stub)   |
| GET    | `/health`               | ✅     | Health check                 |

## Technical Decisions

- **Chi over Gin**: More idiomatic, smaller footprint, stdlib-aligned
- **Zap for logging**: Production-grade, zero-alloc structured logging
- **Graceful shutdown**: 10s drain period for in-flight requests
- **Middleware pattern**: Composable, testable request pipeline

## Testing

- Unit tests for config package (defaults & env vars)
- All tests passing ✅

## What's Next (Day 2)

- [ ] Database schema (Postgres)
- [ ] Migrations setup (golang-migrate)
- [ ] Create notifications table
- [ ] Basic repository pattern for DB access

## Running It

```bash
# Install deps
make deps

# Run server
make run-gateway

# Test endpoint
curl -X POST http://localhost:8080/v1/notifications \
  -H "Content-Type: application/json" \
  -d '{"tenant_id":"t1","user_id":"u1","channel":"email","payload":{}}'

# Run tests
make test
```

## Key Learnings

- Chi's middleware composition is elegant
- Graceful shutdown is crucial for container orchestrators (ECS will use SIGTERM)
- Starting with structured logging from day 1 pays off
- Small, focused commits > large monoliths

---

**Lines of Code**: ~250  
**Time Spent**: ~2-3 hours  
**Commits**: Ready for initial commit
