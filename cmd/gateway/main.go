package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"go.uber.org/zap"
	googlegrpc "google.golang.org/grpc"

	"github.com/lalithlochan/nimbus/internal/ai"
	"github.com/lalithlochan/nimbus/internal/api"
	"github.com/lalithlochan/nimbus/internal/circuitbreaker"
	"github.com/lalithlochan/nimbus/internal/config"
	"github.com/lalithlochan/nimbus/internal/db"
	internalgrpc "github.com/lalithlochan/nimbus/internal/grpc"
	"github.com/lalithlochan/nimbus/internal/metrics"
	"github.com/lalithlochan/nimbus/internal/observ"
	"github.com/lalithlochan/nimbus/internal/rag"
	"github.com/lalithlochan/nimbus/internal/redis"
	"github.com/lalithlochan/nimbus/internal/sqs"
	"github.com/lalithlochan/nimbus/internal/worker"
	notificationv1 "github.com/lalithlochan/nimbus/proto/notification/v1"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	// Load config
	cfg, err := config.Load()
	if err != nil {
		return fmt.Errorf("failed to load config: %w", err)
	}

	// Setup logger
	logger, err := observ.NewLogger(cfg.Env, cfg.LogLevel)
	if err != nil {
		return fmt.Errorf("failed to create logger: %w", err)
	}
	defer func() { _ = logger.Sync() }()

	logger.Info("starting nimbus gateway",
		zap.String("env", cfg.Env),
		zap.Int("port", cfg.Port),
		zap.String("version", "v1.0.1"),
		zap.String("deployment_test-2026-01-28", "true"),
	)

	// Initialize database connection
	ctx := context.Background()
	dbConfig := db.Config{
		Host:     cfg.DBHost,
		Port:     cfg.DBPort,
		User:     cfg.DBUser,
		Password: cfg.DBPassword,
		Database: cfg.DBName,
		SSLMode:  cfg.DBSSLMode,
	}

	database, err := db.New(ctx, dbConfig, logger)
	if err != nil {
		return fmt.Errorf("failed to connect to database: %w", err)
	}
	defer database.Close()

	logger.Info("database connection established",
		zap.String("host", cfg.DBHost),
		zap.Int("port", cfg.DBPort),
		zap.String("database", cfg.DBName),
	)

	// Initialize repository
	repo := db.NewRepository(database, logger)

	// Initialize Redis for idempotency and rate limiting
	redisConfig := redis.Config{
		Host:     cfg.RedisHost,
		Port:     cfg.RedisPort,
		Password: cfg.RedisPassword,
		DB:       cfg.RedisDB,
	}

	redisClient, err := redis.New(ctx, redisConfig, logger)
	if err != nil {
		logger.Warn("redis unavailable, idempotency disabled",
			zap.Error(err),
			zap.String("host", cfg.RedisHost),
		)
	}

	var idempotencyService *redis.IdempotencyService
	var rateLimiter *redis.RateLimiter
	if redisClient != nil {
		idempotencyService = redis.NewIdempotencyService(redisClient, logger)
		rateLimiter = redis.NewRateLimiter(redisClient, logger, redis.RateLimitConfig{
			Limit:  100,             // 100 requests
			Window: 1 * time.Minute, // per minute per tenant
		})
		defer redisClient.Close()
	}

	// Initialize SQS producer
	var producer *sqs.Producer
	if cfg.SQSQueueURL != "" {
		sqsCfg := sqs.Config{
			Region:   cfg.SQSRegion,
			QueueURL: cfg.SQSQueueURL,
			DLQURL:   cfg.SQSDLQURL,
		}
		producer, err = sqs.NewProducer(ctx, sqsCfg, logger)
		if err != nil {
			logger.Warn("sqs producer unavailable, events will not be enqueued",
				zap.Error(err),
			)
		}
		// Guard the deferred Close: if NewProducer failed, producer is nil and
		// calling Close() on it would panic during shutdown.
		if producer != nil {
			defer producer.Close()
		}
	}

	sesCfg := worker.SESConfig{
		Region:    cfg.AWSRegion,
		FromEmail: cfg.SESFromEmail,
	}

	sender, err := worker.NewSESSender(ctx, sesCfg, logger)
	if err != nil {
		return fmt.Errorf("failed to create SES email sender: %w", err)
	}

	// Initialize SNS sender for SMS
	snsSender, err := worker.NewSNSSender(ctx, worker.SNSConfig{
		Region: cfg.SNSRegion,
	}, logger)
	if err != nil {
		logger.Warn("SNS sender unavailable, SMS notifications disabled",
			zap.Error(err),
		)
		snsSender = nil
	}

	// Initialize webhook sender
	webhookSender := worker.NewWebhookSender(logger, worker.WebhookConfig{
		DefaultTimeout: time.Duration(cfg.WebhookTimeout) * time.Second,
	})

	// Wrap each sender with a circuit breaker for resilience.
	// When a downstream service (SES/SNS/webhook) starts failing,
	// the circuit opens and fails fast instead of hammering a dead service.
	sesBreaker := circuitbreaker.New(circuitbreaker.Config{
		Name:            "ses-email",
		MaxFailures:     5,
		RecoveryTimeout: 30 * time.Second,
	}, logger)
	protectedEmail := circuitbreaker.NewProtectedSender(sender, sesBreaker, logger)

	var protectedSNS circuitbreaker.Sender
	var snsBreaker *circuitbreaker.CircuitBreaker
	if snsSender != nil {
		snsBreaker = circuitbreaker.New(circuitbreaker.Config{
			Name:            "sns-sms",
			MaxFailures:     5,
			RecoveryTimeout: 30 * time.Second,
		}, logger)
		protectedSNS = circuitbreaker.NewProtectedSender(snsSender, snsBreaker, logger)
	}

	webhookBreaker := circuitbreaker.New(circuitbreaker.Config{
		Name:            "webhook",
		MaxFailures:     5,
		RecoveryTimeout: 30 * time.Second,
	}, logger)
	protectedWebhook := circuitbreaker.NewProtectedSender(webhookSender, webhookBreaker, logger)

	// Create multi-sender that routes to appropriate channel handler
	var multiSender worker.Sender
	if protectedSNS != nil {
		multiSender = worker.NewMultiSender(logger, protectedEmail, protectedSNS, protectedWebhook)
	} else {
		multiSender = worker.NewMultiSender(logger, protectedEmail, protectedWebhook)
	}

	logger.Info("initialized multi-channel notification system",
		zap.Bool("email_enabled", true),
		zap.Bool("sms_enabled", snsSender != nil),
		zap.Bool("webhook_enabled", true),
	)

	// Initialize AI client (optional — only if OPENAI_API_KEY is set)
	var aiClient *ai.Client
	var aiHandler *ai.Handler
	if cfg.AIEnabled {
		var aiErr error
		aiClient, aiErr = ai.NewClient(ai.Config{
			APIKey: cfg.OpenAIAPIKey,
			Model:  cfg.OpenAIModel,
		}, logger)
		if aiErr != nil {
			logger.Warn("AI features disabled", zap.Error(aiErr))
		} else {
			composeService := ai.NewComposeService(aiClient, repo, logger)
			aiHandler = ai.NewHandler(composeService, logger)

			// Wrap the multi-sender with AI enrichment so template-based
			// notifications get AI-generated content before sending.
			multiSender = ai.NewEnrichmentSender(multiSender, aiClient, logger)

			logger.Info("AI features enabled",
				zap.String("model", cfg.OpenAIModel),
			)
		}
	}

	// Poll interval for the DB worker. When SQS is configured the worker becomes a
	// slow RECONCILIATION BACKSTOP (SQS is the fast hot path), so we poll less
	// aggressively to reduce DB load. Without SQS it stays the primary delivery
	// path and polls frequently.
	pollInterval := 5 * time.Second
	if cfg.SQSQueueURL != "" {
		pollInterval = 30 * time.Second
	}

	w := worker.New(repo, multiSender, worker.Config{
		PollInterval: pollInterval,
		BatchSize:    10,
		MaxRetries:   5,
	}, logger)

	workerCtx, workerCancel := context.WithCancel(context.Background())
	defer workerCancel()

	go w.Start(workerCtx)

	logger.Info("background worker started",
		zap.Duration("poll_interval", pollInterval),
		zap.Bool("sqs_hot_path", cfg.SQSQueueURL != ""),
	)

	// ── SQS Consumer (event-driven hot path) ─────────────────────────────────
	// When a queue is configured, the SQS consumer is the PRIMARY, low-latency
	// delivery path: it long-polls, atomically claims each notification in the DB
	// (the same no-double-send guard the poller uses), and dispatches via the SAME
	// Dispatcher. The DB poller above remains as a reconciliation backstop that
	// catches delayed retries and anything SQS ever drops.
	if cfg.SQSQueueURL != "" {
		sqsConsumerTransport, cErr := sqs.NewConsumer(ctx, sqs.Config{
			Region:   cfg.SQSRegion,
			QueueURL: cfg.SQSQueueURL,
			DLQURL:   cfg.SQSDLQURL,
		}, logger)
		if cErr != nil {
			logger.Warn("sqs consumer unavailable; relying on DB-poll delivery",
				zap.Error(cErr),
			)
		} else {
			dispatcher := worker.NewDispatcher(repo, multiSender, 5, logger)
			sqsConsumer := worker.NewSQSConsumer(
				sqsConsumerTransport,
				repo,
				dispatcher,
				worker.SQSConsumerConfig{
					Receivers:   3,
					Concurrency: 10,
				},
				logger,
			)
			go sqsConsumer.Start(workerCtx)
			logger.Info("sqs consumer started (event-driven hot path)")
		}
	}

	// ── gRPC Server ──────────────────────────────────────────────────────────
	// We start gRPC on a separate port (9090) alongside HTTP (8080).
	//
	// Why two ports instead of one?
	// gRPC uses HTTP/2 with binary framing. Go's net/http mux doesn't natively
	// multiplex HTTP/1.1 REST and gRPC on the same listener without a CONNECT
	// tunnel (grpc-gateway). Separate ports = simpler, zero ambiguity.
	//
	// Internal services call :9090 (gRPC) for efficiency + server-streaming.
	// External clients / browsers call :8080 (REST/JSON).
	grpcServer := googlegrpc.NewServer(
		// Chain multiple interceptors using grpc.ChainUnaryInterceptor.
		// Order matters: auth runs first, then any future tracing/logging interceptors.
		googlegrpc.ChainUnaryInterceptor(
			internalgrpc.AuthInterceptor(cfg.GRPCAuthTokens, logger),
		),
		googlegrpc.ChainStreamInterceptor(
			internalgrpc.StreamAuthInterceptor(cfg.GRPCAuthTokens, logger),
		),
	)
	notificationv1.RegisterNotificationServiceServer(grpcServer, internalgrpc.NewServer(repo, logger))

	grpcListener, err := net.Listen("tcp", fmt.Sprintf(":%d", cfg.GRPCPort))
	if err != nil {
		return fmt.Errorf("failed to create gRPC listener: %w", err)
	}

	go func() {
		logger.Info("gRPC server listening",
			zap.Int("port", cfg.GRPCPort),
		)
		if err := grpcServer.Serve(grpcListener); err != nil {
			logger.Error("gRPC server stopped", zap.Error(err))
		}
	}()

	// ── RAG Pipeline (pgvector + hybrid retrieval) ────────────────────────────
	// Only initialize if OpenAI is enabled (same key used for both compose + RAG).
	var ragHandler *rag.Handler
	if cfg.AIEnabled && database != nil {
		embedder := rag.NewEmbedder(cfg.OpenAIAPIKey)
		store := rag.NewStore(database.Pool(), logger)
		reranker := rag.NewReranker()
		guard := rag.NewGuard()
		ragPipeline := rag.NewPipeline(embedder, store, reranker, guard, aiClient, logger)
		ragHandler = rag.NewHandler(ragPipeline, logger)
		logger.Info("RAG pipeline enabled (pgvector + hybrid search)")
	}

	// Setup router
	r := chi.NewRouter()

	// Middleware
	r.Use(middleware.RequestID)
	r.Use(middleware.RealIP)
	r.Use(middleware.Recoverer)
	r.Use(middleware.Timeout(30 * time.Second))
	r.Use(metrics.Middleware)

	// Custom logging middleware
	r.Use(func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			start := time.Now()
			ww := middleware.NewWrapResponseWriter(w, r.ProtoMajor)

			next.ServeHTTP(ww, r)

			logger.Info("request completed",
				zap.String("method", r.Method),
				zap.String("path", r.URL.Path),
				zap.Int("status", ww.Status()),
				zap.Duration("duration_ms", time.Since(start)),
				zap.String("request_id", middleware.GetReqID(r.Context())),
			)
		})
	})

	// API routes
	var handler *api.Handler
	if idempotencyService != nil && producer != nil {
		handler = api.NewHandlerWithSQS(logger, repo, idempotencyService, producer)
	} else if idempotencyService != nil {
		handler = api.NewHandlerWithIdempotency(logger, repo, idempotencyService)
	} else {
		handler = api.NewHandler(logger, repo)
	}
	r.Route("/v1", func(r chi.Router) {
		// Apply rate limiting to API routes
		r.Use(api.RateLimitMiddleware(rateLimiter, logger, api.TenantKeyFunc))

		r.Post("/notifications", handler.CreateNotification)
		r.Get("/notifications", handler.ListNotifications)
		r.Get("/notifications/{id}", handler.GetNotification)
		r.Patch("/notifications/{id}/status", handler.UpdateNotificationStatus)

		// Dead Letter Queue routes
		r.Get("/dlq", handler.ListDeadLetterQueue)
		r.Get("/dlq/{id}", handler.GetDeadLetterItem)
		r.Post("/dlq/{id}/retry", handler.RetryDeadLetterItem)
		r.Post("/dlq/{id}/discard", handler.DiscardDeadLetterItem)

		// AI-powered compose endpoint (only if AI is enabled)
		if aiHandler != nil {
			r.Post("/ai/compose", aiHandler.HandleCompose)
		}

		// RAG-powered ask endpoint: hybrid retrieval + cited answers
		// POST /v1/ai/ask {"query": "...", "tenant_id": "..."}
		if ragHandler != nil {
			r.Post("/ai/ask", ragHandler.HandleAsk)
		}
	})

	// Health check
	r.Get("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("OK"))
	})

	// Circuit breaker status endpoint — shows real-time health of all downstream services
	breakers := []*circuitbreaker.CircuitBreaker{sesBreaker, webhookBreaker}
	if snsBreaker != nil {
		breakers = append(breakers, snsBreaker)
	}
	r.Get("/v1/health/circuits", func(w http.ResponseWriter, r *http.Request) {
		stats := make([]circuitbreaker.Stats, 0, len(breakers))
		for _, b := range breakers {
			stats = append(stats, b.Stats())
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"circuit_breakers": stats,
		})
	})

	// Admin endpoint to reset a circuit breaker
	r.Post("/v1/admin/circuits/{name}/reset", func(w http.ResponseWriter, r *http.Request) {
		name := chi.URLParam(r, "name")
		for _, b := range breakers {
			if b.Stats().Name == name {
				b.Reset()
				w.Header().Set("Content-Type", "application/json")
				_ = json.NewEncoder(w).Encode(map[string]string{
					"status":  "reset",
					"breaker": name,
				})
				return
			}
		}
		w.WriteHeader(http.StatusNotFound)
		_ = json.NewEncoder(w).Encode(map[string]string{
			"error": "circuit breaker not found: " + name,
		})
	})

	// Prometheus metrics endpoint
	r.Handle("/metrics", metrics.Handler())

	// Setup HTTP server
	srv := &http.Server{
		Addr:         fmt.Sprintf(":%d", cfg.Port),
		Handler:      r,
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 15 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	// Start server in a goroutine
	serverErrors := make(chan error, 1)
	go func() {
		logger.Info("server listening", zap.String("addr", srv.Addr))
		serverErrors <- srv.ListenAndServe()
	}()

	// Listen for shutdown signals
	shutdown := make(chan os.Signal, 1)
	signal.Notify(shutdown, os.Interrupt, syscall.SIGTERM)

	// Block until we receive a signal or server error
	select {
	case err := <-serverErrors:
		return fmt.Errorf("server error: %w", err)
	case sig := <-shutdown:
		logger.Info("shutdown signal received", zap.String("signal", sig.String()))

		// Gracefully stop the gRPC server first (drains in-flight RPCs).
		// GracefulStop waits for all active RPCs to complete before closing.
		// If we have a long-running StreamDeliveryUpdates stream, this gives
		// it time to finish rather than abruptly cutting the connection.
		grpcServer.GracefulStop()
		logger.Info("gRPC server stopped gracefully")

		// Give outstanding HTTP requests 10 seconds to complete
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		if err := srv.Shutdown(ctx); err != nil {
			srv.Close()
			return fmt.Errorf("graceful shutdown failed: %w", err)
		}

		logger.Info("server stopped gracefully")
	}

	return nil
}
