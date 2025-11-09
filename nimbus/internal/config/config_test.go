package config

import (
	"os"
	"testing"
)

func TestLoad_Defaults(t *testing.T) {
	// Clear any existing env vars
	os.Unsetenv("PORT")
	os.Unsetenv("LOG_LEVEL")
	os.Unsetenv("ENV")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}

	if cfg.Port != 8080 {
		t.Errorf("expected port 8080, got %d", cfg.Port)
	}

	if cfg.LogLevel != "info" {
		t.Errorf("expected log level 'info', got %s", cfg.LogLevel)
	}

	if cfg.Env != "development" {
		t.Errorf("expected env 'development', got %s", cfg.Env)
	}
}

func TestLoad_CustomValues(t *testing.T) {
	os.Setenv("PORT", "9000")
	os.Setenv("LOG_LEVEL", "debug")
	os.Setenv("ENV", "production")
	defer func() {
		os.Unsetenv("PORT")
		os.Unsetenv("LOG_LEVEL")
		os.Unsetenv("ENV")
	}()

	cfg, err := Load()
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}

	if cfg.Port != 9000 {
		t.Errorf("expected port 9000, got %d", cfg.Port)
	}

	if cfg.LogLevel != "debug" {
		t.Errorf("expected log level 'debug', got %s", cfg.LogLevel)
	}

	if cfg.Env != "production" {
		t.Errorf("expected env 'production', got %s", cfg.Env)
	}
}
