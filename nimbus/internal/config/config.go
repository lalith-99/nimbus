package config

import (
	"fmt"
	"os"
	"strconv"
)

type Config struct {
	Port     int
	LogLevel string
	Env      string

	// Database
	DBHost     string
	DBPort     int
	DBUser     string
	DBPassword string
	DBName     string
	DBSSLMode  string
}

// Load reads configuration from environment variables with sensible defaults
func Load() (*Config, error) {
	cfg := &Config{
		Port:     8080,
		LogLevel: "info",
		Env:      "development",

		// Local postgres defaults
		DBHost:     "localhost",
		DBPort:     5432,
		DBUser:     "postgres",
		DBPassword: "postgres",
		DBName:     "nimbus",
		DBSSLMode:  "disable",
	}

	if port := os.Getenv("PORT"); port != "" {
		p, err := strconv.Atoi(port)
		if err != nil {
			return nil, fmt.Errorf("invalid PORT: %w", err)
		}
		cfg.Port = p
	}

	if level := os.Getenv("LOG_LEVEL"); level != "" {
		cfg.LogLevel = level
	}

	if env := os.Getenv("ENV"); env != "" {
		cfg.Env = env
	}

	// Database config
	if host := os.Getenv("DB_HOST"); host != "" {
		cfg.DBHost = host
	}

	if port := os.Getenv("DB_PORT"); port != "" {
		p, err := strconv.Atoi(port)
		if err != nil {
			return nil, fmt.Errorf("invalid DB_PORT: %w", err)
		}
		cfg.DBPort = p
	}

	if user := os.Getenv("DB_USER"); user != "" {
		cfg.DBUser = user
	}

	if password := os.Getenv("DB_PASSWORD"); password != "" {
		cfg.DBPassword = password
	}

	if dbname := os.Getenv("DB_NAME"); dbname != "" {
		cfg.DBName = dbname
	}

	if sslmode := os.Getenv("DB_SSLMODE"); sslmode != "" {
		cfg.DBSSLMode = sslmode
	}

	return cfg, nil
}
