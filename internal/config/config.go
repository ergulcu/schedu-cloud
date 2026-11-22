// Copyright 2025 Mehmet ERGULCU
// SPDX-License-Identifier: Apache-2.0

// Package config reads the settings from environment variables.
package config

import (
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"time"
)

// Config holds every setting. See the README for the variable names.
type Config struct {
	HTTPAddr         string
	Store            string // "postgres" or "memory"
	DatabaseURL      string
	InstanceID       string
	PollInterval     time.Duration
	Workers          int
	MisfireThreshold time.Duration
	RunRetention     time.Duration
	RestTimeout      time.Duration
	KafkaBrokers     string
	APIToken         string
	LogLevel         slog.Level
	ShutdownTimeout  time.Duration
}

// FromEnv reads the configuration; lookup is os.LookupEnv outside tests.
func FromEnv(lookup func(string) (string, bool)) (Config, error) {
	get := func(name, fallback string) string {
		if v, ok := lookup(name); ok && strings.TrimSpace(v) != "" {
			return strings.TrimSpace(v)
		}
		return fallback
	}
	host, _ := os.Hostname()
	c := Config{
		HTTPAddr:     get("HTTP_ADDR", ":8080"),
		Store:        strings.ToLower(get("STORE", "postgres")),
		DatabaseURL:  get("DATABASE_URL", ""),
		InstanceID:   get("INSTANCE_ID", fmt.Sprintf("%s-%d", host, os.Getpid())),
		KafkaBrokers: get("KAFKA_BROKERS", ""),
		APIToken:     get("API_TOKEN", ""),
	}
	var errs []string
	duration := func(name, fallback string) time.Duration {
		d, err := time.ParseDuration(get(name, fallback))
		if err != nil || d < 0 {
			errs = append(errs, fmt.Sprintf("%s must be a duration such as 30s", name))
		}
		return d
	}
	c.PollInterval = duration("POLL_INTERVAL", "1s")
	c.MisfireThreshold = duration("MISFIRE_THRESHOLD", "60s")
	c.RunRetention = duration("RUN_RETENTION", "168h")
	c.RestTimeout = duration("REST_TIMEOUT", "30s")
	c.ShutdownTimeout = duration("SHUTDOWN_TIMEOUT", "30s")

	workers, err := strconv.Atoi(get("WORKERS", "10"))
	if err != nil || workers < 1 {
		errs = append(errs, "WORKERS must be a positive number")
	}
	c.Workers = workers

	if err := c.LogLevel.UnmarshalText([]byte(get("LOG_LEVEL", "info"))); err != nil {
		errs = append(errs, "LOG_LEVEL must be debug, info, warn or error")
	}
	switch c.Store {
	case "postgres":
		if c.DatabaseURL == "" {
			errs = append(errs, "DATABASE_URL is required (or set STORE=memory for a quick try)")
		}
	case "memory":
	default:
		errs = append(errs, "STORE must be postgres or memory")
	}
	if c.PollInterval > 0 && c.PollInterval < 100*time.Millisecond {
		errs = append(errs, "POLL_INTERVAL must be at least 100ms")
	}
	if len(errs) > 0 {
		return c, fmt.Errorf("configuration: %s", strings.Join(errs, "; "))
	}
	return c, nil
}
