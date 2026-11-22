// Copyright 2025 Mehmet ERGULCU
// SPDX-License-Identifier: Apache-2.0

// Command scheducloud is a clustered job scheduler with a REST API. Start as many instances as needed
// against the same PostgreSQL database; each due trigger runs on exactly one of them.
package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"
	_ "time/tzdata" // cron time zones work in minimal images too

	"github.com/ergulcu/schedu-cloud/internal/api"
	"github.com/ergulcu/schedu-cloud/internal/config"
	"github.com/ergulcu/schedu-cloud/internal/jobs"
	"github.com/ergulcu/schedu-cloud/internal/model"
	"github.com/ergulcu/schedu-cloud/internal/scheduler"
	"github.com/ergulcu/schedu-cloud/internal/store"
	"github.com/ergulcu/schedu-cloud/internal/store/memory"
	"github.com/ergulcu/schedu-cloud/internal/store/postgres"
)

func main() {
	if err := run(); err != nil {
		slog.Error("scheducloud stopped", "error", err)
		os.Exit(1)
	}
}

func run() error {
	cfg, err := config.FromEnv(os.LookupEnv)
	if err != nil {
		return err
	}
	log := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: cfg.LogLevel})).
		With("instance", cfg.InstanceID)
	slog.SetDefault(log)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	var st store.Store
	if cfg.Store == "memory" {
		log.Warn("using the memory store: nothing is kept across restarts and instances do not share work")
		st = memory.New()
	} else {
		if st, err = postgres.Open(ctx, cfg.DatabaseURL); err != nil {
			return err
		}
	}
	defer st.Close()

	kafka := jobs.KafkaEvent{}
	if cfg.KafkaBrokers != "" {
		producer, err := jobs.NewKafkaProducer(cfg.KafkaBrokers)
		if err != nil {
			return err
		}
		defer producer.Close()
		kafka.Producer = producer
	}
	executors := jobs.Registry{
		model.JobRestGet:    jobs.RestGet{Client: &http.Client{}, DefaultTimeout: cfg.RestTimeout},
		model.JobKafkaEvent: kafka,
	}

	sched := scheduler.New(st, executors, scheduler.Config{
		InstanceID:       cfg.InstanceID,
		PollInterval:     cfg.PollInterval,
		Workers:          cfg.Workers,
		MisfireThreshold: cfg.MisfireThreshold,
		RunRetention:     cfg.RunRetention,
	}, log)
	schedulerDone := make(chan struct{})
	go func() {
		sched.Run(ctx)
		close(schedulerDone)
	}()

	server := &http.Server{
		Addr:              cfg.HTTPAddr,
		Handler:           api.New(st, executors, cfg.APIToken, log).Handler(),
		ReadHeaderTimeout: 10 * time.Second,
		ReadTimeout:       30 * time.Second,
		WriteTimeout:      30 * time.Second,
	}
	serverErr := make(chan error, 1)
	go func() {
		log.Info("HTTP API listening", "addr", cfg.HTTPAddr, "auth", cfg.APIToken != "")
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			serverErr <- err
		}
		close(serverErr)
	}()

	select {
	case <-ctx.Done():
	case err := <-serverErr:
		stop()
		if err != nil {
			return err
		}
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
	defer cancel()
	if err := server.Shutdown(shutdownCtx); err != nil {
		log.Warn("HTTP shutdown", "error", err)
	}
	select {
	case <-schedulerDone:
	case <-shutdownCtx.Done():
		log.Warn("running jobs did not finish in time")
	}
	return nil
}
