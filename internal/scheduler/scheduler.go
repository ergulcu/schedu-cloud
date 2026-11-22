// Copyright 2025 Mehmet ERGULCU
// SPDX-License-Identifier: Apache-2.0

// Package scheduler polls the store for due triggers and runs their jobs.
package scheduler

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/ergulcu/schedu-cloud/internal/jobs"
	"github.com/ergulcu/schedu-cloud/internal/model"
	"github.com/ergulcu/schedu-cloud/internal/schedule"
	"github.com/ergulcu/schedu-cloud/internal/store"
)

// Config tunes the scheduler.
type Config struct {
	// InstanceID is written into every run, so one can see which instance ran it.
	InstanceID string
	// PollInterval is how often the store is asked for due triggers.
	PollInterval time.Duration
	// Workers is how many jobs may run at the same time on this instance.
	Workers int
	// MisfireThreshold is how late a trigger may be before its misfire policy applies.
	MisfireThreshold time.Duration
	// RunRetention is how long run history is kept; 0 keeps it forever.
	RunRetention time.Duration
}

// Scheduler runs jobs when their triggers are due.
type Scheduler struct {
	store     store.Store
	executors jobs.Registry
	cfg       Config
	log       *slog.Logger
	now       func() time.Time

	slots   chan struct{}
	running sync.WaitGroup
	lastGC  time.Time
}

// New returns a scheduler; call Run to start it.
func New(s store.Store, executors jobs.Registry, cfg Config, log *slog.Logger) *Scheduler {
	if cfg.Workers <= 0 {
		cfg.Workers = 10
	}
	if cfg.PollInterval <= 0 {
		cfg.PollInterval = time.Second
	}
	if cfg.MisfireThreshold <= 0 {
		cfg.MisfireThreshold = time.Minute
	}
	return &Scheduler{
		store:     s,
		executors: executors,
		cfg:       cfg,
		log:       log,
		now:       func() time.Time { return time.Now().UTC() },
		slots:     make(chan struct{}, cfg.Workers),
	}
}

// Run polls until the context ends, then waits for the running jobs to finish.
func (s *Scheduler) Run(ctx context.Context) {
	s.log.Info("scheduler started", "instance", s.cfg.InstanceID, "workers", s.cfg.Workers,
		"poll", s.cfg.PollInterval.String())
	ticker := time.NewTicker(s.cfg.PollInterval)
	defer ticker.Stop()
	for {
		if _, err := s.Tick(ctx); err != nil && ctx.Err() == nil {
			s.log.Error("poll failed", "error", err)
		}
		select {
		case <-ctx.Done():
			s.log.Info("scheduler stopping, waiting for running jobs")
			s.running.Wait()
			return
		case <-ticker.C:
		}
	}
}

// Tick claims the due triggers this instance has room for and starts their jobs. It returns how many it started.
func (s *Scheduler) Tick(ctx context.Context) (int, error) {
	s.collectGarbage(ctx)
	free := cap(s.slots) - len(s.slots)
	if free == 0 {
		return 0, nil
	}
	advance := func(t *model.Trigger, now time.Time) (bool, time.Time) {
		return schedule.Advance(t, now, s.cfg.MisfireThreshold)
	}
	fires, err := s.store.AcquireDue(ctx, s.now(), free, advance)
	if err != nil {
		return 0, err
	}
	for _, fire := range fires {
		s.slots <- struct{}{}
		s.running.Add(1)
		go func() {
			defer func() {
				<-s.slots
				s.running.Done()
			}()
			s.execute(context.WithoutCancel(ctx), fire)
		}()
	}
	return len(fires), nil
}

// Wait blocks until the jobs started so far have finished. Tests use it.
func (s *Scheduler) Wait() { s.running.Wait() }

func (s *Scheduler) execute(ctx context.Context, fire model.Fire) {
	log := s.log.With("job", fire.Job.Key.String(), "trigger", fire.Trigger.Key.String(),
		"scheduled", fire.ScheduledTime.Format(time.RFC3339))
	run := &model.Run{JobKey: fire.Job.Key, TriggerKey: fire.Trigger.Key, InstanceID: s.cfg.InstanceID,
		ScheduledTime: fire.ScheduledTime, FireTime: fire.FireTime}

	executor, ok := s.executors[fire.Job.Type]
	var err error
	if !ok {
		run.Message = "no executor for job type " + string(fire.Job.Type)
	} else {
		log.Info("job started")
		run.Message, err = s.safeExecute(ctx, executor, fire.MergedData())
	}
	run.FinishedAt = s.now()
	run.Succeeded = ok && err == nil
	if err != nil {
		if run.Message == "" {
			run.Message = err.Error()
		} else {
			run.Message = err.Error() + " (" + run.Message + ")"
		}
	}
	if run.Succeeded {
		log.Info("job finished", "took", run.FinishedAt.Sub(run.FireTime).String(), "result", run.Message)
	} else {
		log.Warn("job failed", "took", run.FinishedAt.Sub(run.FireTime).String(), "error", run.Message)
	}
	if err := s.store.RecordRun(ctx, run); err != nil {
		log.Error("could not record the run", "error", err)
	}
}

// safeExecute turns a panic in a job into a failed run instead of a crashed instance.
func (s *Scheduler) safeExecute(ctx context.Context, executor jobs.Executor, data map[string]any) (message string, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = panicError{value: r}
		}
	}()
	return executor.Execute(ctx, data)
}

type panicError struct{ value any }

func (p panicError) Error() string { return "job panicked: " + slog.AnyValue(p.value).String() }

func (s *Scheduler) collectGarbage(ctx context.Context) {
	if s.cfg.RunRetention <= 0 || s.now().Sub(s.lastGC) < time.Hour {
		return
	}
	s.lastGC = s.now()
	deleted, err := s.store.DeleteRunsBefore(ctx, s.now().Add(-s.cfg.RunRetention))
	if err != nil {
		s.log.Warn("could not delete old runs", "error", err)
	} else if deleted > 0 {
		s.log.Info("deleted old runs", "count", deleted)
	}
}
