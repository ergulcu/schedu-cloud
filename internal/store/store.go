// Copyright 2025 Mehmet ERGULCU
// SPDX-License-Identifier: Apache-2.0

// Package store defines where jobs, triggers and runs are kept. The PostgreSQL store is the one to use
// with several instances; the memory store is for development and tests.
package store

import (
	"context"
	"time"

	"github.com/ergulcu/schedu-cloud/internal/model"
)

// AdvanceFunc decides whether a due trigger fires now and moves it to its next fire time.
// It is schedule.Advance with the misfire threshold bound.
type AdvanceFunc func(t *model.Trigger, now time.Time) (fire bool, scheduled time.Time)

// Store keeps the scheduler's state. Methods return errors wrapping model.ErrNotFound,
// model.ErrAlreadyExists or model.ErrInvalid where those apply.
type Store interface {
	CreateJob(ctx context.Context, job *model.Job) error
	UpdateJob(ctx context.Context, job *model.Job) error
	GetJob(ctx context.Context, key model.Key) (model.Job, error)
	ListJobs(ctx context.Context) ([]model.Job, error)
	// DeleteJob removes the job and all its triggers. It reports whether the job existed.
	DeleteJob(ctx context.Context, key model.Key) (bool, error)

	// CreateTrigger stores a prepared trigger; its job must exist.
	CreateTrigger(ctx context.Context, t *model.Trigger) error
	// ReplaceTrigger swaps an existing trigger for a newly prepared one with the same key.
	ReplaceTrigger(ctx context.Context, t *model.Trigger) error
	GetTrigger(ctx context.Context, key model.Key) (model.Trigger, error)
	ListTriggers(ctx context.Context) ([]model.Trigger, error)
	ListTriggersOfJob(ctx context.Context, jobKey model.Key) ([]model.Trigger, error)
	// DeleteTrigger removes the trigger, and its job too when the job is not durable and has no other
	// trigger left. It reports whether the trigger existed.
	DeleteTrigger(ctx context.Context, key model.Key) (bool, error)
	// PauseTrigger stops a trigger from firing; ResumeTrigger lets it fire again. Completed triggers stay
	// completed.
	PauseTrigger(ctx context.Context, key model.Key) error
	ResumeTrigger(ctx context.Context, key model.Key) error

	// AcquireDue claims up to limit triggers whose next fire time is not after now, moves each one forward
	// with advance and returns those that must run. With several instances on one database every due fire
	// is returned to exactly one of them.
	AcquireDue(ctx context.Context, now time.Time, limit int, advance AdvanceFunc) ([]model.Fire, error)

	RecordRun(ctx context.Context, run *model.Run) error
	// ListRuns returns the newest runs first, of one job or (with a nil key) of all jobs.
	ListRuns(ctx context.Context, jobKey *model.Key, limit int) ([]model.Run, error)
	DeleteRunsBefore(ctx context.Context, before time.Time) (int64, error)

	Ping(ctx context.Context) error
	Close()
}
