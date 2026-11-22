// Copyright 2025 Mehmet ERGULCU
// SPDX-License-Identifier: Apache-2.0

// Package memory is a Store that lives in the process. Everything is lost on restart and it cannot be
// shared between instances, so it is meant for development and tests.
package memory

import (
	"context"
	"maps"
	"sort"
	"sync"
	"time"

	"github.com/ergulcu/schedu-cloud/internal/model"
	"github.com/ergulcu/schedu-cloud/internal/store"
)

// Store is safe for concurrent use.
type Store struct {
	mu       sync.Mutex
	jobs     map[model.Key]model.Job
	triggers map[model.Key]model.Trigger
	runs     []model.Run
	nextRun  int64
	now      func() time.Time
}

var _ store.Store = (*Store)(nil)

// New returns an empty store.
func New() *Store {
	return &Store{
		jobs:     map[model.Key]model.Job{},
		triggers: map[model.Key]model.Trigger{},
		now:      func() time.Time { return time.Now().UTC() },
	}
}

func (s *Store) CreateJob(_ context.Context, job *model.Job) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.jobs[job.Key]; ok {
		return model.AlreadyExistsf("job %s", job.Key)
	}
	now := s.now()
	job.CreatedAt, job.UpdatedAt = now, now
	s.jobs[job.Key] = cloneJob(*job)
	return nil
}

func (s *Store) UpdateJob(_ context.Context, job *model.Job) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	old, ok := s.jobs[job.Key]
	if !ok {
		return model.NotFoundf("job %s", job.Key)
	}
	job.CreatedAt, job.UpdatedAt = old.CreatedAt, s.now()
	s.jobs[job.Key] = cloneJob(*job)
	return nil
}

func (s *Store) GetJob(_ context.Context, key model.Key) (model.Job, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	job, ok := s.jobs[key]
	if !ok {
		return model.Job{}, model.NotFoundf("job %s", key)
	}
	return cloneJob(job), nil
}

func (s *Store) ListJobs(context.Context) ([]model.Job, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	jobs := make([]model.Job, 0, len(s.jobs))
	for _, job := range s.jobs {
		jobs = append(jobs, cloneJob(job))
	}
	sort.Slice(jobs, func(i, j int) bool { return less(jobs[i].Key, jobs[j].Key) })
	return jobs, nil
}

func (s *Store) DeleteJob(_ context.Context, key model.Key) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.jobs[key]; !ok {
		return false, nil
	}
	delete(s.jobs, key)
	for k, t := range s.triggers {
		if t.JobKey == key {
			delete(s.triggers, k)
		}
	}
	return true, nil
}

func (s *Store) CreateTrigger(_ context.Context, t *model.Trigger) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.jobs[t.JobKey]; !ok {
		return model.NotFoundf("job %s", t.JobKey)
	}
	if _, ok := s.triggers[t.Key]; ok {
		return model.AlreadyExistsf("trigger %s", t.Key)
	}
	now := s.now()
	t.CreatedAt, t.UpdatedAt = now, now
	s.triggers[t.Key] = cloneTrigger(*t)
	return nil
}

func (s *Store) ReplaceTrigger(_ context.Context, t *model.Trigger) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	old, ok := s.triggers[t.Key]
	if !ok {
		return model.NotFoundf("trigger %s", t.Key)
	}
	if _, ok := s.jobs[t.JobKey]; !ok {
		return model.NotFoundf("job %s", t.JobKey)
	}
	if old.State == model.StatePaused && t.State == model.StateNormal {
		t.State = model.StatePaused
	}
	t.CreatedAt, t.UpdatedAt = old.CreatedAt, s.now()
	s.triggers[t.Key] = cloneTrigger(*t)
	return nil
}

func (s *Store) GetTrigger(_ context.Context, key model.Key) (model.Trigger, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	t, ok := s.triggers[key]
	if !ok {
		return model.Trigger{}, model.NotFoundf("trigger %s", key)
	}
	return cloneTrigger(t), nil
}

func (s *Store) ListTriggers(context.Context) ([]model.Trigger, error) {
	return s.filterTriggers(func(model.Trigger) bool { return true }), nil
}

func (s *Store) ListTriggersOfJob(_ context.Context, jobKey model.Key) ([]model.Trigger, error) {
	return s.filterTriggers(func(t model.Trigger) bool { return t.JobKey == jobKey }), nil
}

func (s *Store) filterTriggers(keep func(model.Trigger) bool) []model.Trigger {
	s.mu.Lock()
	defer s.mu.Unlock()
	var list []model.Trigger
	for _, t := range s.triggers {
		if keep(t) {
			list = append(list, cloneTrigger(t))
		}
	}
	sort.Slice(list, func(i, j int) bool { return less(list[i].Key, list[j].Key) })
	return list
}

func (s *Store) DeleteTrigger(_ context.Context, key model.Key) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	t, ok := s.triggers[key]
	if !ok {
		return false, nil
	}
	delete(s.triggers, key)
	if job, ok := s.jobs[t.JobKey]; ok && !job.Durable {
		for _, other := range s.triggers {
			if other.JobKey == t.JobKey {
				return true, nil
			}
		}
		delete(s.jobs, t.JobKey)
	}
	return true, nil
}

func (s *Store) PauseTrigger(_ context.Context, key model.Key) error {
	return s.setState(key, func(state model.TriggerState) model.TriggerState {
		if state == model.StateNormal || state == model.StateError {
			return model.StatePaused
		}
		return state
	})
}

func (s *Store) ResumeTrigger(_ context.Context, key model.Key) error {
	return s.setState(key, func(state model.TriggerState) model.TriggerState {
		if state == model.StatePaused {
			return model.StateNormal
		}
		return state
	})
}

func (s *Store) setState(key model.Key, change func(model.TriggerState) model.TriggerState) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	t, ok := s.triggers[key]
	if !ok {
		return model.NotFoundf("trigger %s", key)
	}
	t.State = change(t.State)
	t.UpdatedAt = s.now()
	s.triggers[key] = t
	return nil
}

func (s *Store) AcquireDue(_ context.Context, now time.Time, limit int, advance store.AdvanceFunc) ([]model.Fire, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var due []model.Trigger
	for _, t := range s.triggers {
		if t.State == model.StateNormal && t.NextFireTime != nil && !t.NextFireTime.After(now) {
			due = append(due, t)
		}
	}
	sort.Slice(due, func(i, j int) bool {
		if !due[i].NextFireTime.Equal(*due[j].NextFireTime) {
			return due[i].NextFireTime.Before(*due[j].NextFireTime)
		}
		return due[i].Priority > due[j].Priority
	})
	if len(due) > limit {
		due = due[:limit]
	}
	var fires []model.Fire
	for _, t := range due {
		fire, scheduled := advance(&t, now)
		t.UpdatedAt = now
		s.triggers[t.Key] = t
		if fire {
			fires = append(fires, model.Fire{Trigger: cloneTrigger(t), Job: cloneJob(s.jobs[t.JobKey]),
				ScheduledTime: scheduled, FireTime: now})
		}
	}
	return fires, nil
}

func (s *Store) RecordRun(_ context.Context, run *model.Run) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.nextRun++
	run.ID = s.nextRun
	s.runs = append(s.runs, *run)
	return nil
}

func (s *Store) ListRuns(_ context.Context, jobKey *model.Key, limit int) ([]model.Run, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var runs []model.Run
	for i := len(s.runs) - 1; i >= 0 && len(runs) < limit; i-- {
		if jobKey == nil || s.runs[i].JobKey == *jobKey {
			runs = append(runs, s.runs[i])
		}
	}
	return runs, nil
}

func (s *Store) DeleteRunsBefore(_ context.Context, before time.Time) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	kept := s.runs[:0]
	var deleted int64
	for _, run := range s.runs {
		if run.FireTime.Before(before) {
			deleted++
			continue
		}
		kept = append(kept, run)
	}
	s.runs = kept
	return deleted, nil
}

func (s *Store) Ping(context.Context) error { return nil }

func (s *Store) Close() {}

func less(a, b model.Key) bool {
	if a.Group != b.Group {
		return a.Group < b.Group
	}
	return a.Name < b.Name
}

func cloneJob(j model.Job) model.Job {
	j.Data = maps.Clone(j.Data)
	return j
}

func cloneTrigger(t model.Trigger) model.Trigger {
	t.Data = maps.Clone(t.Data)
	if t.Cron != nil {
		c := *t.Cron
		t.Cron = &c
	}
	if t.Simple != nil {
		s := *t.Simple
		t.Simple = &s
	}
	t.EndTime = clonePtr(t.EndTime)
	t.NextFireTime = clonePtr(t.NextFireTime)
	t.PreviousFireTime = clonePtr(t.PreviousFireTime)
	return t
}

func clonePtr(p *time.Time) *time.Time {
	if p == nil {
		return nil
	}
	v := *p
	return &v
}
