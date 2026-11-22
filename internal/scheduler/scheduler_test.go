// Copyright 2025 Mehmet ERGULCU
// SPDX-License-Identifier: Apache-2.0

package scheduler

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ergulcu/schedu-cloud/internal/jobs"
	"github.com/ergulcu/schedu-cloud/internal/model"
	"github.com/ergulcu/schedu-cloud/internal/schedule"
	"github.com/ergulcu/schedu-cloud/internal/store/memory"
)

var start = time.Date(2026, 3, 10, 10, 0, 0, 0, time.UTC)

type recorder struct {
	mu    sync.Mutex
	calls []map[string]any
	err   error
	block chan struct{}
	panic bool
}

func (r *recorder) Validate(map[string]any) error { return nil }

func (r *recorder) Execute(_ context.Context, data map[string]any) (string, error) {
	if r.block != nil {
		<-r.block
	}
	if r.panic {
		panic("boom")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.calls = append(r.calls, data)
	return "done", r.err
}

func (r *recorder) count() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.calls)
}

type clock struct{ now atomic.Int64 }

func (c *clock) get() time.Time      { return time.Unix(0, c.now.Load()).UTC() }
func (c *clock) set(t time.Time)     { c.now.Store(t.UnixNano()) }
func (c *clock) add(d time.Duration) { c.set(c.get().Add(d)) }

func setup(t *testing.T, rec *recorder, workers int, repeat int) (*Scheduler, *memory.Store, *clock) {
	t.Helper()
	ctx := context.Background()
	st := memory.New()
	job := &model.Job{Key: model.NewKey("ping", ""), Durable: true, Type: model.JobRestGet,
		Data: map[string]any{"target": "http://job", "shared": "job"}}
	if err := st.CreateJob(ctx, job); err != nil {
		t.Fatal(err)
	}
	tr := &model.Trigger{Key: model.NewKey("every-10s", ""), JobKey: job.Key, Type: model.TriggerSimple,
		StartTime: start, Simple: &model.SimpleSchedule{RepeatInterval: 10_000, RepeatCount: repeat},
		Data: map[string]any{"shared": "trigger"}}
	if err := schedule.Prepare(tr, start); err != nil {
		t.Fatal(err)
	}
	if err := st.CreateTrigger(ctx, tr); err != nil {
		t.Fatal(err)
	}
	s := New(st, jobs.Registry{model.JobRestGet: rec},
		Config{InstanceID: "test-1", Workers: workers, MisfireThreshold: time.Minute},
		slog.New(slog.NewTextHandler(io.Discard, nil)))
	c := &clock{}
	c.set(start)
	s.now = c.get
	return s, st, c
}

func TestFiresOnScheduleAndRecordsRuns(t *testing.T) {
	rec := &recorder{}
	s, st, c := setup(t, rec, 2, 2)
	ctx := context.Background()

	for range 6 { // 0s .. 50s: the trigger fires at 0, 10 and 20 seconds
		if _, err := s.Tick(ctx); err != nil {
			t.Fatal(err)
		}
		s.Wait()
		c.add(10 * time.Second)
	}

	if rec.count() != 3 {
		t.Fatalf("executed %d times, want 3", rec.count())
	}
	if rec.calls[0]["shared"] != "trigger" || rec.calls[0]["target"] != "http://job" {
		t.Errorf("trigger data must override job data: %v", rec.calls[0])
	}
	runs, _ := st.ListRuns(ctx, nil, 10)
	if len(runs) != 3 || !runs[0].Succeeded || runs[0].InstanceID != "test-1" || runs[0].Message != "done" {
		t.Errorf("runs = %+v", runs)
	}
	tr, _ := st.GetTrigger(ctx, model.NewKey("every-10s", ""))
	if tr.State != model.StateComplete {
		t.Errorf("state = %s, want COMPLETE", tr.State)
	}
}

func TestFailedAndPanickingJobsAreRecorded(t *testing.T) {
	ctx := context.Background()

	rec := &recorder{err: errors.New("remote said no")}
	s, st, _ := setup(t, rec, 1, 0)
	s.Tick(ctx) //nolint:errcheck
	s.Wait()
	runs, _ := st.ListRuns(ctx, nil, 1)
	if len(runs) != 1 || runs[0].Succeeded || !strings.Contains(runs[0].Message, "remote said no") {
		t.Errorf("failed run = %+v", runs)
	}

	panicking := &recorder{panic: true}
	s, st, _ = setup(t, panicking, 1, 0)
	s.Tick(ctx) //nolint:errcheck
	s.Wait()
	runs, _ = st.ListRuns(ctx, nil, 1)
	if len(runs) != 1 || runs[0].Succeeded || !strings.Contains(runs[0].Message, "panicked") {
		t.Errorf("panicking run = %+v", runs)
	}
}

func TestDoesNotClaimMoreThanItCanRun(t *testing.T) {
	ctx := context.Background()
	rec := &recorder{block: make(chan struct{})}
	s, st, c := setup(t, rec, 1, -1)

	if n, _ := s.Tick(ctx); n != 1 {
		t.Fatalf("first tick started %d jobs", n)
	}
	c.add(10 * time.Second)
	// The only worker is busy: the due trigger stays in the store for another instance.
	if n, _ := s.Tick(ctx); n != 0 {
		t.Errorf("busy tick started %d jobs, want 0", n)
	}
	tr, _ := st.GetTrigger(ctx, model.NewKey("every-10s", ""))
	if !tr.NextFireTime.Equal(start.Add(10 * time.Second)) {
		t.Errorf("trigger was claimed while busy: next = %v", tr.NextFireTime)
	}
	close(rec.block)
	s.Wait()
}

func TestUnknownJobTypeFailsTheRun(t *testing.T) {
	ctx := context.Background()
	s, st, _ := setup(t, &recorder{}, 1, 0)
	s.executors = jobs.Registry{}

	s.Tick(ctx) //nolint:errcheck
	s.Wait()

	runs, _ := st.ListRuns(ctx, nil, 1)
	if len(runs) != 1 || runs[0].Succeeded || !strings.Contains(runs[0].Message, "no executor") {
		t.Errorf("runs = %+v", runs)
	}
}

func TestOldRunsAreDeleted(t *testing.T) {
	ctx := context.Background()
	s, st, c := setup(t, &recorder{}, 1, 0)
	s.cfg.RunRetention = 24 * time.Hour
	st.RecordRun(ctx, &model.Run{JobKey: model.NewKey("ping", ""), FireTime: start.Add(-48 * time.Hour)}) //nolint:errcheck

	s.Tick(ctx) //nolint:errcheck
	s.Wait()
	c.add(time.Minute)

	runs, _ := st.ListRuns(ctx, nil, 10)
	if len(runs) != 1 || !runs[0].FireTime.Equal(start) {
		t.Errorf("runs after cleanup = %+v", runs)
	}
}

func TestRunStopsAndWaitsForJobs(t *testing.T) {
	rec := &recorder{}
	s, _, _ := setup(t, rec, 1, 0)
	s.cfg.PollInterval = 10 * time.Millisecond
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		s.Run(ctx)
		close(done)
	}()
	deadline := time.Now().Add(2 * time.Second)
	for rec.count() == 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not stop")
	}
	if rec.count() != 1 {
		t.Errorf("executed %d times, want 1", rec.count())
	}
}
