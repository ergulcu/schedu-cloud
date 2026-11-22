// Copyright 2025 Mehmet ERGULCU
// SPDX-License-Identifier: Apache-2.0

// Package storetest is the behaviour every Store must show. The memory and the PostgreSQL store both run it.
package storetest

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/ergulcu/schedu-cloud/internal/model"
	"github.com/ergulcu/schedu-cloud/internal/schedule"
	"github.com/ergulcu/schedu-cloud/internal/store"
)

// Factory returns an empty store for one test.
type Factory func(t *testing.T) store.Store

var base = time.Date(2026, 3, 10, 10, 0, 0, 0, time.UTC)

// Advance is schedule.Advance with a one minute misfire threshold.
func Advance(t *model.Trigger, now time.Time) (bool, time.Time) {
	return schedule.Advance(t, now, time.Minute)
}

// Job returns a durable REST_GET job.
func Job(name string) *model.Job {
	return &model.Job{Key: model.NewKey(name, "jobs"), Durable: true, Type: model.JobRestGet,
		Data: map[string]any{"target": "http://example.test/" + name}}
}

// Trigger returns a prepared SIMPLE trigger for the job that fires every minute from start.
func Trigger(t *testing.T, name string, job model.Key, start time.Time) *model.Trigger {
	t.Helper()
	tr := &model.Trigger{Key: model.NewKey(name, "triggers"), JobKey: job, Type: model.TriggerSimple,
		StartTime: start, Simple: &model.SimpleSchedule{RepeatInterval: 60_000, RepeatCount: -1},
		Data: map[string]any{"extra": "yes"}}
	if err := schedule.Prepare(tr, start); err != nil {
		t.Fatal(err)
	}
	return tr
}

// Run runs the whole suite.
func Run(t *testing.T, newStore Factory) {
	tests := map[string]func(*testing.T, store.Store){
		"jobs":                       testJobs,
		"triggers":                   testTriggers,
		"cron trigger round trip":    testCronRoundTrip,
		"trigger needs its job":      testTriggerNeedsJob,
		"deleting a job":             testDeleteJob,
		"non-durable job goes":       testNonDurableJob,
		"pause and resume":           testPauseResume,
		"acquire due":                testAcquireDue,
		"acquire respects the limit": testAcquireLimit,
		"runs":                       testRuns,
	}
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			s := newStore(t)
			test(t, s)
		})
	}
}

func testJobs(t *testing.T, s store.Store) {
	ctx := context.Background()
	job := Job("ping")
	if err := s.CreateJob(ctx, job); err != nil {
		t.Fatal(err)
	}
	if job.CreatedAt.IsZero() {
		t.Error("CreatedAt not set")
	}
	if err := s.CreateJob(ctx, Job("ping")); !errors.Is(err, model.ErrAlreadyExists) {
		t.Errorf("duplicate job: err = %v, want ErrAlreadyExists", err)
	}

	job.Description = "pings the example"
	job.Data["target"] = "http://example.test/other"
	if err := s.UpdateJob(ctx, job); err != nil {
		t.Fatal(err)
	}
	got, err := s.GetJob(ctx, job.Key)
	if err != nil {
		t.Fatal(err)
	}
	if got.Description != "pings the example" || got.Data["target"] != "http://example.test/other" || !got.Durable {
		t.Errorf("job = %+v", got)
	}
	if err := s.UpdateJob(ctx, Job("missing")); !errors.Is(err, model.ErrNotFound) {
		t.Errorf("update missing: err = %v", err)
	}
	if _, err := s.GetJob(ctx, model.NewKey("missing", "jobs")); !errors.Is(err, model.ErrNotFound) {
		t.Errorf("get missing: err = %v", err)
	}

	if err := s.CreateJob(ctx, Job("another")); err != nil {
		t.Fatal(err)
	}
	jobs, err := s.ListJobs(ctx)
	if err != nil || len(jobs) != 2 || jobs[0].Key.Name != "another" {
		t.Errorf("ListJobs = %v, %v", jobs, err)
	}
}

func testTriggers(t *testing.T, s store.Store) {
	ctx := context.Background()
	job := Job("ping")
	mustCreateJob(t, s, job)
	tr := Trigger(t, "every-minute", job.Key, base)
	if err := s.CreateTrigger(ctx, tr); err != nil {
		t.Fatal(err)
	}
	if err := s.CreateTrigger(ctx, Trigger(t, "every-minute", job.Key, base)); !errors.Is(err, model.ErrAlreadyExists) {
		t.Errorf("duplicate trigger: err = %v", err)
	}

	got, err := s.GetTrigger(ctx, tr.Key)
	if err != nil {
		t.Fatal(err)
	}
	if got.Type != model.TriggerSimple || got.Simple == nil || got.Simple.RepeatInterval != 60_000 ||
		got.Simple.RepeatCount != -1 || !got.NextFireTime.Equal(base) || got.State != model.StateNormal ||
		got.Data["extra"] != "yes" || got.Priority != model.DefaultPriority {
		t.Errorf("trigger = %+v", got)
	}

	replacement := Trigger(t, "every-minute", job.Key, base.Add(time.Hour))
	replacement.Description = "moved"
	if err := s.ReplaceTrigger(ctx, replacement); err != nil {
		t.Fatal(err)
	}
	got, _ = s.GetTrigger(ctx, tr.Key)
	if got.Description != "moved" || !got.NextFireTime.Equal(base.Add(time.Hour)) {
		t.Errorf("replaced trigger = %+v", got)
	}
	if err := s.ReplaceTrigger(ctx, Trigger(t, "unknown", job.Key, base)); !errors.Is(err, model.ErrNotFound) {
		t.Errorf("replace missing: err = %v", err)
	}

	other := Job("other")
	mustCreateJob(t, s, other)
	mustCreateTrigger(t, s, Trigger(t, "a-other", other.Key, base))
	all, _ := s.ListTriggers(ctx)
	ofJob, _ := s.ListTriggersOfJob(ctx, job.Key)
	if len(all) != 2 || len(ofJob) != 1 || ofJob[0].Key != tr.Key {
		t.Errorf("all = %d, of job = %v", len(all), ofJob)
	}
}

func testCronRoundTrip(t *testing.T, s store.Store) {
	ctx := context.Background()
	job := Job("ping")
	mustCreateJob(t, s, job)
	end := base.Add(48 * time.Hour)
	tr := &model.Trigger{Key: model.NewKey("nightly", ""), JobKey: job.Key, Type: model.TriggerCron,
		StartTime: base, EndTime: &end, Priority: 9, MisfirePolicy: model.MisfireSkip,
		Cron: &model.CronSchedule{CronExpression: "0 30 2 * * *", TimeZone: "Europe/Istanbul"}}
	if err := schedule.Prepare(tr, base); err != nil {
		t.Fatal(err)
	}
	mustCreateTrigger(t, s, tr)

	got, err := s.GetTrigger(ctx, tr.Key)
	if err != nil {
		t.Fatal(err)
	}
	if got.Cron == nil || got.Cron.CronExpression != "0 30 2 * * *" || got.Cron.TimeZone != "Europe/Istanbul" ||
		got.Simple != nil || got.EndTime == nil || !got.EndTime.Equal(end) || got.Priority != 9 ||
		got.MisfirePolicy != model.MisfireSkip || !got.NextFireTime.Equal(*tr.NextFireTime) {
		t.Errorf("cron trigger = %+v (cron %+v)", got, got.Cron)
	}
}

func testTriggerNeedsJob(t *testing.T, s store.Store) {
	err := s.CreateTrigger(context.Background(), Trigger(t, "orphan", model.NewKey("nope", "jobs"), base))
	if !errors.Is(err, model.ErrNotFound) {
		t.Errorf("err = %v, want ErrNotFound", err)
	}
}

func testDeleteJob(t *testing.T, s store.Store) {
	ctx := context.Background()
	job := Job("ping")
	mustCreateJob(t, s, job)
	tr := Trigger(t, "t1", job.Key, base)
	mustCreateTrigger(t, s, tr)

	deleted, err := s.DeleteJob(ctx, job.Key)
	if err != nil || !deleted {
		t.Fatalf("DeleteJob = %v, %v", deleted, err)
	}
	if _, err := s.GetTrigger(ctx, tr.Key); !errors.Is(err, model.ErrNotFound) {
		t.Error("the job's triggers must go with it")
	}
	if deleted, _ := s.DeleteJob(ctx, job.Key); deleted {
		t.Error("second delete reported true")
	}
}

func testNonDurableJob(t *testing.T, s store.Store) {
	ctx := context.Background()
	job := Job("temporary")
	job.Durable = false
	mustCreateJob(t, s, job)
	mustCreateTrigger(t, s, Trigger(t, "t1", job.Key, base))
	mustCreateTrigger(t, s, Trigger(t, "t2", job.Key, base))

	if deleted, err := s.DeleteTrigger(ctx, model.NewKey("t1", "triggers")); err != nil || !deleted {
		t.Fatalf("DeleteTrigger = %v, %v", deleted, err)
	}
	if _, err := s.GetJob(ctx, job.Key); err != nil {
		t.Error("the job still has a trigger and must stay")
	}
	if _, err := s.DeleteTrigger(ctx, model.NewKey("t2", "triggers")); err != nil {
		t.Fatal(err)
	}
	if _, err := s.GetJob(ctx, job.Key); !errors.Is(err, model.ErrNotFound) {
		t.Error("a non-durable job must go with its last trigger")
	}
	if deleted, _ := s.DeleteTrigger(ctx, model.NewKey("t2", "triggers")); deleted {
		t.Error("deleting a missing trigger reported true")
	}
}

func testPauseResume(t *testing.T, s store.Store) {
	ctx := context.Background()
	job := Job("ping")
	mustCreateJob(t, s, job)
	tr := Trigger(t, "t1", job.Key, base)
	mustCreateTrigger(t, s, tr)

	if err := s.PauseTrigger(ctx, tr.Key); err != nil {
		t.Fatal(err)
	}
	if fires, _ := s.AcquireDue(ctx, base.Add(time.Second), 10, Advance); len(fires) != 0 {
		t.Error("a paused trigger fired")
	}
	// Replacing the schedule keeps the trigger paused.
	if err := s.ReplaceTrigger(ctx, Trigger(t, "t1", job.Key, base)); err != nil {
		t.Fatal(err)
	}
	if got, _ := s.GetTrigger(ctx, tr.Key); got.State != model.StatePaused {
		t.Errorf("state after replace = %s, want PAUSED", got.State)
	}
	if err := s.ResumeTrigger(ctx, tr.Key); err != nil {
		t.Fatal(err)
	}
	if fires, _ := s.AcquireDue(ctx, base.Add(time.Second), 10, Advance); len(fires) != 1 {
		t.Errorf("resumed trigger fired %d times, want 1", len(fires))
	}
	if err := s.PauseTrigger(ctx, model.NewKey("missing", "")); !errors.Is(err, model.ErrNotFound) {
		t.Errorf("pause missing: err = %v", err)
	}
}

func testAcquireDue(t *testing.T, s store.Store) {
	ctx := context.Background()
	job := Job("ping")
	mustCreateJob(t, s, job)
	mustCreateTrigger(t, s, Trigger(t, "now", job.Key, base))
	mustCreateTrigger(t, s, Trigger(t, "later", job.Key, base.Add(time.Hour)))

	fires, err := s.AcquireDue(ctx, base.Add(time.Second), 10, Advance)
	if err != nil {
		t.Fatal(err)
	}
	if len(fires) != 1 || fires[0].Trigger.Key.Name != "now" {
		t.Fatalf("fires = %+v", fires)
	}
	f := fires[0]
	if !f.ScheduledTime.Equal(base) || f.Job.Key != job.Key || f.Job.Data["target"] == nil ||
		f.MergedData()["extra"] != "yes" {
		t.Errorf("fire = %+v", f)
	}

	// The trigger moved on and does not fire again for the same time.
	if again, _ := s.AcquireDue(ctx, base.Add(2*time.Second), 10, Advance); len(again) != 0 {
		t.Errorf("fired twice for one scheduled time: %+v", again)
	}
	got, _ := s.GetTrigger(ctx, model.NewKey("now", "triggers"))
	if !got.NextFireTime.Equal(base.Add(time.Minute)) || got.TimesTriggered != 1 || got.PreviousFireTime == nil {
		t.Errorf("trigger after fire = %+v", got)
	}
}

func testAcquireLimit(t *testing.T, s store.Store) {
	ctx := context.Background()
	job := Job("ping")
	mustCreateJob(t, s, job)
	for _, name := range []string{"a", "b", "c"} {
		mustCreateTrigger(t, s, Trigger(t, name, job.Key, base))
	}
	first, _ := s.AcquireDue(ctx, base, 2, Advance)
	second, _ := s.AcquireDue(ctx, base, 2, Advance)
	if len(first) != 2 || len(second) != 1 {
		t.Errorf("acquired %d then %d, want 2 then 1", len(first), len(second))
	}
}

func testRuns(t *testing.T, s store.Store) {
	ctx := context.Background()
	jobA, jobB := model.NewKey("a", "jobs"), model.NewKey("b", "jobs")
	for i, key := range []model.Key{jobA, jobB, jobA} {
		run := &model.Run{JobKey: key, TriggerKey: model.NewKey("t", ""), InstanceID: "test",
			ScheduledTime: base.Add(time.Duration(i) * time.Minute), FireTime: base.Add(time.Duration(i) * time.Minute),
			FinishedAt: base.Add(time.Duration(i) * time.Minute), Succeeded: i != 1, Message: "200 OK"}
		if err := s.RecordRun(ctx, run); err != nil {
			t.Fatal(err)
		}
		if run.ID == 0 {
			t.Error("run id not set")
		}
	}
	all, _ := s.ListRuns(ctx, nil, 10)
	ofA, _ := s.ListRuns(ctx, &jobA, 10)
	limited, _ := s.ListRuns(ctx, nil, 1)
	if len(all) != 3 || len(ofA) != 2 || len(limited) != 1 || !limited[0].FireTime.Equal(base.Add(2*time.Minute)) {
		t.Errorf("all = %d, of a = %d, newest = %+v", len(all), len(ofA), limited)
	}
	deleted, err := s.DeleteRunsBefore(ctx, base.Add(90*time.Second))
	if err != nil || deleted != 2 {
		t.Errorf("DeleteRunsBefore = %d, %v; want 2", deleted, err)
	}
}

func mustCreateJob(t *testing.T, s store.Store, job *model.Job) {
	t.Helper()
	if err := s.CreateJob(context.Background(), job); err != nil {
		t.Fatal(err)
	}
}

func mustCreateTrigger(t *testing.T, s store.Store, tr *model.Trigger) {
	t.Helper()
	if err := s.CreateTrigger(context.Background(), tr); err != nil {
		t.Fatal(err)
	}
}
