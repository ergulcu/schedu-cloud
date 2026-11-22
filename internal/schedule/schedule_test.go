// Copyright 2025 Mehmet ERGULCU
// SPDX-License-Identifier: Apache-2.0

package schedule

import (
	"errors"
	"testing"
	"time"

	"github.com/ergulcu/schedu-cloud/internal/model"
)

var t0 = time.Date(2026, 3, 10, 10, 0, 0, 0, time.UTC)

func simple(interval time.Duration, repeat int) *model.Trigger {
	return &model.Trigger{
		Key:       model.NewKey("t", ""),
		JobKey:    model.NewKey("j", ""),
		Type:      model.TriggerSimple,
		StartTime: t0,
		Simple:    &model.SimpleSchedule{RepeatInterval: interval.Milliseconds(), RepeatCount: repeat},
	}
}

func cronTrigger(expr, tz string) *model.Trigger {
	return &model.Trigger{
		Key:       model.NewKey("c", "g"),
		JobKey:    model.NewKey("j", ""),
		Type:      model.TriggerCron,
		StartTime: t0,
		Cron:      &model.CronSchedule{CronExpression: expr, TimeZone: tz},
	}
}

func mustPrepare(t *testing.T, tr *model.Trigger) {
	t.Helper()
	if err := Prepare(tr, t0); err != nil {
		t.Fatalf("Prepare: %v", err)
	}
}

func TestPrepareFillsDefaults(t *testing.T) {
	tr := simple(time.Minute, 2)
	tr.Key.Group = ""
	tr.StartTime = time.Time{}
	mustPrepare(t, tr)

	if tr.Key.Group != model.DefaultGroup || tr.JobKey.Group != model.DefaultGroup {
		t.Errorf("groups = %q/%q, want DEFAULT", tr.Key.Group, tr.JobKey.Group)
	}
	if !tr.StartTime.Equal(t0) || !tr.NextFireTime.Equal(t0) {
		t.Errorf("start/next = %v/%v, want %v", tr.StartTime, tr.NextFireTime, t0)
	}
	if tr.Priority != model.DefaultPriority || tr.MisfirePolicy != model.MisfireFireOnceNow || tr.State != model.StateNormal {
		t.Errorf("defaults not applied: %+v", tr)
	}
}

func TestPrepareRejectsBadTriggers(t *testing.T) {
	end := t0.Add(-time.Hour)
	cases := map[string]*model.Trigger{
		"no name":            func() *model.Trigger { tr := simple(time.Minute, 1); tr.Key.Name = ""; return tr }(),
		"no job":             func() *model.Trigger { tr := simple(time.Minute, 1); tr.JobKey.Name = ""; return tr }(),
		"no schedule":        {Key: model.NewKey("t", ""), JobKey: model.NewKey("j", ""), Type: model.TriggerSimple},
		"unknown type":       {Key: model.NewKey("t", ""), JobKey: model.NewKey("j", ""), Type: "WEEKLY"},
		"interval too short": simple(100*time.Millisecond, 3),
		"repeat below -1":    simple(time.Minute, -2),
		"end before start":   func() *model.Trigger { tr := simple(time.Minute, 1); tr.EndTime = &end; return tr }(),
		"bad cron":           cronTrigger("every day", ""),
		"bad time zone":      cronTrigger("0 0 * * * *", "Mars/Olympus"),
		"bad misfire":        func() *model.Trigger { tr := simple(time.Minute, 1); tr.MisfirePolicy = "LATER"; return tr }(),
		"never fires":        cronTrigger("0 0 0 30 2 *", ""), // 30 February
	}
	for name, tr := range cases {
		t.Run(name, func(t *testing.T) {
			if err := Prepare(tr, t0); !errors.Is(err, model.ErrInvalid) {
				t.Fatalf("err = %v, want ErrInvalid", err)
			}
		})
	}
}

func TestSimpleTriggerFiresRepeatCountPlusOneTimes(t *testing.T) {
	tr := simple(10*time.Second, 2)
	mustPrepare(t, tr)

	var fired []time.Time
	for now := t0; tr.State == model.StateNormal; now = *tr.NextFireTime {
		fire, scheduled := Advance(tr, now, time.Minute)
		if !fire {
			t.Fatalf("did not fire at %v", now)
		}
		fired = append(fired, scheduled)
		if tr.NextFireTime == nil {
			break
		}
	}

	want := []time.Time{t0, t0.Add(10 * time.Second), t0.Add(20 * time.Second)}
	if len(fired) != len(want) {
		t.Fatalf("fired %v, want %v", fired, want)
	}
	for i := range want {
		if !fired[i].Equal(want[i]) {
			t.Errorf("fire %d at %v, want %v", i, fired[i], want[i])
		}
	}
	if tr.State != model.StateComplete || tr.TimesTriggered != 3 {
		t.Errorf("state %s after %d fires, want COMPLETE after 3", tr.State, tr.TimesTriggered)
	}
	if final := FinalFireTime(tr); final == nil || !final.Equal(want[2]) {
		t.Errorf("final fire time = %v, want %v", final, want[2])
	}
}

func TestOneShotTrigger(t *testing.T) {
	tr := simple(0, 0)
	mustPrepare(t, tr)

	if fire, _ := Advance(tr, t0.Add(time.Second), time.Minute); !fire {
		t.Fatal("one-shot trigger did not fire")
	}
	if tr.State != model.StateComplete || tr.NextFireTime != nil {
		t.Errorf("state = %s next = %v, want COMPLETE without next", tr.State, tr.NextFireTime)
	}
}

func TestForeverTriggerKeepsTheGrid(t *testing.T) {
	tr := simple(time.Minute, -1)
	mustPrepare(t, tr)

	// The poll ran 2 seconds late; the next fire stays on the minute grid.
	Advance(tr, t0.Add(2*time.Second), time.Minute)
	if !tr.NextFireTime.Equal(t0.Add(time.Minute)) {
		t.Errorf("next = %v, want %v", tr.NextFireTime, t0.Add(time.Minute))
	}
	if FinalFireTime(tr) != nil {
		t.Error("a forever trigger has no final fire time")
	}
}

func TestEndTimeCompletesTheTrigger(t *testing.T) {
	tr := simple(time.Minute, -1)
	end := t0.Add(150 * time.Second)
	tr.EndTime = &end
	mustPrepare(t, tr)

	fires := 0
	for tr.NextFireTime != nil {
		if fire, _ := Advance(tr, *tr.NextFireTime, time.Minute); fire {
			fires++
		}
	}
	if fires != 3 || tr.State != model.StateComplete {
		t.Errorf("fired %d times, state %s; want 3 and COMPLETE", fires, tr.State)
	}
}

func TestMisfireFireOnceNow(t *testing.T) {
	tr := simple(time.Minute, -1)
	mustPrepare(t, tr)

	now := t0.Add(10*time.Minute + 5*time.Second) // ten fires were missed
	fire, scheduled := Advance(tr, now, time.Minute)

	if !fire || !scheduled.Equal(t0) {
		t.Fatalf("fire=%v scheduled=%v, want a single fire for %v", fire, scheduled, t0)
	}
	if want := t0.Add(11 * time.Minute); !tr.NextFireTime.Equal(want) {
		t.Errorf("next = %v, want %v (no catch-up of the missed fires)", tr.NextFireTime, want)
	}
	if tr.TimesTriggered != 1 {
		t.Errorf("times triggered = %d, want 1", tr.TimesTriggered)
	}
}

func TestMisfireSkip(t *testing.T) {
	tr := simple(time.Minute, -1)
	tr.MisfirePolicy = model.MisfireSkip
	mustPrepare(t, tr)

	fire, _ := Advance(tr, t0.Add(10*time.Minute+5*time.Second), time.Minute)

	if fire {
		t.Fatal("a skipped misfire must not fire")
	}
	if want := t0.Add(11 * time.Minute); !tr.NextFireTime.Equal(want) {
		t.Errorf("next = %v, want %v", tr.NextFireTime, want)
	}
}

func TestSmallDelaysAreNotMisfires(t *testing.T) {
	tr := simple(time.Minute, -1)
	tr.MisfirePolicy = model.MisfireSkip
	mustPrepare(t, tr)

	if fire, _ := Advance(tr, t0.Add(30*time.Second), time.Minute); !fire {
		t.Fatal("30 seconds late is within the threshold and must fire")
	}
}

func TestCronWithSeconds(t *testing.T) {
	tr := cronTrigger("*/15 * * * * *", "")
	tr.StartTime = t0.Add(500 * time.Millisecond)
	mustPrepare(t, tr)

	if want := t0.Add(15 * time.Second); !tr.NextFireTime.Equal(want) {
		t.Fatalf("first = %v, want %v", tr.NextFireTime, want)
	}
	Advance(tr, *tr.NextFireTime, time.Minute)
	if want := t0.Add(30 * time.Second); !tr.NextFireTime.Equal(want) {
		t.Errorf("second = %v, want %v", tr.NextFireTime, want)
	}
}

func TestCronFiresAtStartWhenItMatches(t *testing.T) {
	tr := cronTrigger("0 0 10 * * ?", "") // Quartz style "?"
	mustPrepare(t, tr)

	if !tr.NextFireTime.Equal(t0) {
		t.Errorf("first = %v, want %v", tr.NextFireTime, t0)
	}
}

func TestCronFiveFieldsAndTimeZone(t *testing.T) {
	// 09:00 in Istanbul (UTC+3) is 06:00 UTC.
	tr := cronTrigger("0 9 * * MON-FRI", "Europe/Istanbul")
	mustPrepare(t, tr)

	want := time.Date(2026, 3, 11, 6, 0, 0, 0, time.UTC) // t0 is Tuesday 10:00 UTC, already past 06:00
	if !tr.NextFireTime.Equal(want) {
		t.Errorf("first = %v, want %v", tr.NextFireTime, want)
	}
	if tr.NextFireTime.Location() != time.UTC {
		t.Error("fire times are stored in UTC")
	}
}
