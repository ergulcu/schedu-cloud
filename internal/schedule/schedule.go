// Copyright 2025 Mehmet ERGULCU
// SPDX-License-Identifier: Apache-2.0

// Package schedule computes fire times. It is pure: it changes trigger values and never talks to a store,
// so every store (memory, PostgreSQL) moves triggers forward in exactly the same way.
package schedule

import (
	"fmt"
	"time"

	"github.com/robfig/cron/v3"

	"github.com/ergulcu/schedu-cloud/internal/model"
)

// MinRepeatInterval is the shortest repeat interval of a SIMPLE trigger. The scheduler polls about once a
// second, so shorter intervals would only pile up catch-up fires.
const MinRepeatInterval = time.Second

// The seconds field is optional: "0 */5 * * * *" (Quartz style, with seconds) and "*/5 * * * *" both work.
// "?" is accepted as in Quartz. Descriptors such as "@hourly" and "@every 90s" work too.
var parser = cron.NewParser(cron.SecondOptional | cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow | cron.Descriptor)

// ParseCron validates a cron expression and returns its schedule in the given IANA time zone (UTC when empty).
func ParseCron(expression, timeZone string) (cron.Schedule, *time.Location, error) {
	loc := time.UTC
	if timeZone != "" {
		var err error
		if loc, err = time.LoadLocation(timeZone); err != nil {
			return nil, nil, model.Invalidf("unknown time zone %q", timeZone)
		}
	}
	sched, err := parser.Parse(expression)
	if err != nil {
		return nil, nil, model.Invalidf("cron expression %q: %v", expression, err)
	}
	return sched, loc, nil
}

// Prepare validates a new or replaced trigger, fills in the defaults and computes its first fire time.
func Prepare(t *model.Trigger, now time.Time) error {
	t.Key = t.Key.Normalized()
	t.JobKey = t.JobKey.Normalized()
	if err := t.Key.Validate("trigger"); err != nil {
		return err
	}
	if err := t.JobKey.Validate("job"); err != nil {
		return err
	}
	if t.Priority == 0 {
		t.Priority = model.DefaultPriority
	}
	switch t.MisfirePolicy {
	case "":
		t.MisfirePolicy = model.MisfireFireOnceNow
	case model.MisfireFireOnceNow, model.MisfireSkip:
	default:
		return model.Invalidf("misfirePolicy must be %s or %s", model.MisfireFireOnceNow, model.MisfireSkip)
	}

	switch t.Type {
	case model.TriggerSimple:
		if t.Simple == nil {
			return model.Invalidf("a SIMPLE trigger needs simpleSchedule")
		}
		if t.Simple.RepeatCount < -1 {
			return model.Invalidf("repeatCount must be -1 (forever) or more")
		}
		if t.Simple.RepeatCount != 0 && time.Duration(t.Simple.RepeatInterval)*time.Millisecond < MinRepeatInterval {
			return model.Invalidf("repeatInterval must be at least %d ms", MinRepeatInterval.Milliseconds())
		}
		if t.Simple.RepeatInterval < 0 {
			return model.Invalidf("repeatInterval cannot be negative")
		}
		t.Cron = nil
	case model.TriggerCron:
		if t.Cron == nil || t.Cron.CronExpression == "" {
			return model.Invalidf("a CRON trigger needs cronSchedule.cronExpression")
		}
		if _, _, err := ParseCron(t.Cron.CronExpression, t.Cron.TimeZone); err != nil {
			return err
		}
		t.Simple = nil
	default:
		return model.Invalidf("triggerType must be %s or %s", model.TriggerSimple, model.TriggerCron)
	}

	if t.StartTime.IsZero() {
		t.StartTime = now
	}
	t.StartTime = t.StartTime.UTC().Truncate(time.Millisecond)
	if t.EndTime != nil {
		end := t.EndTime.UTC().Truncate(time.Millisecond)
		if !end.After(t.StartTime) {
			return model.Invalidf("endTime must be after startTime")
		}
		t.EndTime = &end
	}

	first, err := firstFireTime(t)
	if err != nil {
		return err
	}
	if first == nil {
		return model.Invalidf("the trigger would never fire")
	}
	t.NextFireTime = first
	t.PreviousFireTime = nil
	t.TimesTriggered = 0
	t.State = model.StateNormal
	return nil
}

// Advance is called for a trigger whose next fire time has come. It reports whether the job has to run now
// and for which scheduled time, and moves the trigger to its following fire time (or completes it).
// A trigger that is more than misfireThreshold late follows its misfire policy.
func Advance(t *model.Trigger, now time.Time, misfireThreshold time.Duration) (fire bool, scheduled time.Time) {
	if t.NextFireTime == nil {
		t.State = model.StateComplete
		return false, time.Time{}
	}
	scheduled = *t.NextFireTime
	late := now.Sub(scheduled) > misfireThreshold

	from := scheduled
	switch {
	case !late:
		fire = true
	case t.MisfirePolicy == model.MisfireSkip:
		fire = false
		from = now
	default: // fire once now, forget the other missed times
		fire = true
		from = now
	}
	if fire {
		t.TimesTriggered++
		fired := now
		t.PreviousFireTime = &fired
	}

	next, err := fireTimeAfter(t, from)
	if err != nil || next == nil {
		t.NextFireTime = nil
		t.State = model.StateComplete
		if err != nil {
			t.State = model.StateError
		}
	} else {
		t.NextFireTime = next
	}
	return fire, scheduled
}

// FinalFireTime is the last time a trigger can fire, or nil when it repeats without end.
func FinalFireTime(t *model.Trigger) *time.Time {
	if t.Type == model.TriggerSimple && t.Simple != nil && t.Simple.RepeatCount >= 0 {
		last := t.StartTime.Add(time.Duration(int64(t.Simple.RepeatCount)*t.Simple.RepeatInterval) * time.Millisecond)
		if t.EndTime != nil && last.After(*t.EndTime) {
			return lastSimpleBefore(t, *t.EndTime)
		}
		return &last
	}
	return nil
}

func firstFireTime(t *model.Trigger) (*time.Time, error) {
	switch t.Type {
	case model.TriggerSimple:
		start := t.StartTime
		return withinEnd(t, start), nil
	case model.TriggerCron:
		sched, loc, err := ParseCron(t.Cron.CronExpression, t.Cron.TimeZone)
		if err != nil {
			return nil, err
		}
		// Next returns times strictly after its argument, at whole seconds.
		first := sched.Next(t.StartTime.In(loc).Add(-time.Nanosecond))
		if first.Before(t.StartTime) {
			first = sched.Next(first)
		}
		return cronResult(t, first), nil
	}
	return nil, fmt.Errorf("unknown trigger type %q", t.Type)
}

// fireTimeAfter returns the first scheduled time strictly after the given time.
func fireTimeAfter(t *model.Trigger, after time.Time) (*time.Time, error) {
	switch t.Type {
	case model.TriggerSimple:
		s := t.Simple
		if s == nil || s.RepeatCount == 0 || s.RepeatInterval <= 0 {
			return nil, nil
		}
		interval := time.Duration(s.RepeatInterval) * time.Millisecond
		// Fire i (0, 1, 2, ...) is due at start + i*interval.
		index := int64(0)
		if !after.Before(t.StartTime) {
			index = int64(after.Sub(t.StartTime)/interval) + 1
		}
		if s.RepeatCount >= 0 && index > int64(s.RepeatCount) {
			return nil, nil
		}
		next := t.StartTime.Add(time.Duration(index) * interval)
		return withinEnd(t, next), nil
	case model.TriggerCron:
		sched, loc, err := ParseCron(t.Cron.CronExpression, t.Cron.TimeZone)
		if err != nil {
			return nil, err
		}
		return cronResult(t, sched.Next(after.In(loc))), nil
	}
	return nil, fmt.Errorf("unknown trigger type %q", t.Type)
}

func cronResult(t *model.Trigger, next time.Time) *time.Time {
	if next.IsZero() { // the expression has no future time
		return nil
	}
	return withinEnd(t, next.UTC())
}

func withinEnd(t *model.Trigger, next time.Time) *time.Time {
	if t.EndTime != nil && next.After(*t.EndTime) {
		return nil
	}
	return &next
}

func lastSimpleBefore(t *model.Trigger, end time.Time) *time.Time {
	interval := time.Duration(t.Simple.RepeatInterval) * time.Millisecond
	if interval <= 0 {
		start := t.StartTime
		return &start
	}
	last := t.StartTime.Add(end.Sub(t.StartTime) / interval * interval)
	return &last
}
