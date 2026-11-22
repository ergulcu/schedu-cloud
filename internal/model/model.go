// Copyright 2025 Mehmet ERGULCU
// SPDX-License-Identifier: Apache-2.0

// Package model holds the scheduler's domain types: jobs (what to do), triggers (when to do it)
// and runs (what happened). The names follow Quartz, which the Java version was built on.
package model

import (
	"errors"
	"fmt"
	"time"
)

// DefaultGroup is used when a key has no group, as in Quartz.
const DefaultGroup = "DEFAULT"

// Key names a job or a trigger. Name and group together are unique.
type Key struct {
	Name  string `json:"name"`
	Group string `json:"group"`
}

// NewKey returns a key with DefaultGroup filled in.
func NewKey(name, group string) Key {
	if group == "" {
		group = DefaultGroup
	}
	return Key{Name: name, Group: group}
}

// Normalized fills in the default group.
func (k Key) Normalized() Key { return NewKey(k.Name, k.Group) }

func (k Key) String() string { return k.Group + "." + k.Name }

// Validate checks that the key has a name.
func (k Key) Validate(what string) error {
	if k.Name == "" {
		return Invalidf("%s key needs a name", what)
	}
	if len(k.Name) > 200 || len(k.Group) > 200 {
		return Invalidf("%s name and group may have at most 200 characters", what)
	}
	return nil
}

// JobType selects the code that runs when a job fires.
type JobType string

const (
	// JobRestGet sends GET to the URL in the "target" data entry.
	JobRestGet JobType = "REST_GET"
	// JobKafkaEvent publishes the "value" data entry to the Kafka topic in "topic".
	JobKafkaEvent JobType = "KAFKA_EVENT"
)

// Valid reports whether the type is known.
func (t JobType) Valid() bool { return t == JobRestGet || t == JobKafkaEvent }

// Job is the work. Its data is merged with the data of the trigger that fires it.
type Job struct {
	Key         Key    `json:"key"`
	Description string `json:"description,omitempty"`
	// Durable jobs stay when their last trigger is deleted; others are removed with it.
	Durable   bool           `json:"durable"`
	Type      JobType        `json:"jobType"`
	Data      map[string]any `json:"jobDataMap,omitempty"`
	CreatedAt time.Time      `json:"createdAt"`
	UpdatedAt time.Time      `json:"updatedAt"`
}

// Validate checks the job before it is stored.
func (j *Job) Validate() error {
	if err := j.Key.Validate("job"); err != nil {
		return err
	}
	if !j.Type.Valid() {
		return Invalidf("jobType must be %s or %s", JobRestGet, JobKafkaEvent)
	}
	return nil
}

// TriggerType is the kind of schedule.
type TriggerType string

const (
	// TriggerSimple fires at a start time and then every RepeatInterval, RepeatCount more times.
	TriggerSimple TriggerType = "SIMPLE"
	// TriggerCron fires on the times a cron expression describes.
	TriggerCron TriggerType = "CRON"
)

// TriggerState is the life cycle state of a trigger.
type TriggerState string

const (
	StateNormal   TriggerState = "NORMAL"
	StatePaused   TriggerState = "PAUSED"
	StateComplete TriggerState = "COMPLETE"
	StateError    TriggerState = "ERROR"
	// StateNone is reported for a trigger that does not exist.
	StateNone TriggerState = "NONE"
)

// MisfirePolicy says what happens when a trigger was due long ago, e.g. because every instance was down.
type MisfirePolicy string

const (
	// MisfireFireOnceNow fires once right away and then continues with the schedule from now.
	MisfireFireOnceNow MisfirePolicy = "FIRE_ONCE_NOW"
	// MisfireSkip drops the missed fire times and waits for the next one.
	MisfireSkip MisfirePolicy = "SKIP"
)

// SimpleSchedule is the schedule of a SIMPLE trigger.
type SimpleSchedule struct {
	// RepeatInterval is the pause between fires in milliseconds.
	RepeatInterval int64 `json:"repeatInterval"`
	// RepeatCount is how many times the trigger fires after the first time; -1 means forever.
	RepeatCount int `json:"repeatCount"`
}

// CronSchedule is the schedule of a CRON trigger.
type CronSchedule struct {
	CronExpression string `json:"cronExpression"`
	// TimeZone is an IANA name such as Europe/Istanbul; UTC when empty.
	TimeZone string `json:"timeZone,omitempty"`
}

// Trigger says when a job runs.
type Trigger struct {
	Key              Key             `json:"key"`
	JobKey           Key             `json:"jobKey"`
	Type             TriggerType     `json:"triggerType"`
	Description      string          `json:"description,omitempty"`
	StartTime        time.Time       `json:"startTime"`
	EndTime          *time.Time      `json:"endTime,omitempty"`
	NextFireTime     *time.Time      `json:"nextFireTime,omitempty"`
	PreviousFireTime *time.Time      `json:"previousFireTime,omitempty"`
	Priority         int             `json:"priority"`
	MisfirePolicy    MisfirePolicy   `json:"misfirePolicy"`
	State            TriggerState    `json:"state"`
	Cron             *CronSchedule   `json:"cronSchedule,omitempty"`
	Simple           *SimpleSchedule `json:"simpleSchedule,omitempty"`
	TimesTriggered   int64           `json:"timesTriggered"`
	Data             map[string]any  `json:"jobDataMap,omitempty"`
	CreatedAt        time.Time       `json:"createdAt"`
	UpdatedAt        time.Time       `json:"updatedAt"`
}

// DefaultPriority is the priority of triggers that do not set one; higher fires first when several are due.
const DefaultPriority = 5

// Run is one execution of a job.
type Run struct {
	ID            int64     `json:"id"`
	JobKey        Key       `json:"jobKey"`
	TriggerKey    Key       `json:"triggerKey"`
	InstanceID    string    `json:"instanceId"`
	ScheduledTime time.Time `json:"scheduledTime"`
	FireTime      time.Time `json:"fireTime"`
	FinishedAt    time.Time `json:"finishedAt"`
	Succeeded     bool      `json:"succeeded"`
	Message       string    `json:"message"`
}

// Fire is a due trigger that a scheduler instance has claimed and must now run.
type Fire struct {
	Trigger       Trigger
	Job           Job
	ScheduledTime time.Time
	FireTime      time.Time
}

// MergedData returns the job data overlaid with the trigger data.
func (f Fire) MergedData() map[string]any {
	data := make(map[string]any, len(f.Job.Data)+len(f.Trigger.Data))
	for k, v := range f.Job.Data {
		data[k] = v
	}
	for k, v := range f.Trigger.Data {
		data[k] = v
	}
	return data
}

// Sentinel errors that the API maps to status codes.
var (
	ErrNotFound      = errors.New("not found")
	ErrAlreadyExists = errors.New("already exists")
	ErrInvalid       = errors.New("invalid")
)

// Invalidf returns an error wrapping ErrInvalid.
func Invalidf(format string, args ...any) error {
	return fmt.Errorf("%w: %s", ErrInvalid, fmt.Sprintf(format, args...))
}

// NotFoundf returns an error wrapping ErrNotFound.
func NotFoundf(format string, args ...any) error {
	return fmt.Errorf("%w: %s", ErrNotFound, fmt.Sprintf(format, args...))
}

// AlreadyExistsf returns an error wrapping ErrAlreadyExists.
func AlreadyExistsf(format string, args ...any) error {
	return fmt.Errorf("%w: %s", ErrAlreadyExists, fmt.Sprintf(format, args...))
}
