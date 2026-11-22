// Copyright 2025 Mehmet ERGULCU
// SPDX-License-Identifier: Apache-2.0

// Package postgres is the Store for production. Several scheduler instances can share one database:
// due triggers are claimed with SELECT ... FOR UPDATE SKIP LOCKED, so each fire runs on one instance only
// (the job Quartz's clustered JDBC job store did in the Java version).
package postgres

import (
	"context"
	"embed"
	"errors"
	"fmt"
	"io/fs"
	"sort"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/ergulcu/schedu-cloud/internal/model"
	"github.com/ergulcu/schedu-cloud/internal/store"
)

//go:embed migrations/*.sql
var migrations embed.FS

// migrationLock is the advisory lock that keeps two starting instances from migrating at the same time.
const migrationLock = 7_315_200_421

// Store is safe for concurrent use.
type Store struct {
	pool *pgxpool.Pool
}

var _ store.Store = (*Store)(nil)

// Open connects to the database and brings the schema up to date.
func Open(ctx context.Context, url string) (*Store, error) {
	pool, err := pgxpool.New(ctx, url)
	if err != nil {
		return nil, fmt.Errorf("connect to PostgreSQL: %w", err)
	}
	s := &Store{pool: pool}
	if err := s.migrate(ctx); err != nil {
		pool.Close()
		return nil, err
	}
	return s, nil
}

func (s *Store) migrate(ctx context.Context) error {
	conn, err := s.pool.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("connect to PostgreSQL: %w", err)
	}
	defer conn.Release()
	if _, err := conn.Exec(ctx, "SELECT pg_advisory_lock($1)", migrationLock); err != nil {
		return fmt.Errorf("lock for migration: %w", err)
	}
	defer conn.Exec(context.WithoutCancel(ctx), "SELECT pg_advisory_unlock($1)", migrationLock) //nolint:errcheck

	if _, err := conn.Exec(ctx, `CREATE TABLE IF NOT EXISTS schema_migrations (
		version TEXT PRIMARY KEY, applied_at TIMESTAMPTZ NOT NULL DEFAULT now())`); err != nil {
		return fmt.Errorf("create schema_migrations: %w", err)
	}
	names, err := fs.Glob(migrations, "migrations/*.sql")
	if err != nil {
		return err
	}
	sort.Strings(names)
	for _, name := range names {
		version := strings.TrimSuffix(strings.TrimPrefix(name, "migrations/"), ".sql")
		var applied bool
		if err := conn.QueryRow(ctx, "SELECT EXISTS (SELECT 1 FROM schema_migrations WHERE version = $1)", version).
			Scan(&applied); err != nil {
			return err
		}
		if applied {
			continue
		}
		sql, err := migrations.ReadFile(name)
		if err != nil {
			return err
		}
		err = pgx.BeginFunc(ctx, conn, func(tx pgx.Tx) error {
			if _, err := tx.Exec(ctx, string(sql)); err != nil {
				return err
			}
			_, err := tx.Exec(ctx, "INSERT INTO schema_migrations (version) VALUES ($1)", version)
			return err
		})
		if err != nil {
			return fmt.Errorf("migration %s: %w", version, err)
		}
	}
	return nil
}

func (s *Store) Ping(ctx context.Context) error { return s.pool.Ping(ctx) }

func (s *Store) Close() { s.pool.Close() }

// ---- jobs ----

func (s *Store) CreateJob(ctx context.Context, job *model.Job) error {
	now := time.Now().UTC()
	_, err := s.pool.Exec(ctx, `INSERT INTO jobs (job_group, job_name, description, durable, job_type, job_data,
		created_at, updated_at) VALUES ($1, $2, $3, $4, $5, $6, $7, $7)`,
		job.Key.Group, job.Key.Name, job.Description, job.Durable, string(job.Type), data(job.Data), now)
	if isUniqueViolation(err) {
		return model.AlreadyExistsf("job %s", job.Key)
	}
	if err != nil {
		return err
	}
	job.CreatedAt, job.UpdatedAt = now, now
	return nil
}

func (s *Store) UpdateJob(ctx context.Context, job *model.Job) error {
	now := time.Now().UTC()
	err := s.pool.QueryRow(ctx, `UPDATE jobs SET description = $3, durable = $4, job_type = $5, job_data = $6,
		updated_at = $7 WHERE job_group = $1 AND job_name = $2 RETURNING created_at`,
		job.Key.Group, job.Key.Name, job.Description, job.Durable, string(job.Type), data(job.Data), now).
		Scan(&job.CreatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return model.NotFoundf("job %s", job.Key)
	}
	job.UpdatedAt = now
	return err
}

const jobColumns = "job_group, job_name, description, durable, job_type, job_data, created_at, updated_at"

func (s *Store) GetJob(ctx context.Context, key model.Key) (model.Job, error) {
	job, err := scanJob(s.pool.QueryRow(ctx, "SELECT "+jobColumns+" FROM jobs WHERE job_group = $1 AND job_name = $2",
		key.Group, key.Name))
	if errors.Is(err, pgx.ErrNoRows) {
		return model.Job{}, model.NotFoundf("job %s", key)
	}
	return job, err
}

func (s *Store) ListJobs(ctx context.Context) ([]model.Job, error) {
	rows, err := s.pool.Query(ctx, "SELECT "+jobColumns+" FROM jobs ORDER BY job_group, job_name")
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, func(row pgx.CollectableRow) (model.Job, error) { return scanJob(row) })
}

func (s *Store) DeleteJob(ctx context.Context, key model.Key) (bool, error) {
	tag, err := s.pool.Exec(ctx, "DELETE FROM jobs WHERE job_group = $1 AND job_name = $2", key.Group, key.Name)
	return tag.RowsAffected() > 0, err
}

func scanJob(row pgx.Row) (model.Job, error) {
	var job model.Job
	var jobType string
	err := row.Scan(&job.Key.Group, &job.Key.Name, &job.Description, &job.Durable, &jobType, &job.Data,
		&job.CreatedAt, &job.UpdatedAt)
	job.Type = model.JobType(jobType)
	job.CreatedAt, job.UpdatedAt = job.CreatedAt.UTC(), job.UpdatedAt.UTC()
	if len(job.Data) == 0 {
		job.Data = nil
	}
	return job, err
}

// ---- triggers ----

const triggerColumns = `trigger_group, trigger_name, job_group, job_name, trigger_type, description, start_time,
	end_time, next_fire_time, previous_fire_time, priority, misfire_policy, state, cron_expression, time_zone,
	repeat_interval_ms, repeat_count, times_triggered, trigger_data, created_at, updated_at`

func (s *Store) CreateTrigger(ctx context.Context, t *model.Trigger) error {
	now := time.Now().UTC()
	args := triggerArgs(t)
	_, err := s.pool.Exec(ctx, `INSERT INTO triggers (`+triggerColumns+`)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $20)`,
		append(args, now)...)
	switch {
	case isUniqueViolation(err):
		return model.AlreadyExistsf("trigger %s", t.Key)
	case isForeignKeyViolation(err):
		return model.NotFoundf("job %s", t.JobKey)
	case err != nil:
		return err
	}
	t.CreatedAt, t.UpdatedAt = now, now
	return nil
}

func (s *Store) ReplaceTrigger(ctx context.Context, t *model.Trigger) error {
	now := time.Now().UTC()
	args := append(triggerArgs(t), now)
	// A paused trigger stays paused when its schedule is replaced.
	err := s.pool.QueryRow(ctx, `UPDATE triggers SET job_group = $3, job_name = $4, trigger_type = $5,
		description = $6, start_time = $7, end_time = $8, next_fire_time = $9, previous_fire_time = $10,
		priority = $11, misfire_policy = $12,
		state = CASE WHEN state = 'PAUSED' AND $13 = 'NORMAL' THEN 'PAUSED' ELSE $13 END,
		cron_expression = $14, time_zone = $15, repeat_interval_ms = $16, repeat_count = $17,
		times_triggered = $18, trigger_data = $19, updated_at = $20
		WHERE trigger_group = $1 AND trigger_name = $2 RETURNING state, created_at`, args...).
		Scan((*string)(&t.State), &t.CreatedAt)
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		return model.NotFoundf("trigger %s", t.Key)
	case isForeignKeyViolation(err):
		return model.NotFoundf("job %s", t.JobKey)
	case err != nil:
		return err
	}
	t.UpdatedAt = now
	return nil
}

func (s *Store) GetTrigger(ctx context.Context, key model.Key) (model.Trigger, error) {
	t, err := scanTrigger(s.pool.QueryRow(ctx, "SELECT "+triggerColumns+
		" FROM triggers WHERE trigger_group = $1 AND trigger_name = $2", key.Group, key.Name))
	if errors.Is(err, pgx.ErrNoRows) {
		return model.Trigger{}, model.NotFoundf("trigger %s", key)
	}
	return t, err
}

func (s *Store) ListTriggers(ctx context.Context) ([]model.Trigger, error) {
	return s.queryTriggers(ctx, "SELECT "+triggerColumns+" FROM triggers ORDER BY trigger_group, trigger_name")
}

func (s *Store) ListTriggersOfJob(ctx context.Context, jobKey model.Key) ([]model.Trigger, error) {
	return s.queryTriggers(ctx, "SELECT "+triggerColumns+
		" FROM triggers WHERE job_group = $1 AND job_name = $2 ORDER BY trigger_group, trigger_name",
		jobKey.Group, jobKey.Name)
}

func (s *Store) queryTriggers(ctx context.Context, sql string, args ...any) ([]model.Trigger, error) {
	rows, err := s.pool.Query(ctx, sql, args...)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, func(row pgx.CollectableRow) (model.Trigger, error) { return scanTrigger(row) })
}

func (s *Store) DeleteTrigger(ctx context.Context, key model.Key) (bool, error) {
	var deleted bool
	err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		var jobGroup, jobName string
		err := tx.QueryRow(ctx, `DELETE FROM triggers WHERE trigger_group = $1 AND trigger_name = $2
			RETURNING job_group, job_name`, key.Group, key.Name).Scan(&jobGroup, &jobName)
		if errors.Is(err, pgx.ErrNoRows) {
			return nil
		}
		if err != nil {
			return err
		}
		deleted = true
		_, err = tx.Exec(ctx, `DELETE FROM jobs j WHERE j.job_group = $1 AND j.job_name = $2 AND NOT j.durable
			AND NOT EXISTS (SELECT 1 FROM triggers t WHERE t.job_group = j.job_group AND t.job_name = j.job_name)`,
			jobGroup, jobName)
		return err
	})
	return deleted, err
}

func (s *Store) PauseTrigger(ctx context.Context, key model.Key) error {
	return s.setState(ctx, key, `CASE WHEN state IN ('NORMAL', 'ERROR') THEN 'PAUSED' ELSE state END`)
}

func (s *Store) ResumeTrigger(ctx context.Context, key model.Key) error {
	return s.setState(ctx, key, `CASE WHEN state = 'PAUSED' THEN 'NORMAL' ELSE state END`)
}

func (s *Store) setState(ctx context.Context, key model.Key, expression string) error {
	tag, err := s.pool.Exec(ctx, "UPDATE triggers SET state = "+expression+
		", updated_at = now() WHERE trigger_group = $1 AND trigger_name = $2", key.Group, key.Name)
	if err == nil && tag.RowsAffected() == 0 {
		return model.NotFoundf("trigger %s", key)
	}
	return err
}

// AcquireDue claims due triggers in one transaction. Rows another instance has locked are skipped, so two
// instances polling at the same moment split the due triggers between them instead of both firing them.
func (s *Store) AcquireDue(ctx context.Context, now time.Time, limit int, advance store.AdvanceFunc) ([]model.Fire, error) {
	if limit <= 0 {
		return nil, nil
	}
	var fires []model.Fire
	err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		fires = fires[:0]
		rows, err := tx.Query(ctx, `SELECT `+prefixed("t", triggerColumns)+`, `+prefixed("j", jobColumns)+`
			FROM triggers t JOIN jobs j ON j.job_group = t.job_group AND j.job_name = t.job_name
			WHERE t.state = 'NORMAL' AND t.next_fire_time <= $1
			ORDER BY t.next_fire_time, t.priority DESC
			LIMIT $2
			FOR UPDATE OF t SKIP LOCKED`, now, limit)
		if err != nil {
			return err
		}
		type claimed struct {
			trigger model.Trigger
			job     model.Job
		}
		list, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (claimed, error) {
			var c claimed
			err := scanTriggerAndJob(row, &c.trigger, &c.job)
			return c, err
		})
		if err != nil {
			return err
		}
		for _, c := range list {
			t := c.trigger
			fire, scheduled := advance(&t, now)
			if _, err := tx.Exec(ctx, `UPDATE triggers SET next_fire_time = $3, previous_fire_time = $4,
				times_triggered = $5, state = $6, updated_at = $7 WHERE trigger_group = $1 AND trigger_name = $2`,
				t.Key.Group, t.Key.Name, t.NextFireTime, t.PreviousFireTime, t.TimesTriggered, string(t.State), now); err != nil {
				return err
			}
			if fire {
				fires = append(fires, model.Fire{Trigger: t, Job: c.job, ScheduledTime: scheduled, FireTime: now})
			}
		}
		return nil
	})
	return fires, err
}

func triggerArgs(t *model.Trigger) []any {
	var cronExpression, timeZone *string
	if t.Cron != nil {
		cronExpression, timeZone = &t.Cron.CronExpression, &t.Cron.TimeZone
	}
	var interval *int64
	var count *int
	if t.Simple != nil {
		interval, count = &t.Simple.RepeatInterval, &t.Simple.RepeatCount
	}
	return []any{t.Key.Group, t.Key.Name, t.JobKey.Group, t.JobKey.Name, string(t.Type), t.Description,
		t.StartTime, t.EndTime, t.NextFireTime, t.PreviousFireTime, t.Priority, string(t.MisfirePolicy),
		string(t.State), cronExpression, timeZone, interval, count, t.TimesTriggered, data(t.Data)}
}

func scanTrigger(row pgx.Row) (model.Trigger, error) {
	var t model.Trigger
	err := scanTriggerInto(row, &t)
	return t, err
}

func scanTriggerAndJob(row pgx.Row, t *model.Trigger, job *model.Job) error {
	var jobType string
	err := scanTriggerInto(row, t, &job.Key.Group, &job.Key.Name, &job.Description, &job.Durable, &jobType,
		&job.Data, &job.CreatedAt, &job.UpdatedAt)
	job.Type = model.JobType(jobType)
	job.CreatedAt, job.UpdatedAt = job.CreatedAt.UTC(), job.UpdatedAt.UTC()
	if len(job.Data) == 0 {
		job.Data = nil
	}
	return err
}

// scanTriggerInto reads the triggerColumns, followed by any extra columns into extra.
func scanTriggerInto(row pgx.Row, t *model.Trigger, extra ...any) error {
	var (
		triggerType, misfire, state string
		cronExpression, timeZone    *string
		interval                    *int64
		count                       *int
	)
	dest := []any{&t.Key.Group, &t.Key.Name, &t.JobKey.Group, &t.JobKey.Name, &triggerType, &t.Description,
		&t.StartTime, &t.EndTime, &t.NextFireTime, &t.PreviousFireTime, &t.Priority, &misfire, &state,
		&cronExpression, &timeZone, &interval, &count, &t.TimesTriggered, &t.Data, &t.CreatedAt, &t.UpdatedAt}
	if err := row.Scan(append(dest, extra...)...); err != nil {
		return err
	}
	t.Type = model.TriggerType(triggerType)
	t.MisfirePolicy = model.MisfirePolicy(misfire)
	t.State = model.TriggerState(state)
	if cronExpression != nil {
		t.Cron = &model.CronSchedule{CronExpression: *cronExpression}
		if timeZone != nil {
			t.Cron.TimeZone = *timeZone
		}
	}
	if interval != nil && count != nil {
		t.Simple = &model.SimpleSchedule{RepeatInterval: *interval, RepeatCount: *count}
	}
	t.StartTime = t.StartTime.UTC()
	t.EndTime = utc(t.EndTime)
	t.NextFireTime = utc(t.NextFireTime)
	t.PreviousFireTime = utc(t.PreviousFireTime)
	t.CreatedAt, t.UpdatedAt = t.CreatedAt.UTC(), t.UpdatedAt.UTC()
	if len(t.Data) == 0 {
		t.Data = nil
	}
	return nil
}

// ---- runs ----

func (s *Store) RecordRun(ctx context.Context, run *model.Run) error {
	return s.pool.QueryRow(ctx, `INSERT INTO job_runs (job_group, job_name, trigger_group, trigger_name, instance_id,
		scheduled_time, fire_time, finished_at, succeeded, message) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
		RETURNING id`, run.JobKey.Group, run.JobKey.Name, run.TriggerKey.Group, run.TriggerKey.Name, run.InstanceID,
		run.ScheduledTime, run.FireTime, run.FinishedAt, run.Succeeded, run.Message).Scan(&run.ID)
}

func (s *Store) ListRuns(ctx context.Context, jobKey *model.Key, limit int) ([]model.Run, error) {
	sql := `SELECT id, job_group, job_name, trigger_group, trigger_name, instance_id, scheduled_time, fire_time,
		finished_at, succeeded, message FROM job_runs`
	args := []any{limit}
	if jobKey != nil {
		sql += " WHERE job_group = $2 AND job_name = $3"
		args = append(args, jobKey.Group, jobKey.Name)
	}
	rows, err := s.pool.Query(ctx, sql+" ORDER BY fire_time DESC, id DESC LIMIT $1", args...)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, func(row pgx.CollectableRow) (model.Run, error) {
		var r model.Run
		err := row.Scan(&r.ID, &r.JobKey.Group, &r.JobKey.Name, &r.TriggerKey.Group, &r.TriggerKey.Name,
			&r.InstanceID, &r.ScheduledTime, &r.FireTime, &r.FinishedAt, &r.Succeeded, &r.Message)
		r.ScheduledTime, r.FireTime, r.FinishedAt = r.ScheduledTime.UTC(), r.FireTime.UTC(), r.FinishedAt.UTC()
		return r, err
	})
}

func (s *Store) DeleteRunsBefore(ctx context.Context, before time.Time) (int64, error) {
	tag, err := s.pool.Exec(ctx, "DELETE FROM job_runs WHERE fire_time < $1", before)
	return tag.RowsAffected(), err
}

// ---- helpers ----

func data(m map[string]any) map[string]any {
	if m == nil {
		return map[string]any{}
	}
	return m
}

func utc(t *time.Time) *time.Time {
	if t == nil {
		return nil
	}
	v := t.UTC()
	return &v
}

func prefixed(alias, columns string) string {
	parts := strings.Split(columns, ",")
	for i, p := range parts {
		parts[i] = alias + "." + strings.TrimSpace(p)
	}
	return strings.Join(parts, ", ")
}

func isUniqueViolation(err error) bool { return pgCode(err) == "23505" }

func isForeignKeyViolation(err error) bool { return pgCode(err) == "23503" }

func pgCode(err error) string {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		return pgErr.Code
	}
	return ""
}
