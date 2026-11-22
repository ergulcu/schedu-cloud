-- Copyright 2025 Mehmet ERGULCU
-- SPDX-License-Identifier: Apache-2.0

CREATE TABLE jobs (
    job_group   TEXT        NOT NULL,
    job_name    TEXT        NOT NULL,
    description TEXT        NOT NULL DEFAULT '',
    durable     BOOLEAN     NOT NULL,
    job_type    TEXT        NOT NULL,
    job_data    JSONB       NOT NULL DEFAULT '{}',
    created_at  TIMESTAMPTZ NOT NULL,
    updated_at  TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (job_group, job_name)
);

CREATE TABLE triggers (
    trigger_group      TEXT        NOT NULL,
    trigger_name       TEXT        NOT NULL,
    job_group          TEXT        NOT NULL,
    job_name           TEXT        NOT NULL,
    trigger_type       TEXT        NOT NULL,
    description        TEXT        NOT NULL DEFAULT '',
    start_time         TIMESTAMPTZ NOT NULL,
    end_time           TIMESTAMPTZ,
    next_fire_time     TIMESTAMPTZ,
    previous_fire_time TIMESTAMPTZ,
    priority           INTEGER     NOT NULL,
    misfire_policy     TEXT        NOT NULL,
    state              TEXT        NOT NULL,
    cron_expression    TEXT,
    time_zone          TEXT,
    repeat_interval_ms BIGINT,
    repeat_count       INTEGER,
    times_triggered    BIGINT      NOT NULL DEFAULT 0,
    trigger_data       JSONB       NOT NULL DEFAULT '{}',
    created_at         TIMESTAMPTZ NOT NULL,
    updated_at         TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (trigger_group, trigger_name),
    FOREIGN KEY (job_group, job_name) REFERENCES jobs (job_group, job_name) ON DELETE CASCADE
);

-- The scheduler asks for this all the time: due triggers that may fire.
CREATE INDEX triggers_due ON triggers (next_fire_time, priority DESC) WHERE state = 'NORMAL';
CREATE INDEX triggers_job ON triggers (job_group, job_name);

CREATE TABLE job_runs (
    id             BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    job_group      TEXT        NOT NULL,
    job_name       TEXT        NOT NULL,
    trigger_group  TEXT        NOT NULL,
    trigger_name   TEXT        NOT NULL,
    instance_id    TEXT        NOT NULL,
    scheduled_time TIMESTAMPTZ NOT NULL,
    fire_time      TIMESTAMPTZ NOT NULL,
    finished_at    TIMESTAMPTZ NOT NULL,
    succeeded      BOOLEAN     NOT NULL,
    message        TEXT        NOT NULL DEFAULT ''
);

CREATE INDEX job_runs_job ON job_runs (job_group, job_name, fire_time DESC);
CREATE INDEX job_runs_fire_time ON job_runs (fire_time);
