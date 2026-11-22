# ScheduCloud

A clustered job scheduler with a REST API, written in Go. Jobs say *what* to do (call a URL, publish a Kafka
record), triggers say *when* (every N seconds, or a cron expression). Run as many instances as you like
against one PostgreSQL database: each due trigger fires on exactly one of them.

ScheduCloud was a Spring Boot application on Quartz; this is a rewrite with the same REST paths.

## Quick start

```bash
docker compose up --build       # PostgreSQL + two scheduler instances on ports 8081 and 8082
```

```bash
# A job that calls a URL ...
curl -X POST localhost:8081/api/job-details -d '{
  "key": {"name": "ping", "group": "demo"},
  "jobType": "REST_GET",
  "jobDataMap": {"target": "https://example.com/health"}
}'

# ... every 30 seconds
curl -X POST localhost:8081/api/triggers -d '{
  "key": {"name": "every-30s", "group": "demo"},
  "jobKey": {"name": "ping", "group": "demo"},
  "triggerType": "SIMPLE",
  "simpleSchedule": {"repeatInterval": 30000, "repeatCount": -1}
}'

# ... and at 09:00 on weekdays, Istanbul time
curl -X POST localhost:8082/api/triggers -d '{
  "key": {"name": "weekday-morning", "group": "demo"},
  "jobKey": {"name": "ping", "group": "demo"},
  "triggerType": "CRON",
  "cronSchedule": {"cronExpression": "0 0 9 * * MON-FRI", "timeZone": "Europe/Istanbul"}
}'

curl "localhost:8081/api/job-runs?jobName=ping&jobGroup=demo"   # what happened, and on which instance
```

Without Docker: `STORE=memory go run ./cmd/scheducloud` (nothing is kept across restarts).

## Jobs

| jobType | Data | What it does |
|---|---|---|
| `REST_GET` | `target` (URL), optional `headers` (object), `timeoutSeconds` | GET the URL; status 400 or above fails the run |
| `KAFKA_EVENT` | `topic`, `value` (string or JSON), optional `key` | publish one record; needs `KAFKA_BROKERS` |

The data of a trigger is merged over the data of its job, so one job can serve several triggers with
different targets. Jobs are durable unless `"durable": false`; a non-durable job is deleted with its last trigger.

## Triggers

- **SIMPLE**: fires at `startTime` (now when empty), then every `repeatInterval` milliseconds,
  `repeatCount` more times (`-1` = forever). The fire times stay on that grid.
- **CRON**: a cron expression with an optional seconds field (`0 */5 * * * ?` or `*/5 * * * *`),
  `?` as in Quartz, and descriptors such as `@hourly`. Day-of-week numbers are 0–6 with Sunday as 0
  (Quartz used 1–7), names (`MON-FRI`) work in both. Quartz's `L`, `W` and `#` are not supported.
  `timeZone` is an IANA name, UTC when empty.
- `endTime`, `priority` (higher fires first when several are due, default 5) and `misfirePolicy`:
  when a trigger is more than `MISFIRE_THRESHOLD` late (e.g. every instance was down),
  `FIRE_ONCE_NOW` (default) runs it once and continues from now, `SKIP` waits for the next time.

## API

| Method and path | |
|---|---|
| `GET /api/job-details/all` | all jobs |
| `GET /api/job-details?name=&group=` | one job |
| `POST /api/job-details`, `PUT /api/job-details` | create (201, 409 if it exists), replace |
| `DELETE /api/job-details?name=&group=` | delete the job and its triggers |
| `GET /api/triggers/all` | all triggers |
| `GET /api/triggers?name=&group=` | one trigger, with `nextFireTime` and `finalFireTime` |
| `POST /api/triggers`, `PUT /api/triggers` | create (the job must exist), replace the schedule |
| `DELETE /api/triggers?name=&group=` | delete; answers `true` or `false` |
| `GET /api/triggers/state?name=&group=` | `NORMAL`, `PAUSED`, `COMPLETE`, `ERROR` or `NONE` |
| `POST /api/triggers/pause?name=&group=`, `/resume` | 204 |
| `GET /api/triggers/job?jobName=&jobGroup=` | the triggers of a job |
| `GET /api/job-runs?jobName=&jobGroup=&limit=` | run history, newest first |
| `GET /healthz`, `GET /readyz` | liveness, and readiness (database reachable) |

The group defaults to `DEFAULT`. Errors are `{"error": "..."}` with 400, 404 or 409.
When `API_TOKEN` is set, `/api` needs `Authorization: Bearer <token>`.

**Changes for clients of the Java version:** a missing trigger or job is `404` (it was `204`);
`jobType` replaces the Java class name (`jobClass` values ending in `RestGetJob` or `KafkaEventJob`
are still understood); pause and resume answer `204`. The Quartz tables (`qrtz_*`) are not read:
create the jobs and triggers again.

## Configuration

| Variable | Default | |
|---|---|---|
| `DATABASE_URL` | (required) | e.g. `postgres://user:pass@host:5432/scheducloud` |
| `STORE` | `postgres` | `memory` for a quick try |
| `HTTP_ADDR` | `:8080` | |
| `INSTANCE_ID` | host name and process id | written into every run |
| `POLL_INTERVAL` | `1s` | how often due triggers are claimed |
| `WORKERS` | `10` | jobs running at the same time on one instance |
| `MISFIRE_THRESHOLD` | `60s` | |
| `REST_TIMEOUT` | `30s` | default timeout of `REST_GET` |
| `KAFKA_BROKERS` | | comma separated |
| `API_TOKEN` | | bearer token for `/api` |
| `RUN_RETENTION` | `168h` | older runs are deleted; `0` keeps them |
| `LOG_LEVEL` | `info` | logs are JSON on stdout |

The schema is created and migrated at startup (an advisory lock keeps two instances from doing it at once).

## How the clustering works

Every instance polls for due triggers with `SELECT ... FOR UPDATE SKIP LOCKED` inside a transaction,
moves each claimed trigger to its next fire time and commits before running the job. A row locked by one
instance is skipped by the others, so a fire is never handed out twice. An instance only claims as many
triggers as it has free workers, so a busy instance leaves work to the others.

## Development

```bash
go test ./...            # unit tests; the PostgreSQL and Kafka tests start containers (skipped without Docker)
go test -short ./...     # without containers
go vet ./... && gofmt -l .
```

On Docker Desktop for Windows, set `TESTCONTAINERS_RYUK_DISABLED=true` if the Testcontainers reaper fails to start.

## License

Copyright 2025 Mehmet ERGULCU. Licensed under the [Apache License 2.0](LICENSE).
