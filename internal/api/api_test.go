// Copyright 2025 Mehmet ERGULCU
// SPDX-License-Identifier: Apache-2.0

package api

import (
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/ergulcu/schedu-cloud/internal/jobs"
	"github.com/ergulcu/schedu-cloud/internal/model"
	"github.com/ergulcu/schedu-cloud/internal/store/memory"
)

var now = time.Date(2026, 3, 10, 10, 0, 0, 0, time.UTC)

func newServer(t *testing.T, token string) (*httptest.Server, *memory.Store) {
	t.Helper()
	st := memory.New()
	srv := New(st, jobs.Registry{model.JobRestGet: jobs.RestGet{}, model.JobKafkaEvent: jobs.KafkaEvent{}}, token,
		slog.New(slog.NewTextHandler(io.Discard, nil)))
	srv.now = func() time.Time { return now }
	ts := httptest.NewServer(srv.Handler())
	t.Cleanup(ts.Close)
	return ts, st
}

func call(t *testing.T, ts *httptest.Server, method, path, body string, headers ...string) (int, string) {
	t.Helper()
	req, err := http.NewRequest(method, ts.URL+path, strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	for i := 0; i+1 < len(headers); i += 2 {
		req.Header.Set(headers[i], headers[i+1])
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(b)
}

const restJob = `{"key":{"name":"ping","group":"demo"},"description":"pings","jobType":"REST_GET",
	"jobDataMap":{"target":"https://example.com/health"}}`

const cronTrigger = `{"key":{"name":"every-5-min","group":"demo"},"jobKey":{"name":"ping","group":"demo"},
	"triggerType":"CRON","cronSchedule":{"cronExpression":"0 */5 * * * ?","timeZone":"Europe/Istanbul"}}`

func TestJobLifecycle(t *testing.T) {
	ts, _ := newServer(t, "")

	if status, body := call(t, ts, "POST", "/api/job-details", restJob); status != http.StatusCreated ||
		!strings.Contains(body, `"durable":true`) {
		t.Fatalf("create = %d %s", status, body)
	}
	if status, _ := call(t, ts, "POST", "/api/job-details", restJob); status != http.StatusConflict {
		t.Errorf("duplicate create = %d, want 409", status)
	}
	if status, body := call(t, ts, "GET", "/api/job-details?name=ping&group=demo", ""); status != http.StatusOK ||
		!strings.Contains(body, "https://example.com/health") {
		t.Errorf("get = %d %s", status, body)
	}
	if status, _ := call(t, ts, "GET", "/api/job-details?name=nope", ""); status != http.StatusNotFound {
		t.Errorf("get missing = %d, want 404", status)
	}
	updated := strings.Replace(restJob, `"pings"`, `"pings twice"`, 1)
	if status, body := call(t, ts, "PUT", "/api/job-details", updated); status != http.StatusOK ||
		!strings.Contains(body, "pings twice") {
		t.Errorf("update = %d %s", status, body)
	}
	if status, body := call(t, ts, "GET", "/api/job-details/all", ""); status != http.StatusOK ||
		!strings.HasPrefix(body, `[{"key":{"name":"ping"`) {
		t.Errorf("list = %d %s", status, body)
	}
	if status, body := call(t, ts, "DELETE", "/api/job-details?name=ping&group=demo", ""); status != http.StatusOK ||
		strings.TrimSpace(body) != "true" {
		t.Errorf("delete = %d %s", status, body)
	}
}

func TestJobValidation(t *testing.T) {
	ts, _ := newServer(t, "")
	cases := map[string]string{
		"no name":      `{"key":{"name":""},"jobType":"REST_GET"}`,
		"unknown type": `{"key":{"name":"x"},"jobType":"SEND_FAX"}`,
		"bad target":   `{"key":{"name":"x"},"jobType":"REST_GET","jobDataMap":{"target":"ftp://x"}}`,
		"not durable":  `{"key":{"name":"x"},"jobType":"REST_GET","durable":false}`,
		"not json":     `{"key":`,
		"empty topic":  `{"key":{"name":"x"},"jobType":"KAFKA_EVENT","jobDataMap":{"topic":""}}`,
	}
	for name, body := range cases {
		if status, resp := call(t, ts, "POST", "/api/job-details", body); status != http.StatusBadRequest ||
			!strings.Contains(resp, `"error"`) {
			t.Errorf("%s: %d %s", name, status, resp)
		}
	}
}

func TestJavaJobClassNamesStillWork(t *testing.T) {
	ts, _ := newServer(t, "")
	status, body := call(t, ts, "POST", "/api/job-details",
		`{"key":{"name":"legacy"},"jobClass":"tr.com.idias.scheducloud.job.RestGetJob","jobDataMap":{"target":"http://x.test"}}`)
	if status != http.StatusCreated || !strings.Contains(body, `"jobType":"REST_GET"`) ||
		!strings.Contains(body, `"group":"DEFAULT"`) {
		t.Errorf("legacy create = %d %s", status, body)
	}
}

func TestTriggerLifecycle(t *testing.T) {
	ts, _ := newServer(t, "")
	call(t, ts, "POST", "/api/job-details", restJob)

	status, body := call(t, ts, "POST", "/api/triggers", cronTrigger)
	if status != http.StatusCreated {
		t.Fatalf("create = %d %s", status, body)
	}
	var created struct {
		NextFireTime time.Time `json:"nextFireTime"`
		State        string    `json:"state"`
	}
	json.Unmarshal([]byte(body), &created)                             //nolint:errcheck
	if !created.NextFireTime.Equal(now) || created.State != "NORMAL" { // 10:00 matches */5 itself
		t.Errorf("created = %+v", created)
	}
	if status, _ := call(t, ts, "POST", "/api/triggers", cronTrigger); status != http.StatusConflict {
		t.Errorf("duplicate = %d, want 409", status)
	}

	if status, body := call(t, ts, "GET", "/api/triggers/state?name=every-5-min&group=demo", ""); status != http.StatusOK ||
		strings.TrimSpace(body) != `"NORMAL"` {
		t.Errorf("state = %d %s", status, body)
	}
	if status, _ := call(t, ts, "POST", "/api/triggers/pause?name=every-5-min&group=demo", ""); status != http.StatusNoContent {
		t.Errorf("pause = %d", status)
	}
	if _, body := call(t, ts, "GET", "/api/triggers/state?name=every-5-min&group=demo", ""); strings.TrimSpace(body) != `"PAUSED"` {
		t.Errorf("state after pause = %s", body)
	}
	call(t, ts, "POST", "/api/triggers/resume?name=every-5-min&group=demo", "")

	simple := `{"key":{"name":"every-5-min","group":"demo"},"jobKey":{"name":"ping","group":"demo"},
		"triggerType":"SIMPLE","simpleSchedule":{"repeatInterval":60000,"repeatCount":4}}`
	status, body = call(t, ts, "PUT", "/api/triggers", simple)
	if status != http.StatusOK || !strings.Contains(body, `"finalFireTime":"2026-03-10T10:04:00Z"`) ||
		strings.Contains(body, "cronSchedule") {
		t.Errorf("update = %d %s", status, body)
	}
	if status, body := call(t, ts, "GET", "/api/triggers/job?jobName=ping&jobGroup=demo", ""); status != http.StatusOK ||
		!strings.Contains(body, `"triggerType":"SIMPLE"`) {
		t.Errorf("triggers of job = %d %s", status, body)
	}
	if status, body := call(t, ts, "GET", "/api/triggers/all", ""); status != http.StatusOK || !strings.HasPrefix(body, "[") {
		t.Errorf("all = %d %s", status, body)
	}
	if _, body := call(t, ts, "DELETE", "/api/triggers?name=every-5-min&group=demo", ""); strings.TrimSpace(body) != "true" {
		t.Errorf("delete = %s", body)
	}
	if _, body := call(t, ts, "GET", "/api/triggers/state?name=every-5-min&group=demo", ""); strings.TrimSpace(body) != `"NONE"` {
		t.Errorf("state of deleted = %s", body)
	}
	if status, _ := call(t, ts, "GET", "/api/triggers?name=every-5-min&group=demo", ""); status != http.StatusNotFound {
		t.Errorf("get deleted = %d, want 404 (the Java version answered 204)", status)
	}
}

func TestTriggerErrors(t *testing.T) {
	ts, _ := newServer(t, "")
	if status, _ := call(t, ts, "POST", "/api/triggers", cronTrigger); status != http.StatusNotFound {
		t.Errorf("trigger for a missing job = %d, want 404", status)
	}
	call(t, ts, "POST", "/api/job-details", restJob)
	bad := strings.Replace(cronTrigger, "0 */5 * * * ?", "every five minutes", 1)
	if status, body := call(t, ts, "POST", "/api/triggers", bad); status != http.StatusBadRequest ||
		!strings.Contains(body, "cron expression") {
		t.Errorf("bad cron = %d %s", status, body)
	}
	if status, _ := call(t, ts, "POST", "/api/triggers/pause?name=missing", ""); status != http.StatusNotFound {
		t.Errorf("pause missing = %d", status)
	}
	if status, _ := call(t, ts, "PUT", "/api/triggers", cronTrigger); status != http.StatusNotFound {
		t.Errorf("update missing = %d", status)
	}
}

func TestRuns(t *testing.T) {
	ts, st := newServer(t, "")
	st.RecordRun(t.Context(), &model.Run{JobKey: model.NewKey("ping", "demo"), FireTime: now, Succeeded: true}) //nolint:errcheck
	st.RecordRun(t.Context(), &model.Run{JobKey: model.NewKey("other", ""), FireTime: now})                     //nolint:errcheck

	if _, body := call(t, ts, "GET", "/api/job-runs?jobName=ping&jobGroup=demo", ""); strings.Count(body, `"id"`) != 1 {
		t.Errorf("runs of ping = %s", body)
	}
	if _, body := call(t, ts, "GET", "/api/job-runs?limit=10", ""); strings.Count(body, `"id"`) != 2 {
		t.Errorf("all runs = %s", body)
	}
	if status, _ := call(t, ts, "GET", "/api/job-runs?limit=0", ""); status != http.StatusBadRequest {
		t.Errorf("limit 0 = %d", status)
	}
}

func TestTokenProtectsTheAPIButNotTheHealthChecks(t *testing.T) {
	ts, _ := newServer(t, "s3cret")

	if status, _ := call(t, ts, "GET", "/api/job-details/all", ""); status != http.StatusUnauthorized {
		t.Errorf("without token = %d", status)
	}
	if status, _ := call(t, ts, "GET", "/api/job-details/all", "", "Authorization", "Bearer wrong"); status != http.StatusUnauthorized {
		t.Errorf("wrong token = %d", status)
	}
	if status, _ := call(t, ts, "GET", "/api/job-details/all", "", "Authorization", "Bearer s3cret"); status != http.StatusOK {
		t.Errorf("right token = %d", status)
	}
	for _, path := range []string{"/healthz", "/readyz"} {
		if status, _ := call(t, ts, "GET", path, ""); status != http.StatusOK {
			t.Errorf("%s = %d", path, status)
		}
	}
}
