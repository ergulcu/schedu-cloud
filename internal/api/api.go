// Copyright 2025 Mehmet ERGULCU
// SPDX-License-Identifier: Apache-2.0

// Package api is the REST interface. The paths are those of the Java version, so existing clients keep
// working; "not found" is now 404 (it was 204) and errors carry a JSON body.
package api

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/ergulcu/schedu-cloud/internal/jobs"
	"github.com/ergulcu/schedu-cloud/internal/model"
	"github.com/ergulcu/schedu-cloud/internal/schedule"
	"github.com/ergulcu/schedu-cloud/internal/store"
)

// Server serves the API.
type Server struct {
	store     store.Store
	executors jobs.Registry
	token     string
	log       *slog.Logger
	now       func() time.Time
}

// New returns the API. When token is not empty every /api request needs "Authorization: Bearer <token>".
func New(s store.Store, executors jobs.Registry, token string, log *slog.Logger) *Server {
	return &Server{store: s, executors: executors, token: token, log: log,
		now: func() time.Time { return time.Now().UTC() }}
}

// Handler returns the routes.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusOK) })
	mux.HandleFunc("GET /readyz", s.ready)

	api := http.NewServeMux()
	api.HandleFunc("GET /api/job-details/all", s.listJobs)
	api.HandleFunc("GET /api/job-details", s.getJob)
	api.HandleFunc("POST /api/job-details", s.createJob)
	api.HandleFunc("PUT /api/job-details", s.updateJob)
	api.HandleFunc("DELETE /api/job-details", s.deleteJob)

	api.HandleFunc("GET /api/triggers/all", s.listTriggers)
	api.HandleFunc("GET /api/triggers", s.getTrigger)
	api.HandleFunc("POST /api/triggers", s.createTrigger)
	api.HandleFunc("PUT /api/triggers", s.updateTrigger)
	api.HandleFunc("DELETE /api/triggers", s.deleteTrigger)
	api.HandleFunc("GET /api/triggers/state", s.triggerState)
	api.HandleFunc("POST /api/triggers/pause", s.pauseTrigger)
	api.HandleFunc("POST /api/triggers/resume", s.resumeTrigger)
	api.HandleFunc("GET /api/triggers/job", s.triggersOfJob)

	api.HandleFunc("GET /api/job-runs", s.listRuns)

	mux.Handle("/api/", s.authenticate(api))
	return mux
}

func (s *Server) authenticate(next http.Handler) http.Handler {
	if s.token == "" {
		return next
	}
	want := []byte("Bearer " + s.token)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if subtle.ConstantTimeCompare([]byte(r.Header.Get("Authorization")), want) != 1 {
			w.Header().Set("WWW-Authenticate", `Bearer realm="scheducloud"`)
			writeError(w, http.StatusUnauthorized, "missing or wrong bearer token")
			return
		}
		next.ServeHTTP(w, r)
	})
}

func (s *Server) ready(w http.ResponseWriter, r *http.Request) {
	if err := s.store.Ping(r.Context()); err != nil {
		writeError(w, http.StatusServiceUnavailable, "store is not reachable")
		return
	}
	w.WriteHeader(http.StatusOK)
}

// ---- jobs ----

// jobRequest is the body of POST and PUT /api/job-details. jobClass is accepted for clients of the Java
// version and mapped to the matching jobType.
type jobRequest struct {
	Key         model.Key      `json:"key"`
	Description string         `json:"description"`
	Durable     *bool          `json:"durable"`
	JobType     model.JobType  `json:"jobType"`
	JobClass    string         `json:"jobClass"`
	JobDataMap  map[string]any `json:"jobDataMap"`
}

func (req jobRequest) job() model.Job {
	jobType := req.JobType
	if jobType == "" {
		switch {
		case strings.HasSuffix(req.JobClass, "RestGetJob"):
			jobType = model.JobRestGet
		case strings.HasSuffix(req.JobClass, "KafkaEventJob"):
			jobType = model.JobKafkaEvent
		}
	}
	durable := true // as in the Java version
	if req.Durable != nil {
		durable = *req.Durable
	}
	return model.Job{Key: req.Key.Normalized(), Description: req.Description, Durable: durable, Type: jobType,
		Data: req.JobDataMap}
}

func (s *Server) validateJob(job *model.Job) error {
	if err := job.Validate(); err != nil {
		return err
	}
	if executor, ok := s.executors[job.Type]; ok {
		return executor.Validate(job.Data)
	}
	return nil
}

func (s *Server) listJobs(w http.ResponseWriter, r *http.Request) {
	list, err := s.store.ListJobs(r.Context())
	s.respond(w, http.StatusOK, nonNil(list), err)
}

func (s *Server) getJob(w http.ResponseWriter, r *http.Request) {
	key, err := keyFromQuery(r, "name", "group", "job")
	if err != nil {
		s.fail(w, err)
		return
	}
	job, err := s.store.GetJob(r.Context(), key)
	s.respond(w, http.StatusOK, job, err)
}

func (s *Server) createJob(w http.ResponseWriter, r *http.Request) {
	var req jobRequest
	if !decode(w, r, &req) {
		return
	}
	job := req.job()
	if err := s.validateJob(&job); err != nil {
		s.fail(w, err)
		return
	}
	if !job.Durable {
		// A job without triggers that is not durable would be deleted at once (Quartz refuses it too).
		s.fail(w, model.Invalidf("a job created without a trigger must be durable"))
		return
	}
	err := s.store.CreateJob(r.Context(), &job)
	s.respond(w, http.StatusCreated, job, err)
}

func (s *Server) updateJob(w http.ResponseWriter, r *http.Request) {
	var req jobRequest
	if !decode(w, r, &req) {
		return
	}
	job := req.job()
	if err := s.validateJob(&job); err != nil {
		s.fail(w, err)
		return
	}
	err := s.store.UpdateJob(r.Context(), &job)
	s.respond(w, http.StatusOK, job, err)
}

func (s *Server) deleteJob(w http.ResponseWriter, r *http.Request) {
	key, err := keyFromQuery(r, "name", "group", "job")
	if err != nil {
		s.fail(w, err)
		return
	}
	deleted, err := s.store.DeleteJob(r.Context(), key)
	s.respond(w, http.StatusOK, deleted, err)
}

// ---- triggers ----

// triggerResponse adds the computed final fire time to a trigger.
type triggerResponse struct {
	model.Trigger
	FinalFireTime *time.Time `json:"finalFireTime,omitempty"`
}

func respondTrigger(t model.Trigger) triggerResponse {
	return triggerResponse{Trigger: t, FinalFireTime: schedule.FinalFireTime(&t)}
}

func (s *Server) listTriggers(w http.ResponseWriter, r *http.Request) {
	list, err := s.store.ListTriggers(r.Context())
	s.respond(w, http.StatusOK, mapTriggers(list), err)
}

func (s *Server) getTrigger(w http.ResponseWriter, r *http.Request) {
	key, err := keyFromQuery(r, "name", "group", "trigger")
	if err != nil {
		s.fail(w, err)
		return
	}
	t, err := s.store.GetTrigger(r.Context(), key)
	s.respond(w, http.StatusOK, respondTrigger(t), err)
}

func (s *Server) createTrigger(w http.ResponseWriter, r *http.Request) {
	t, ok := s.decodeTrigger(w, r)
	if !ok {
		return
	}
	err := s.store.CreateTrigger(r.Context(), &t)
	s.respond(w, http.StatusCreated, respondTrigger(t), err)
}

func (s *Server) updateTrigger(w http.ResponseWriter, r *http.Request) {
	t, ok := s.decodeTrigger(w, r)
	if !ok {
		return
	}
	err := s.store.ReplaceTrigger(r.Context(), &t)
	s.respond(w, http.StatusOK, respondTrigger(t), err)
}

func (s *Server) decodeTrigger(w http.ResponseWriter, r *http.Request) (model.Trigger, bool) {
	var t model.Trigger
	if !decode(w, r, &t) {
		return t, false
	}
	// Values the scheduler keeps are not taken from the client.
	t.NextFireTime, t.PreviousFireTime, t.TimesTriggered = nil, nil, 0
	if err := schedule.Prepare(&t, s.now()); err != nil {
		s.fail(w, err)
		return t, false
	}
	return t, true
}

func (s *Server) deleteTrigger(w http.ResponseWriter, r *http.Request) {
	key, err := keyFromQuery(r, "name", "group", "trigger")
	if err != nil {
		s.fail(w, err)
		return
	}
	deleted, err := s.store.DeleteTrigger(r.Context(), key)
	s.respond(w, http.StatusOK, deleted, err)
}

func (s *Server) triggerState(w http.ResponseWriter, r *http.Request) {
	key, err := keyFromQuery(r, "name", "group", "trigger")
	if err != nil {
		s.fail(w, err)
		return
	}
	t, err := s.store.GetTrigger(r.Context(), key)
	if errors.Is(err, model.ErrNotFound) {
		s.respond(w, http.StatusOK, model.StateNone, nil)
		return
	}
	s.respond(w, http.StatusOK, t.State, err)
}

func (s *Server) pauseTrigger(w http.ResponseWriter, r *http.Request) {
	s.changeState(w, r, s.store.PauseTrigger)
}

func (s *Server) resumeTrigger(w http.ResponseWriter, r *http.Request) {
	s.changeState(w, r, s.store.ResumeTrigger)
}

func (s *Server) changeState(w http.ResponseWriter, r *http.Request, change func(context.Context, model.Key) error) {
	key, err := keyFromQuery(r, "name", "group", "trigger")
	if err != nil {
		s.fail(w, err)
		return
	}
	if err := change(r.Context(), key); err != nil {
		s.fail(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) triggersOfJob(w http.ResponseWriter, r *http.Request) {
	key, err := keyFromQuery(r, "jobName", "jobGroup", "job")
	if err != nil {
		s.fail(w, err)
		return
	}
	list, err := s.store.ListTriggersOfJob(r.Context(), key)
	s.respond(w, http.StatusOK, mapTriggers(list), err)
}

// ---- runs ----

func (s *Server) listRuns(w http.ResponseWriter, r *http.Request) {
	limit := 50
	if v := r.URL.Query().Get("limit"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 1 || n > 1000 {
			s.fail(w, model.Invalidf("limit must be between 1 and 1000"))
			return
		}
		limit = n
	}
	var jobKey *model.Key
	if r.URL.Query().Get("jobName") != "" {
		key := model.NewKey(r.URL.Query().Get("jobName"), r.URL.Query().Get("jobGroup"))
		jobKey = &key
	}
	runs, err := s.store.ListRuns(r.Context(), jobKey, limit)
	s.respond(w, http.StatusOK, nonNil(runs), err)
}

// ---- helpers ----

const maxBody = 1 << 20

func decode(w http.ResponseWriter, r *http.Request, v any) bool {
	dec := json.NewDecoder(io.LimitReader(r.Body, maxBody))
	if err := dec.Decode(v); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON body: "+err.Error())
		return false
	}
	return true
}

func keyFromQuery(r *http.Request, nameParam, groupParam, what string) (model.Key, error) {
	key := model.NewKey(r.URL.Query().Get(nameParam), r.URL.Query().Get(groupParam))
	if err := key.Validate(what); err != nil {
		return key, err
	}
	return key, nil
}

func (s *Server) respond(w http.ResponseWriter, status int, body any, err error) {
	if err != nil {
		s.fail(w, err)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(body); err != nil {
		s.log.Warn("could not write the response", "error", err)
	}
}

func (s *Server) fail(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, model.ErrInvalid):
		writeError(w, http.StatusBadRequest, err.Error())
	case errors.Is(err, model.ErrNotFound):
		writeError(w, http.StatusNotFound, err.Error())
	case errors.Is(err, model.ErrAlreadyExists):
		writeError(w, http.StatusConflict, err.Error())
	default:
		s.log.Error("request failed", "error", err)
		writeError(w, http.StatusInternalServerError, "internal error")
	}
}

func writeError(w http.ResponseWriter, status int, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(map[string]string{"error": message}) //nolint:errcheck
}

func mapTriggers(list []model.Trigger) []triggerResponse {
	out := make([]triggerResponse, 0, len(list))
	for _, t := range list {
		out = append(out, respondTrigger(t))
	}
	return out
}

func nonNil[T any](list []T) []T {
	if list == nil {
		return []T{}
	}
	return list
}
