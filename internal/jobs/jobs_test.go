// Copyright 2025 Mehmet ERGULCU
// SPDX-License-Identifier: Apache-2.0

package jobs

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/ergulcu/schedu-cloud/internal/model"
)

func TestRestGetSucceeds(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.Header.Get("X-Token") != "abc" {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		w.Write([]byte("pong")) //nolint:errcheck
	}))
	defer server.Close()

	message, err := RestGet{}.Execute(context.Background(), map[string]any{
		"target": server.URL, "headers": map[string]any{"X-Token": "abc"}})

	if err != nil {
		t.Fatal(err)
	}
	if message != "200 OK: pong" {
		t.Errorf("message = %q", message)
	}
}

func TestRestGetFailsOnErrorStatus(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, strings.Repeat("x", 2000), http.StatusInternalServerError)
	}))
	defer server.Close()

	message, err := RestGet{}.Execute(context.Background(), map[string]any{"target": server.URL})

	if err == nil {
		t.Fatal("a 500 answer must fail the run")
	}
	if !strings.HasPrefix(message, "500 Internal Server Error") || len(message) > 600 {
		t.Errorf("message = %q (len %d)", message[:40], len(message))
	}
}

func TestRestGetHonoursTheTimeout(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-time.After(5 * time.Second):
		case <-r.Context().Done():
		}
	}))
	defer server.Close()

	start := time.Now()
	_, err := RestGet{DefaultTimeout: 100 * time.Millisecond}.Execute(context.Background(), map[string]any{"target": server.URL})

	if !errors.Is(err, context.DeadlineExceeded) || time.Since(start) > 2*time.Second {
		t.Errorf("err = %v after %v", err, time.Since(start))
	}
}

func TestRestGetNeedsAnHttpTarget(t *testing.T) {
	for _, data := range []map[string]any{{}, {"target": "ftp://files.example"}, {"target": "not a url"}} {
		if _, err := (RestGet{}).Execute(context.Background(), data); err == nil {
			t.Errorf("Execute(%v) succeeded", data)
		}
	}
	if err := (RestGet{}).Validate(map[string]any{"target": 42}); !errors.Is(err, model.ErrInvalid) {
		t.Errorf("Validate(42) = %v", err)
	}
	if err := (RestGet{}).Validate(map[string]any{}); err != nil {
		t.Errorf("the target may come from the trigger: %v", err)
	}
}

type fakeProducer struct {
	topic      string
	key, value []byte
	err        error
}

func (f *fakeProducer) Produce(_ context.Context, topic string, key, value []byte) error {
	f.topic, f.key, f.value = topic, key, value
	return f.err
}

func TestKafkaEventPublishesStringsAndJSON(t *testing.T) {
	producer := &fakeProducer{}
	job := KafkaEvent{Producer: producer}

	if _, err := job.Execute(context.Background(), map[string]any{"topic": "orders", "key": 7, "value": "hello"}); err != nil {
		t.Fatal(err)
	}
	if producer.topic != "orders" || string(producer.key) != "7" || string(producer.value) != "hello" {
		t.Errorf("published %s %s %s", producer.topic, producer.key, producer.value)
	}

	message, err := job.Execute(context.Background(), map[string]any{"topic": "orders",
		"value": map[string]any{"id": 1, "status": "NEW"}})
	if err != nil {
		t.Fatal(err)
	}
	if string(producer.value) != `{"id":1,"status":"NEW"}` || producer.key != nil {
		t.Errorf("value = %s key = %v", producer.value, producer.key)
	}
	if message != "published 23 bytes to orders" {
		t.Errorf("message = %q", message)
	}
}

func TestKafkaEventErrors(t *testing.T) {
	if _, err := (KafkaEvent{}).Execute(context.Background(), map[string]any{"topic": "x"}); err == nil ||
		!strings.Contains(err.Error(), "KAFKA_BROKERS") {
		t.Errorf("without a producer: %v", err)
	}
	if _, err := (KafkaEvent{Producer: &fakeProducer{}}).Execute(context.Background(), map[string]any{}); err == nil {
		t.Error("a missing topic must fail")
	}
	broken := &fakeProducer{err: errors.New("broker down")}
	if _, err := (KafkaEvent{Producer: broken}).Execute(context.Background(), map[string]any{"topic": "x"}); err == nil ||
		!strings.Contains(err.Error(), "broker down") {
		t.Errorf("producer error: %v", err)
	}
	if err := (KafkaEvent{}).Validate(map[string]any{"topic": ""}); !errors.Is(err, model.ErrInvalid) {
		t.Errorf("Validate empty topic = %v", err)
	}
}
