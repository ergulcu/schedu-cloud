// Copyright 2025 Mehmet ERGULCU
// SPDX-License-Identifier: Apache-2.0

// Package jobs runs the work behind each job type.
package jobs

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/ergulcu/schedu-cloud/internal/model"
)

// Executor runs one job type. The data is the job's data overlaid with the firing trigger's data.
// The returned text is kept as the run's message.
type Executor interface {
	Execute(ctx context.Context, data map[string]any) (string, error)
	// Validate checks the data a job is created with. Entries that may still come from a trigger can be missing.
	Validate(data map[string]any) error
}

// Registry maps job types to their executors.
type Registry map[model.JobType]Executor

// ---- REST_GET ----

// RestGet sends GET to data["target"]. A response with status 400 or above counts as a failure.
// Optional entries: "headers" (object of header values) and "timeoutSeconds".
type RestGet struct {
	Client *http.Client
	// DefaultTimeout applies when the data has no timeoutSeconds.
	DefaultTimeout time.Duration
}

const maxBodyInMessage = 500

func (r RestGet) Validate(data map[string]any) error {
	target, ok := data["target"]
	if !ok {
		return nil
	}
	s, ok := target.(string)
	if !ok {
		return model.Invalidf("target must be a URL string")
	}
	return checkURL(s)
}

func (r RestGet) Execute(ctx context.Context, data map[string]any) (string, error) {
	target, _ := data["target"].(string)
	if target == "" {
		return "", errors.New(`REST_GET needs a "target" URL in the job data`)
	}
	if err := checkURL(target); err != nil {
		return "", err
	}
	timeout := r.DefaultTimeout
	if seconds, ok := data["timeoutSeconds"].(float64); ok && seconds > 0 {
		timeout = time.Duration(seconds * float64(time.Second))
	}
	if timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, target, nil)
	if err != nil {
		return "", err
	}
	req.Header.Set("User-Agent", "scheducloud")
	if headers, ok := data["headers"].(map[string]any); ok {
		for name, value := range headers {
			req.Header.Set(name, fmt.Sprint(value))
		}
	}
	client := r.Client
	if client == nil {
		client = http.DefaultClient
	}
	resp, err := client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(io.LimitReader(resp.Body, maxBodyInMessage+1))
	message := resp.Status
	if text := strings.TrimSpace(string(body)); text != "" {
		if len(text) > maxBodyInMessage {
			text = text[:maxBodyInMessage] + "…"
		}
		message += ": " + text
	}
	if resp.StatusCode >= 400 {
		return message, fmt.Errorf("GET %s answered %s", target, resp.Status)
	}
	return message, nil
}

func checkURL(s string) error {
	u, err := url.Parse(s)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		return model.Invalidf("target %q is not an http or https URL", s)
	}
	return nil
}

// ---- KAFKA_EVENT ----

// Producer publishes one record and waits for the broker's acknowledgement.
type Producer interface {
	Produce(ctx context.Context, topic string, key, value []byte) error
}

// KafkaEvent publishes data["value"] to data["topic"] with the optional data["key"]. A value that is not a
// string is sent as JSON. In the Java version this job type existed but did nothing.
type KafkaEvent struct {
	// Producer is nil when no brokers are configured; the job then fails with a clear message.
	Producer Producer
}

func (k KafkaEvent) Validate(data map[string]any) error {
	if topic, ok := data["topic"]; ok {
		if s, ok := topic.(string); !ok || s == "" {
			return model.Invalidf("topic must be a non-empty string")
		}
	}
	return nil
}

func (k KafkaEvent) Execute(ctx context.Context, data map[string]any) (string, error) {
	if k.Producer == nil {
		return "", errors.New("KAFKA_EVENT needs Kafka; set KAFKA_BROKERS")
	}
	topic, _ := data["topic"].(string)
	if topic == "" {
		return "", errors.New(`KAFKA_EVENT needs a "topic" in the job data`)
	}
	value, err := encode(data["value"])
	if err != nil {
		return "", err
	}
	var key []byte
	if data["key"] != nil {
		key = []byte(fmt.Sprint(data["key"]))
	}
	if err := k.Producer.Produce(ctx, topic, key, value); err != nil {
		return "", fmt.Errorf("publish to %s: %w", topic, err)
	}
	return fmt.Sprintf("published %d bytes to %s", len(value), topic), nil
}

func encode(value any) ([]byte, error) {
	switch v := value.(type) {
	case nil:
		return nil, nil
	case string:
		return []byte(v), nil
	default:
		b, err := json.Marshal(v)
		if err != nil {
			return nil, fmt.Errorf("encode value: %w", err)
		}
		return b, nil
	}
}
