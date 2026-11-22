// Copyright 2025 Mehmet ERGULCU
// SPDX-License-Identifier: Apache-2.0

package config

import (
	"log/slog"
	"strings"
	"testing"
	"time"
)

func env(values map[string]string) func(string) (string, bool) {
	return func(name string) (string, bool) {
		v, ok := values[name]
		return v, ok
	}
}

func TestDefaults(t *testing.T) {
	c, err := FromEnv(env(map[string]string{"DATABASE_URL": "postgres://db/x"}))
	if err != nil {
		t.Fatal(err)
	}
	if c.HTTPAddr != ":8080" || c.Store != "postgres" || c.PollInterval != time.Second || c.Workers != 10 ||
		c.MisfireThreshold != time.Minute || c.RunRetention != 168*time.Hour || c.LogLevel != slog.LevelInfo ||
		c.InstanceID == "" {
		t.Errorf("defaults = %+v", c)
	}
}

func TestValues(t *testing.T) {
	c, err := FromEnv(env(map[string]string{"STORE": "memory", "WORKERS": "3", "POLL_INTERVAL": "250ms",
		"LOG_LEVEL": "debug", "INSTANCE_ID": "node-a", "API_TOKEN": " t "}))
	if err != nil {
		t.Fatal(err)
	}
	if c.Workers != 3 || c.PollInterval != 250*time.Millisecond || c.LogLevel != slog.LevelDebug ||
		c.InstanceID != "node-a" || c.APIToken != "t" {
		t.Errorf("config = %+v", c)
	}
}

func TestErrorsAreCollected(t *testing.T) {
	_, err := FromEnv(env(map[string]string{"WORKERS": "0", "POLL_INTERVAL": "soon", "LOG_LEVEL": "loud"}))
	if err == nil {
		t.Fatal("expected an error")
	}
	for _, part := range []string{"DATABASE_URL", "WORKERS", "POLL_INTERVAL", "LOG_LEVEL"} {
		if !strings.Contains(err.Error(), part) {
			t.Errorf("error %q does not mention %s", err, part)
		}
	}
}
