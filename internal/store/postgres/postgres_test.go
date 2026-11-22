// Copyright 2025 Mehmet ERGULCU
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"

	"github.com/ergulcu/schedu-cloud/internal/model"
	"github.com/ergulcu/schedu-cloud/internal/store"
	"github.com/ergulcu/schedu-cloud/internal/store/storetest"
)

// The tests need a PostgreSQL server. SCHEDUCLOUD_TEST_POSTGRES points at one (CI uses a service container);
// otherwise one is started with Testcontainers, and without Docker the tests are skipped.
var (
	serverOnce sync.Once
	serverURL  string
	serverErr  error
	container  *tcpostgres.PostgresContainer
)

// TestMain stops the container itself, so nothing is left behind when Ryuk (the Testcontainers reaper)
// is turned off with TESTCONTAINERS_RYUK_DISABLED=true.
func TestMain(m *testing.M) {
	code := m.Run()
	if container != nil {
		_ = testcontainers.TerminateContainer(container)
	}
	os.Exit(code)
}

func adminURL(t *testing.T) string {
	t.Helper()
	if url := os.Getenv("SCHEDUCLOUD_TEST_POSTGRES"); url != "" {
		return url
	}
	if testing.Short() {
		t.Skip("PostgreSQL tests are skipped with -short")
	}
	testcontainers.SkipIfProviderIsNotHealthy(t)
	serverOnce.Do(func() {
		ctx := context.Background()
		var err error
		container, err = tcpostgres.Run(ctx, "postgres:18-alpine",
			tcpostgres.WithDatabase("scheducloud"), tcpostgres.WithUsername("test"), tcpostgres.WithPassword("test"),
			tcpostgres.BasicWaitStrategies())
		if err != nil {
			serverErr = err
			return
		}
		serverURL, serverErr = container.ConnectionString(ctx, "sslmode=disable")
	})
	if serverErr != nil {
		t.Fatalf("start PostgreSQL: %v", serverErr)
	}
	return serverURL
}

var dbCounter int
var dbMu sync.Mutex

// newStore creates a fresh database for every test so the tests do not see each other's rows.
func newStore(t *testing.T) *Store {
	t.Helper()
	ctx := context.Background()
	admin := adminURL(t)
	dbMu.Lock()
	dbCounter++
	name := fmt.Sprintf("scheducloud_test_%d_%d", os.Getpid(), dbCounter)
	dbMu.Unlock()

	conn, err := pgx.Connect(ctx, admin)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close(ctx)
	if _, err := conn.Exec(ctx, "CREATE DATABASE "+name); err != nil {
		t.Fatal(err)
	}
	cfg, err := pgx.ParseConfig(admin)
	if err != nil {
		t.Fatal(err)
	}
	url := fmt.Sprintf("postgres://%s:%s@%s:%d/%s?sslmode=disable", cfg.User, cfg.Password, cfg.Host, cfg.Port, name)
	s, err := Open(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		s.Close()
		c, err := pgx.Connect(context.Background(), admin)
		if err == nil {
			c.Exec(context.Background(), "DROP DATABASE IF EXISTS "+name+" WITH (FORCE)") //nolint:errcheck
			c.Close(context.Background())
		}
	})
	return s
}

func TestStore(t *testing.T) {
	storetest.Run(t, func(t *testing.T) store.Store { return newStore(t) })
}

func TestMigrationsRunOnce(t *testing.T) {
	s := newStore(t)
	// Opening the same database again finds the schema in place.
	if err := s.migrate(context.Background()); err != nil {
		t.Fatalf("second migration: %v", err)
	}
	var count int
	if err := s.pool.QueryRow(context.Background(), "SELECT count(*) FROM schema_migrations").Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Errorf("schema_migrations has %d rows, want 1", count)
	}
}

// TestConcurrentInstancesFireEachTriggerOnce plays several scheduler instances polling one database
// at the same moment. Every due trigger must be handed to exactly one of them.
func TestConcurrentInstancesFireEachTriggerOnce(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	start := time.Date(2026, 3, 10, 10, 0, 0, 0, time.UTC)
	job := storetest.Job("ping")
	if err := s.CreateJob(ctx, job); err != nil {
		t.Fatal(err)
	}
	const triggers = 50
	for i := range triggers {
		if err := s.CreateTrigger(ctx, storetest.Trigger(t, fmt.Sprintf("t%02d", i), job.Key, start)); err != nil {
			t.Fatal(err)
		}
	}

	var mu sync.Mutex
	seen := map[model.Key]int{}
	var wg sync.WaitGroup
	for range 5 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				fires, err := s.AcquireDue(ctx, start.Add(time.Second), 7, storetest.Advance)
				if err != nil {
					t.Error(err)
					return
				}
				if len(fires) == 0 {
					return
				}
				mu.Lock()
				for _, f := range fires {
					seen[f.Trigger.Key]++
				}
				mu.Unlock()
			}
		}()
	}
	wg.Wait()

	if len(seen) != triggers {
		t.Errorf("%d triggers fired, want %d", len(seen), triggers)
	}
	for key, n := range seen {
		if n != 1 {
			t.Errorf("trigger %s fired %d times", key, n)
		}
	}
}
