// Copyright 2025 Mehmet ERGULCU
// SPDX-License-Identifier: Apache-2.0

package jobs

import (
	"context"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	tckafka "github.com/testcontainers/testcontainers-go/modules/kafka"
	"github.com/twmb/franz-go/pkg/kgo"
)

// TestKafkaProducerAgainstABroker publishes through franz-go into a real broker and reads the record back.
func TestKafkaProducerAgainstABroker(t *testing.T) {
	if testing.Short() {
		t.Skip("Kafka test is skipped with -short")
	}
	testcontainers.SkipIfProviderIsNotHealthy(t)
	ctx := context.Background()
	container, err := tckafka.Run(ctx, "confluentinc/confluent-local:7.9.11", tckafka.WithClusterID("scheducloud-test"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = testcontainers.TerminateContainer(container) })
	brokers, err := container.Brokers(ctx)
	if err != nil {
		t.Fatal(err)
	}

	producer, err := NewKafkaProducer(brokers[0])
	if err != nil {
		t.Fatal(err)
	}
	defer producer.Close()
	message, err := KafkaEvent{Producer: producer}.Execute(ctx, map[string]any{
		"topic": "scheduled-events", "key": "k1", "value": map[string]any{"hello": "kafka"}})
	if err != nil {
		t.Fatal(err)
	}
	t.Log(message)

	consumer, err := kgo.NewClient(kgo.SeedBrokers(brokers...), kgo.ConsumeTopics("scheduled-events"),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
	if err != nil {
		t.Fatal(err)
	}
	defer consumer.Close()
	pollCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	fetches := consumer.PollFetches(pollCtx)
	if err := fetches.Err(); err != nil {
		t.Fatal(err)
	}
	records := fetches.Records()
	if len(records) != 1 || string(records[0].Key) != "k1" || string(records[0].Value) != `{"hello":"kafka"}` {
		t.Fatalf("records = %+v", records)
	}
}
