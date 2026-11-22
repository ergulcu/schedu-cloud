// Copyright 2025 Mehmet ERGULCU
// SPDX-License-Identifier: Apache-2.0

package jobs

import (
	"context"
	"strings"

	"github.com/twmb/franz-go/pkg/kgo"
)

// KafkaProducer is a Producer backed by franz-go, a pure Go Kafka client.
type KafkaProducer struct {
	client *kgo.Client
}

// NewKafkaProducer connects lazily to the comma separated brokers.
func NewKafkaProducer(brokers string) (*KafkaProducer, error) {
	client, err := kgo.NewClient(
		kgo.SeedBrokers(strings.Split(brokers, ",")...),
		kgo.AllowAutoTopicCreation(),
		kgo.RequiredAcks(kgo.AllISRAcks()),
	)
	if err != nil {
		return nil, err
	}
	return &KafkaProducer{client: client}, nil
}

func (p *KafkaProducer) Produce(ctx context.Context, topic string, key, value []byte) error {
	return p.client.ProduceSync(ctx, &kgo.Record{Topic: topic, Key: key, Value: value}).FirstErr()
}

// Close flushes and closes the client.
func (p *KafkaProducer) Close() { p.client.Close() }
