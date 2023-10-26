// Copyright 2023 Hugo Hromic
// SPDX-License-Identifier: Apache-2.0

package kadumper

import (
	"crypto/tls"
	"fmt"
	"log/slog"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/plugin/kslog"
)

// NewKafkaConsumerClient returns a new Kafka consumer client.
func NewKafkaConsumerClient(
	seedBrokers, topics []string,
	consumerGroup string,
	fromBeginning bool,
	tcfg *tls.Config,
	logger *slog.Logger,
) (*kgo.Client, error) {
	opts := []kgo.Opt{
		kgo.WithLogger(kslog.New(logger)),
		kgo.SeedBrokers(seedBrokers...),
		kgo.ConsumeTopics(topics...),
	}

	if consumerGroup != "" {
		opts = append(opts, kgo.ConsumerGroup(consumerGroup))
	}

	if !fromBeginning {
		opts = append(opts, kgo.ConsumeResetOffset(kgo.NewOffset().AtEnd()))
	}

	if tcfg != nil {
		opts = append(opts, kgo.DialTLSConfig(tcfg))
	}

	cl, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, fmt.Errorf("new client: %w", err)
	}

	return cl, nil
}
