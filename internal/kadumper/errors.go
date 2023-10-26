// Copyright 2023 Hugo Hromic
// SPDX-License-Identifier: Apache-2.0

package kadumper

import "errors"

// Errors used by the kadumper package.
var (
	// ErrKafkaClientClosed is returned when a Kafka client is closed.
	ErrKafkaClientClosed = errors.New("kafka client closed")
)
