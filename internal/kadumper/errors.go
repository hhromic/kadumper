// SPDX-FileCopyrightText: Copyright 2023 Hugo Hromic
// SPDX-License-Identifier: Apache-2.0

package kadumper

import "errors"

// Errors used by the kadumper package.
var (
	// ErrUnknownDumperName is returned when an unknown Kafka records dumper name is used.
	ErrUnknownDumperName = errors.New("unknown dumper name")

	// ErrKafkaClientClosed is returned when a Kafka client is closed.
	ErrKafkaClientClosed = errors.New("kafka client closed")
)
