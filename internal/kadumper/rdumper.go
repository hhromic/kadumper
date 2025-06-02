// SPDX-FileCopyrightText: Copyright 2023 Hugo Hromic
// SPDX-License-Identifier: Apache-2.0

package kadumper

import (
	"bufio"
	"context"
	"fmt"
	"os"

	"github.com/twmb/franz-go/pkg/kgo"
)

// Dumper represents a supported Kafka records dumper.
type Dumper int

// Supported Kafka records dumper.
const (
	// DumperStdout is a Kafka records dumper which outputs records to stdout.
	DumperStdout Dumper = iota
)

// String returns a name for the Kafka records dumper.
func (d Dumper) String() string {
	switch d {
	case DumperStdout:
		return "stdout"
	default:
		return ""
	}
}

// MarshalText implements [encoding.TextMarshaler] by calling [Dumper.String].
func (d Dumper) MarshalText() ([]byte, error) {
	return []byte(d.String()), nil
}

// UnmarshalText implements [encoding.TextUnmarshaler].
// It accepts any string produced by [Dumper.MarshalText].
func (d *Dumper) UnmarshalText(b []byte) error {
	str := string(b)
	switch str {
	case DumperStdout.String():
		*d = DumperStdout
	default:
		return fmt.Errorf("%w: %q", ErrUnknownDumperName, str)
	}

	return nil
}

// RecordDumper is a Kafka records dumper.
type RecordDumper interface {
	DumpRecord(ctx context.Context, record *kgo.Record) error
}

// NewStdoutRecordDumper creates a new Kafka records dumper which outputs records to stdout.
func NewStdoutRecordDumper() *WriterRecordDumper {
	return &WriterRecordDumper{ //nolint:exhaustruct
		Writer: bufio.NewWriter(os.Stdout),
	}
}
