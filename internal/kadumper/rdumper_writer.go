// SPDX-FileCopyrightText: Copyright 2023 Hugo Hromic
// SPDX-License-Identifier: Apache-2.0

package kadumper

import (
	"bufio"
	"context"
	"fmt"

	"github.com/twmb/franz-go/pkg/kgo"
)

// WriterRecordDumper dumps Kafka records to a buffered writer.
type WriterRecordDumper struct {
	// Deserializer is the deserializer to use for Kafka record keys and values.
	Deserializer Deserializer
	// DumpTimestamp is whether to dump Kafka record timestamps.
	DumpTimestamp bool
	// DumpPartition is whether to dump Kafka record partition numbers.
	DumpPartition bool
	// DumpOffset is whether to dump Kafka record partition offsets.
	DumpOffset bool
	// DumpKey is whether to dump Kafka record keys.
	DumpKey bool
	// Writer is the buffered writer to use for dumping Kafka records.
	Writer *bufio.Writer
}

// DumpRecord dumps a Kafka record to the configured buffered writer.
//
//nolint:cyclop
func (wrd *WriterRecordDumper) DumpRecord(ctx context.Context, record *kgo.Record) error {
	defer wrd.Writer.Flush() //nolint:errcheck

	if wrd.DumpTimestamp {
		if _, err := fmt.Fprintf(wrd.Writer, "%d\t", record.Timestamp.UnixMilli()); err != nil {
			return fmt.Errorf("write timestamp: %w", err)
		}
	}

	if wrd.DumpPartition {
		if _, err := fmt.Fprintf(wrd.Writer, "%d\t", record.Partition); err != nil {
			return fmt.Errorf("write partition: %w", err)
		}
	}

	if wrd.DumpOffset {
		if _, err := fmt.Fprintf(wrd.Writer, "%d\t", record.Offset); err != nil {
			return fmt.Errorf("write offset: %w", err)
		}
	}

	if wrd.DumpKey { //nolint:nestif
		if record.Key != nil {
			if err := wrd.writeData(ctx, record.Key); err != nil {
				return fmt.Errorf("write key data: %w", err)
			}
		} else {
			if _, err := fmt.Fprint(wrd.Writer, "null"); err != nil {
				return fmt.Errorf("write null key: %w", err)
			}
		}

		if _, err := fmt.Fprint(wrd.Writer, "\t"); err != nil {
			return fmt.Errorf("write key tab separator: %w", err)
		}
	}

	if record.Value != nil {
		if err := wrd.writeData(ctx, record.Value); err != nil {
			return fmt.Errorf("write value data: %w", err)
		}
	} else {
		if _, err := fmt.Fprint(wrd.Writer, "null"); err != nil {
			return fmt.Errorf("write null value: %w", err)
		}
	}

	if _, err := fmt.Fprint(wrd.Writer, "\n"); err != nil {
		return fmt.Errorf("write newline: %w", err)
	}

	return nil
}

func (wrd *WriterRecordDumper) writeData(ctx context.Context, data []byte) error {
	if wrd.Deserializer != nil {
		json, err := wrd.Deserializer.DeserializeJSON(ctx, data)
		if err != nil {
			return fmt.Errorf("deserialize JSON: %w", err)
		}

		data = json
	}

	if _, err := wrd.Writer.Write(data); err != nil {
		return fmt.Errorf("write: %w", err)
	}

	return nil
}
