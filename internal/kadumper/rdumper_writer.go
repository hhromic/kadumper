// Copyright 2023 Hugo Hromic
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
func (wrd *WriterRecordDumper) DumpRecord(ctx context.Context, record *kgo.Record) error {
	defer wrd.Writer.Flush()

	if wrd.DumpTimestamp {
		fmt.Fprintf(wrd.Writer, "%d\t", record.Timestamp.UnixMilli())
	}

	if wrd.DumpPartition {
		fmt.Fprintf(wrd.Writer, "%d\t", record.Partition)
	}

	if wrd.DumpOffset {
		fmt.Fprintf(wrd.Writer, "%d\t", record.Offset)
	}

	if wrd.DumpKey {
		if record.Key != nil {
			if err := wrd.writeData(ctx, record.Key); err != nil {
				return fmt.Errorf("write key data: %w", err)
			}
		} else {
			fmt.Fprint(wrd.Writer, "null")
		}

		fmt.Fprint(wrd.Writer, "\t")
	}

	if record.Value != nil {
		if err := wrd.writeData(ctx, record.Value); err != nil {
			return fmt.Errorf("write value data: %w", err)
		}
	} else {
		fmt.Fprint(wrd.Writer, "null")
	}

	fmt.Fprint(wrd.Writer, "\n")

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
