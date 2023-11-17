// Copyright 2023 Hugo Hromic
// SPDX-License-Identifier: Apache-2.0

package kadumper

import (
	"bufio"
	"context"
	"fmt"

	"github.com/twmb/franz-go/pkg/kgo"
)

// RecordDumper is a Kafka records dumper.
type RecordDumper struct {
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

// DumpRecord dumps a Kafka record.
func (rd *RecordDumper) DumpRecord(ctx context.Context, record *kgo.Record) error {
	defer rd.Writer.Flush()

	if rd.DumpTimestamp {
		fmt.Fprintf(rd.Writer, "%d\t", record.Timestamp.UnixMilli())
	}

	if rd.DumpPartition {
		fmt.Fprintf(rd.Writer, "%d\t", record.Partition)
	}

	if rd.DumpOffset {
		fmt.Fprintf(rd.Writer, "%d\t", record.Offset)
	}

	if rd.DumpKey {
		if record.Key != nil {
			if err := rd.WriteData(ctx, record.Key); err != nil {
				return fmt.Errorf("write key data: %w", err)
			}
		} else {
			fmt.Fprint(rd.Writer, "null")
		}

		fmt.Fprint(rd.Writer, "\t")
	}

	if record.Value != nil {
		if err := rd.WriteData(ctx, record.Value); err != nil {
			return fmt.Errorf("write value data: %w", err)
		}
	} else {
		fmt.Fprint(rd.Writer, "null")
	}

	fmt.Fprint(rd.Writer, "\n")

	return nil
}

// WriteData deserializes data into JSON representation using [RecordDumper.Deserializer] and
// writes the result using [RecordDumper.Writer].
func (rd *RecordDumper) WriteData(ctx context.Context, data []byte) error {
	if rd.Deserializer != nil {
		json, err := rd.Deserializer.DeserializeJSON(ctx, data)
		if err != nil {
			return fmt.Errorf("deserialize JSON: %w", err)
		}

		data = json
	}

	if _, err := rd.Writer.Write(data); err != nil {
		return fmt.Errorf("write: %w", err)
	}

	return nil
}
