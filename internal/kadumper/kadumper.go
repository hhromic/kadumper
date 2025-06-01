// SPDX-FileCopyrightText: Copyright 2023 Hugo Hromic
// SPDX-License-Identifier: Apache-2.0

package kadumper

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// DumpRecords dumps records from Kafka using a provided client and record dumper.
//
//nolint:cyclop
func DumpRecords(
	ctx context.Context,
	kcl *kgo.Client,
	rdmp RecordDumper,
	maxRecords int,
	fetchTimeout time.Duration,
) error {
	pollFunc := func() kgo.Fetches {
		return kcl.PollFetches(ctx)
	}

	if fetchTimeout.Nanoseconds() > 0 {
		pollFunc = func() kgo.Fetches {
			ctx, cancel := context.WithTimeout(ctx, fetchTimeout)
			defer cancel()

			return kcl.PollFetches(ctx)
		}
	}

	records := 0
	for maxRecords == 0 || records < maxRecords {
		fetches := pollFunc()

		if fetches.IsClientClosed() {
			return ErrKafkaClientClosed
		}

		if ferrs := fetches.Errors(); len(ferrs) > 0 {
			var errs []error

			for _, ferr := range ferrs {
				if errors.Is(ferr.Err, context.DeadlineExceeded) {
					return nil
				}

				errs = append(errs, ferr.Err)
			}

			return fmt.Errorf("kafka client poll fetches: %w", errors.Join(errs...))
		}

		iter := fetches.RecordIter()
		for !iter.Done() && (maxRecords == 0 || records < maxRecords) {
			r := iter.Next()

			if err := rdmp.DumpRecord(ctx, r); err != nil {
				return fmt.Errorf("dump record: %w", err)
			}

			records++
		}
	}

	return nil
}
