// Copyright 2023 Hugo Hromic
// SPDX-License-Identifier: Apache-2.0

package kadumper

import (
	"context"
	"errors"
	"fmt"

	"github.com/twmb/franz-go/pkg/kgo"
)

func DumpRecords(
	ctx context.Context,
	kcl *kgo.Client,
	rdmp RecordDumper,
	maxRecords int,
) error {
	records := 0
	for maxRecords == 0 || records < maxRecords {
		fetches := kcl.PollFetches(ctx)

		if fetches.IsClientClosed() {
			return ErrKafkaClientClosed
		}

		if ferrs := fetches.Errors(); len(ferrs) > 0 {
			var errs []error
			for _, ferr := range ferrs {
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
