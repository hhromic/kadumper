// SPDX-FileCopyrightText: Copyright 2023 Hugo Hromic
// SPDX-License-Identifier: Apache-2.0

package kadumper

import (
	"crypto/tls"
	"fmt"
	"net/http"
	"net/url"

	"github.com/twmb/franz-go/pkg/sr"
)

// NewRegistryClient returns a new schema registry client.
func NewRegistryClient(srURL url.URL, tcfg *tls.Config) (*sr.Client, error) {
	opts := []sr.ClientOpt{
		sr.URLs(srURL.String()),
	}

	if tcfg != nil {
		cl := &http.Client{ //nolint:exhaustruct
			Transport: &http.Transport{ //nolint:exhaustruct
				TLSClientConfig: tcfg,
			},
		}
		opts = append(opts, sr.HTTPClient(cl))
	}

	rcl, err := sr.NewClient(opts...)
	if err != nil {
		return nil, fmt.Errorf("new client: %w", err)
	}

	return rcl, nil
}
