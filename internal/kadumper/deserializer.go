// Copyright 2023 Hugo Hromic
// SPDX-License-Identifier: Apache-2.0

package kadumper

import (
	"context"
	"fmt"

	"github.com/linkedin/goavro/v2"
	"github.com/twmb/franz-go/pkg/sr"
	"github.com/twmb/go-cache/cache"
)

//nolint:gochecknoglobals
var defaultHeader = &sr.ConfluentHeader{}

// Deserializer is a binary data deserializer.
type Deserializer interface {
	// DeserializeJSON deserializes binary data into JSON representation.
	DeserializeJSON(ctx context.Context, data []byte) ([]byte, error)
}

// AvroCodecCache is a type alias for a cache of schema IDs and Avro codecs.
type AvroCodecCache = cache.Cache[int, *goavro.Codec]

// AvroDeserializer implements an Avro binary data [Deserializer].
//
// Avro schemas are automatically downloaded and cached from a schema registry service.
type AvroDeserializer struct {
	// RegistryClient is the schema registry client to use for downloading schemas.
	RegistryClient *sr.Client
	// Cache is the cache to use for caching downloaded schemas.
	Cache *AvroCodecCache
}

// DeserializeJSON deserializes Avro binary data into JSON representation.
//
// Schema IDs are decoded from the data and codecs are retrieved using [AvroDeserializer.CodecForSchemaID].
func (ad *AvroDeserializer) DeserializeJSON(ctx context.Context, data []byte) ([]byte, error) {
	schID, payload, err := defaultHeader.DecodeID(data)
	if err != nil {
		return nil, fmt.Errorf("schema ID header decode: %w", err)
	}

	codec, err := ad.CodecForSchemaID(ctx, schID)
	if err != nil {
		return nil, fmt.Errorf("avro codec for schema ID: %d: %w", schID, err)
	}

	nat, _, err := codec.NativeFromBinary(payload)
	if err != nil {
		return nil, fmt.Errorf("avro codec native from binary: %w", err)
	}

	data, err = codec.TextualFromNative(nil, nat)
	if err != nil {
		return nil, fmt.Errorf("avro codec textual from native: %w", err)
	}

	return data, nil
}

// CodecForSchemaID returns an Avro codec for a schema ID.
//
// Available codecs are managed using [AvroDeserializer.Cache]. If a requested codec is not found in
// the cache, a schema is downloaded from the schema registry and a new codec is created and cached.
func (ad *AvroDeserializer) CodecForSchemaID(ctx context.Context, schID int) (*goavro.Codec, error) {
	codec, _, ks := ad.Cache.TryGet(schID)
	if ks != cache.Hit {
		sch, err := ad.RegistryClient.SchemaByID(ctx, schID)
		if err != nil {
			return nil, fmt.Errorf("registry schema text by ID: %w", err)
		}

		codec, err = goavro.NewCodecForStandardJSONFull(sch.Schema)
		if err != nil {
			return nil, fmt.Errorf("new avro codec: %w", err)
		}

		ad.Cache.Set(schID, codec)
	}

	return codec, nil
}
