# Kafka Avro Dumper

Kafka Avro dumping utility written in Go.

Most command-line tools that can consume Avro records from Kafka output data using
[Avro JSON encoding](https://avro.apache.org/docs/1.12.0/specification/#json-encoding).
When using Avro [union types](https://avro.apache.org/docs/1.12.0/specification/#unions),
the Avro JSON encoding includes the specific type in the field to disambiguate.

For example, for a `bar` field defined as type `["null", "string"]` in Avro:
```json
{"foo":100,"bar":{"string":"hello"}}
```

While the above is necessary for correctly encoding JSON data into Avro, it becomes inconvenient
when decoding plain JSON data from Avro and feeding it into non-Avro applications for processing.

For a more convenient plain JSON data decoding, this tool uses the
[`goavro`](https://pkg.go.dev/github.com/linkedin/goavro/v2) library. More specifically, `kadumper`
uses the [Standard Full JSON](https://pkg.go.dev/github.com/linkedin/goavro/v2#NewCodecForStandardJSONFull)
codec which (from the documentation) _provides full serialization/deserialization for JSON data that
meets the expectations of regular internet JSON_.

For the same example `bar` field shown above, `kadumper` outputs the following JSON data:
```json
{"foo":100,"bar":"hello"}
```

In addition, `kadumper` can also output raw textual data (non-Avro) from Kafka.

## Usage

All currently available command-line arguments can be seen with `-h/--help`.

Consuming Avro data from Kafka is currently only supported in
[Confluent Wire format](https://docs.confluent.io/platform/current/schema-registry/fundamentals/serdes-develop/index.html#wire-format)
with schemas automatically downloaded from a Schema Registry service.
Downloaded schemas are cached with a configurable maximum caching age.
[Confluent Platform](https://docs.confluent.io/platform/current/overview.html) and
[Redpanda](https://redpanda.com/) have both been tested.

To output raw textual data (non-Avro) from Kafka, omit the `--schema-registry-url` argument.

Output Kafka records are separated by a newline `\n` character. For each record, output timestamps,
partitions, offsets, keys and values are separated by a TAB `\t` character. All output is sent to
the standard output.

Currently, only anonymous plain-text and mutual TLS authentication connections are supported.

## Building

> [!NOTE]
> Ready-to-use binaries are available in the
> [releases page](https://github.com/hhromic/kadumper/releases).

To build a snapshot locally using [GoReleaser](https://goreleaser.com/):
```bash
goreleaser build --clean --single-target --snapshot
```

## Releasing

To release a new version in GitHub using [GoReleaser](https://goreleaser.com/):
```bash
git tag vX.Y.Z
CGO_ENABLED=0 GITHUB_TOKEN=$(< /path/to/token) goreleaser release --clean
```

## License

This project is licensed under the [Apache License Version 2.0](LICENSE).
