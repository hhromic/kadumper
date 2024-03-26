package main

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"log/slog"
	"net/url"
	"os"
	"os/signal"
	"runtime"
	"syscall"
	"time"

	"github.com/alexflint/go-arg"
	tkslog "github.com/hhromic/go-toolkit/slog"
	"github.com/hhromic/kadumper/internal/buildinfo"
	"github.com/hhromic/kadumper/internal/kadumper"
	"github.com/linkedin/goavro/v2"
	"github.com/twmb/go-cache/cache"
	"github.com/twmb/tlscfg"
	"go.uber.org/automaxprocs/maxprocs"
)

//nolint:lll,tagalign
type args struct {
	SeedBrokers       []string        `arg:"--seed-brokers,env:KADUMPER_SEED_BROKERS,required" placeholder:"BROKER" help:"Kafka bootstrap/seed brokers"`
	Topics            []string        `arg:"--topics,env:KADUMPER_TOPICS,required" placeholder:"TOPIC" help:"topics to consume from"`
	ConsumerGroup     string          `arg:"--consumer-group,env:KADUMPER_CONSUMER_GROUP" placeholder:"GROUP" help:"consumer group for distributed consumption"`
	SchemaRegistryURL *url.URL        `arg:"--schema-registry-url,env:KADUMPER_SCHEMA_REGISTRY_URL" placeholder:"URL" help:"URL of the schema registry"`
	SchemaMaxAge      time.Duration   `arg:"--schema-max-age,env:KADUMPER_SCHEMA_MAX_AGE" default:"1h" placeholder:"DURATION" help:"maximum caching age for storing downloaded schemas"`
	TLSClientCert     string          `arg:"--tls-client-cert,env:KADUMPER_TLS_CLIENT_CERT" placeholder:"FILE" help:"TLS client certificate in PEM format"`
	TLSClientKey      string          `arg:"--tls-client-key,env:KADUMPER_TLS_CLIENT_KEY" placeholder:"FILE" help:"TLS client key in PEM format"`
	FromBeginning     bool            `arg:"--from-beginning,env:KADUMPER_FROM_BEGINNING" help:"start consuming from the beginning of the topic(s) instead of from the end"`
	MaxRecords        int             `arg:"--max-records,env:KADUMPER_MAX_RECORDS" default:"0" placeholder:"NUMBER" help:"maximum number of records to consume before exiting ('0' for unlimited)"`
	FetchTimeout      time.Duration   `arg:"--fetch-timeout,env:KADUMPER_FETCH_TIMEOUT" default:"0s" placeholder:"DURATION" help:"records fetch timeout before exiting ('0s' for unlimited)"`
	Dumper            kadumper.Dumper `arg:"--dumper,env:KADUMPER_DUMPER" default:"stdout" placeholder:"DUMPER" help:"Kafka records dumper to use for output"`
	DumpTimestamp     bool            `arg:"--dump-timestamp,env:KADUMPER_DUMP_TIMESTAMP" help:"whether to dump the timestamp of consumed records"`
	DumpPartition     bool            `arg:"--dump-partition,env:KADUMPER_DUMP_PARTITION" help:"whether to dump the partition number of consumed records"`
	DumpOffset        bool            `arg:"--dump-offset,env:KADUMPER_DUMP_OFFSET" help:"whether to dump the partition offset of consumed records"`
	DumpKey           bool            `arg:"--dump-key,env:KADUMPER_DUMP_KEY" help:"whether to dump the key of consumed records"`
	LogHandler        tkslog.Handler  `arg:"--log-handler,env:KADUMPER_LOG_HANDLER" default:"auto" placeholder:"HANDLER" help:"application logging handler"`
	LogLevel          slog.Level      `arg:"--log-level,env:KADUMPER_LOG_LEVEL" default:"info" placeholder:"LEVEL" help:"application logging level"`
}

func main() {
	var args args
	_ = arg.MustParse(&args)

	logger := tkslog.NewSlogLogger(os.Stderr, args.LogHandler, args.LogLevel)

	if err := appMain(logger, args); err != nil {
		logger.Error("application error", "err", err)
		os.Exit(1)
	}
}

//nolint:funlen,cyclop
func appMain(logger *slog.Logger, args args) error {
	if _, err := maxprocs.Set(); err != nil {
		slog.Warn("failed to set GOMAXPROCS", "err", err)
	}

	logger.Info("starting",
		"version", buildinfo.Version,
		"goversion", buildinfo.GoVersion,
		"gitcommit", buildinfo.GitCommit,
		"gitbranch", buildinfo.GitBranch,
		"builddate", buildinfo.BuildDate,
		"gomaxprocs", runtime.GOMAXPROCS(0),
	)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	var tcfg *tls.Config

	if args.TLSClientCert != "" && args.TLSClientKey != "" {
		cfg, err := tlscfg.New(
			tlscfg.MaybeWithDiskKeyPair(args.TLSClientCert, args.TLSClientKey),
		)
		if err != nil {
			return fmt.Errorf("new TLS config: %w", err)
		}

		tcfg = cfg

		logger.Info(
			"TLS configuration initialized",
			"tls_client_cert", args.TLSClientCert,
			"tls_client_key", args.TLSClientKey,
		)
	}

	kcl, err := kadumper.NewKafkaConsumerClient(
		args.SeedBrokers,
		args.Topics,
		args.ConsumerGroup,
		args.FromBeginning,
		tcfg,
		logger,
	)
	if err != nil {
		return fmt.Errorf("new kafka consumer client: %w", err)
	}
	defer kcl.Close()

	logger.Info(
		"Kafka consumer client initialized",
		"seed_brokers", args.SeedBrokers,
		"topics", args.Topics,
		"consumer_group", args.ConsumerGroup,
		"from_beginning", args.FromBeginning,
	)

	var rdmp kadumper.RecordDumper

	switch args.Dumper { //nolint:gocritic
	case kadumper.DumperStdout:
		stdoutDumper := kadumper.NewStdoutRecordDumper()

		stdoutDumper.DumpTimestamp = args.DumpTimestamp
		stdoutDumper.DumpPartition = args.DumpPartition
		stdoutDumper.DumpOffset = args.DumpOffset
		stdoutDumper.DumpKey = args.DumpKey

		if args.SchemaRegistryURL != nil {
			rcl, err := kadumper.NewRegistryClient(*args.SchemaRegistryURL, tcfg)
			if err != nil {
				return fmt.Errorf("new registry client: %w", err)
			}

			stypes, err := rcl.SupportedTypes(ctx)
			if err != nil {
				return fmt.Errorf("registry supported types: %w", err)
			}

			stdoutDumper.Deserializer = &kadumper.AvroDeserializer{
				RegistryClient: rcl,
				Cache: cache.New[int, *goavro.Codec](
					cache.AutoCleanInterval(time.Minute*30), //nolint:gomnd
					cache.MaxAge(args.SchemaMaxAge),
				),
			}

			logger.Info(
				"schema registry client and Avro deserializer initialized",
				"url", args.SchemaRegistryURL,
				"supported_types", stypes,
				"schema_max_age", args.SchemaMaxAge,
			)
		}

		logger.Info(
			"stdout record dumper initialized",
			"with_deserializer", stdoutDumper.Deserializer != nil,
			"dump_timestamp", stdoutDumper.DumpTimestamp,
			"dump_partition", stdoutDumper.DumpPartition,
			"dump_offset", stdoutDumper.DumpOffset,
			"dump_key", stdoutDumper.DumpKey,
		)

		rdmp = stdoutDumper
	}

	logger.Info("pinging Kafka cluster")

	if err := kcl.Ping(ctx); err != nil {
		return fmt.Errorf("kafka cluster ping: %w", err)
	}

	logger.Info(
		"dumping records",
		"max_records", args.MaxRecords,
		"fetch_timeout", args.FetchTimeout,
	)

	err = kadumper.DumpRecords(ctx, kcl, rdmp, args.MaxRecords, args.FetchTimeout)
	if err != nil && !errors.Is(err, context.Canceled) {
		return fmt.Errorf("dump records: %w", err)
	}

	logger.Info("finished")

	return nil
}
