package main

import (
	"bufio"
	"context"
	"crypto/tls"
	"fmt"
	"log/slog"
	"net/url"
	"os"
	"os/signal"
	"runtime"
	"time"

	"github.com/alexflint/go-arg"
	tkslog "github.com/hhromic/go-toolkit/slog"
	"github.com/hhromic/kadumper/internal/buildinfo"
	"github.com/hhromic/kadumper/internal/kadumper"
	"github.com/linkedin/goavro/v2"
	"github.com/twmb/franz-go/pkg/sr"
	"github.com/twmb/go-cache/cache"
	"github.com/twmb/tlscfg"
	"go.uber.org/automaxprocs/maxprocs"
	"golang.org/x/sys/unix"
)

//nolint:lll,tagalign
type args struct {
	SeedBrokers       []string       `arg:"--seed-brokers,env:KADUMPER_SEED_BROKERS,required" help:"Kafka bootstrap/seed brokers" placeholder:"BROKER"`
	Topics            []string       `arg:"--topics,env:KADUMPER_TOPICS,required" help:"topics to consume from" placeholder:"TOPIC"`
	ConsumerGroup     string         `arg:"--consumer-group,env:KADUMPER_CONSUMER_GROUP" help:"consumer group for distributed consumption" placeholder:"GROUP"`
	SchemaRegistryURL *url.URL       `arg:"--schema-registry-url,env:KADUMPER_SCHEMA_REGISTRY_URL" help:"URL of the schema registry" placeholder:"URL"`
	TLSClientCert     string         `arg:"--tls-client-cert,env:KADUMPER_TLS_CLIENT_CERT" help:"TLS client certificate in PEM format" placeholder:"FILE"`
	TLSClientKey      string         `arg:"--tls-client-key,env:KADUMPER_TLS_CLIENT_KEY" help:"TLS client key in PEM format" placeholder:"FILE"`
	FromBeginning     bool           `arg:"--from-beginning,env:KADUMPER_FROM_BEGINNING" help:"start consuming from the beginning of the topic(s) instead of from the end"`
	MaxRecords        int            `arg:"--max-records,env:KADUMPER_MAX_RECORDS" default:"0" help:"maximum number of records to consume ('0' for unlimited)" placeholder:"NUMBER"`
	DumpTimestamp     bool           `arg:"--dump-timestamp,env:KADUMPER_DUMP_TIMESTAMP" help:"whether to dump the timestamp of consumed records"`
	DumpPartition     bool           `arg:"--dump-partition,env:KADUMPER_DUMP_PARTITION" help:"whether to dump the partition number of consumed records"`
	DumpOffset        bool           `arg:"--dump-offset,env:KADUMPER_DUMP_OFFSET" help:"whether to dump the partition offset of consumed records"`
	DumpKey           bool           `arg:"--dump-key,env:KADUMPER_DUMP_KEY" help:"whether to dump the key of consumed records"`
	LogHandler        tkslog.Handler `arg:"--log-handler,env:KADUMPER_LOG_HANDLER" default:"auto" placeholder:"HANDLER" help:"application logging handler"`
	LogLevel          slog.Level     `arg:"--log-level,env:KADUMPER_LOG_LEVEL" default:"info" placeholder:"LEVEL" help:"application logging level"`
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

	var adeser *kadumper.AvroDeserializer

	if args.SchemaRegistryURL != nil {
		rcl, err := kadumper.NewRegistryClient(*args.SchemaRegistryURL, tcfg)
		if err != nil {
			return fmt.Errorf("new registry client: %w", err)
		}

		adeser = &kadumper.AvroDeserializer{
			Header:         &sr.ConfluentHeader{},
			RegistryClient: rcl,
			Cache: cache.New[int, *goavro.Codec](
				cache.AutoCleanInterval(time.Minute*30), //nolint:gomnd
				cache.MaxAge(time.Hour),
			),
		}

		logger.Info(
			"schema registry client and Avro deserializer initialized",
			"url", args.SchemaRegistryURL,
		)
	}

	rdmp := &kadumper.RecordDumper{
		Deserializer:    adeser,
		DumpTimestamp:   args.DumpTimestamp,
		DumpPartition:   args.DumpPartition,
		DumpOffset:      args.DumpOffset,
		DumpKey:         args.DumpKey,
		UseDeserializer: adeser != nil,
		Writer:          bufio.NewWriter(os.Stdout),
	}

	logger.Info(
		"record dumper initialized",
		"dump_timestamp", args.DumpTimestamp,
		"dump_partition", args.DumpPartition,
		"dump_offset", args.DumpOffset,
		"dump_key", args.DumpKey,
	)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, unix.SIGTERM)
	defer stop()

	logger.Info("pinging Kafka cluster")

	if err := kcl.Ping(ctx); err != nil {
		return fmt.Errorf("kafka cluster ping: %w", err)
	}

	if adeser != nil {
		stypes, err := adeser.RegistryClient.SupportedTypes(ctx)
		if err != nil {
			return fmt.Errorf("registry supported types: %w", err)
		}

		logger.Info(
			"schema registry contacted",
			"supported_types", stypes,
		)
	}

	logger.Info(
		"dumping records",
		"max_records", args.MaxRecords,
	)

	if err := kadumper.DumpRecords(ctx, kcl, rdmp, args.MaxRecords); err != nil {
		return fmt.Errorf("dump records: %w", err)
	}

	logger.Info("finished")

	return nil
}
