// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/inhies/go-bytesize"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/timescale/promscale/pkg/log"
	plan "github.com/timescale/promscale/pkg/migration-tool/planner"
	"github.com/timescale/promscale/pkg/migration-tool/reader"
	"github.com/timescale/promscale/pkg/migration-tool/utils"
	"github.com/timescale/promscale/pkg/migration-tool/writer"
	"github.com/timescale/promscale/pkg/version"
)

const (
	migrationJobName     = "prom-migrator"
	progressMetricName   = "prom_migrator_progress"
	validMetricNameRegex = `^[a-zA-Z_:][a-zA-Z0-9_:]*$`

	defaultTimeout         = time.Minute * 5
	defaultRetryDelay      = time.Second
	defaultStartTime       = "1970-01-01T00:00:00+00:00" // RFC3339 based time.Unix from 0 seconds.
	defaultMaxReadDuration = time.Hour * 2
	defaultLaIncrement     = time.Minute
)

type config struct {
	name    string
	start   string
	end     string
	mint    int64
	mintSec int64
	// We keep 'humanReadableTime' even though its not directly set by flags, as it helps in slab time-range as UX.
	humanReadableTime    bool
	maxt                 int64
	maxtSec              int64
	maxSlabSizeBytes     int64
	maxSlabSize          string
	concurrentPull       int
	concurrentPush       int
	readerClientConfig   utils.ClientConfig
	maxReadDuration      time.Duration // Max range of slab in terms of time.
	writerClientConfig   utils.ClientConfig
	progressMetricName   string
	progressMetricURL    string
	progressEnabled      bool
	garbageCollectOnPush bool
	laIncrement          time.Duration
	readerAuth           utils.Auth
	writerAuth           utils.Auth
	progressMetricAuth   utils.Auth
	readerMetricsMatcher string
}

func main() {
	conf := new(config)
	args := os.Args[1:]
	if shouldProceed := parseArgs(args); !shouldProceed {
		os.Exit(0)
	}

	parseFlags(conf, os.Args[1:])

	if err := log.Init(log.Config{Format: "logfmt", Level: "debug"}); err != nil {
		fmt.Println("Version: ", version.PromMigrator)
		_, _ = fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	log.Info("Version", version.PromMigrator)
	if err := validateConf(conf); err != nil {
		log.Error("msg", "could not parse flags", "error", err)
		os.Exit(1)
	}
	log.Info("msg", fmt.Sprintf("%v+", conf))

	planConfig := &plan.Config{
		Mint:                 conf.mint,
		Maxt:                 conf.maxt,
		JobName:              conf.name,
		SlabSizeLimitBytes:   conf.maxSlabSizeBytes,
		MaxReadDuration:      conf.maxReadDuration,
		LaIncrement:          conf.laIncrement,
		HumanReadableTime:    conf.humanReadableTime,
		NumStores:            conf.concurrentPull,
		ProgressEnabled:      conf.progressEnabled,
		ProgressMetricName:   conf.progressMetricName,
		ProgressClientConfig: getDefaultClientConfig(conf.progressMetricURL),
		HTTPConfig:           conf.progressMetricAuth.ToHTTPClientConfig(),
	}
	planner, proceed, err := plan.Init(planConfig)
	if err != nil {
		log.Error("msg", "could not create plan", "error", err)
		os.Exit(2)
	}
	if !proceed {
		os.Exit(0)
	}

	ms, err := parser.ParseExpr(conf.readerMetricsMatcher)
	if err != nil {
		log.Error("msg", "could not parse reader metrics matcher", "error", err)
		os.Exit(2)
	}

	vs, ok := ms.(*parser.VectorSelector)
	if !ok {
		log.Error("msg", "invalid metrics matcher", "error", "only vector selector can be used")
		os.Exit(2)
	}

	var (
		readErrChan  = make(chan error)
		writeErrChan = make(chan error)
		sigSlabRead  = make(chan *plan.Slab)
	)
	cont, cancelFunc := context.WithCancel(context.Background())
	readerConfig := reader.Config{
		Context:         cont,
		ClientConfig:    conf.readerClientConfig,
		Plan:            planner,
		HTTPConfig:      conf.readerAuth.ToHTTPClientConfig(),
		ConcurrentPulls: conf.concurrentPull,
		SigSlabRead:     sigSlabRead,
		MetricsMatchers: vs.LabelMatchers,
	}
	read, err := reader.New(readerConfig)
	if err != nil {
		log.Error("msg", "could not create reader", "error", err)
		os.Exit(2)
	}

	writerConfig := writer.Config{
		Context:              cont,
		ClientConfig:         conf.writerClientConfig,
		HTTPConfig:           conf.writerAuth.ToHTTPClientConfig(),
		ProgressEnabled:      conf.progressEnabled,
		ProgressMetricName:   conf.progressMetricName,
		GarbageCollectOnPush: conf.garbageCollectOnPush,
		MigrationJobName:     conf.name,
		ConcurrentPush:       conf.concurrentPush,
		SigSlabRead:          sigSlabRead,
	}
	write, err := writer.New(writerConfig)
	if err != nil {
		log.Error("msg", "could not create writer", "error", err)
		os.Exit(2)
	}

	read.Run(readErrChan)
	write.Run(writeErrChan)

loop:
	for {
		select {
		case err = <-readErrChan:
			if err != nil {
				cancelFunc()
				log.Error("msg", fmt.Errorf("running reader: %w", err).Error())
				os.Exit(2)
			}
		case err, ok := <-writeErrChan:
			cancelFunc() // As in any ideal case, the reader will always exit normally first.
			if ok {
				log.Error("msg", fmt.Errorf("running writer: %w", err).Error())
				os.Exit(2)
			}
			break loop
		}
	}

	log.Info("msg", "migration successfully carried out")
	log.Info("msg", "exiting!")
}

func parseFlags(conf *config, args []string) {
	// todo: update docs.
	flag.StringVar(&conf.name, "migration-name", migrationJobName, "Name for the current migration that is to be carried out. "+
		"It corresponds to the value of the label 'job' set inside the progress-metric-name.")
	flag.StringVar(&conf.start, "start", defaultStartTime, fmt.Sprintf("Start time (in RFC3339 format, like '%s', or in number of seconds since the unix epoch, UTC) from which the data migration is to be carried out. (inclusive)", defaultStartTime))
	flag.StringVar(&conf.end, "end", "", fmt.Sprintf("End time (in RFC3339 format, like '%s', or in number of seconds since the unix epoch, UTC) for carrying out data migration (exclusive). ", defaultStartTime)+
		"By default if this value is unset, then 'end' will be set to the time at which migration is starting. ")
	flag.StringVar(&conf.maxSlabSize, "max-read-size", "500MB", "(units: B, KB, MB, GB, TB, PB) the maximum size of data that should be read at a single time. "+
		"More the read size, faster will be the migration but higher will be the memory usage. Example: 250MB.")
	flag.BoolVar(&conf.garbageCollectOnPush, "gc-on-push", false, "Run garbage collector after every slab is pushed. "+
		"This may lead to better memory management since GC is kick right after each slab to clean unused memory blocks.")

	flag.IntVar(&conf.concurrentPush, "concurrent-push", 1, "Concurrent push enables pushing of slabs concurrently. "+
		"Each slab is divided into 'concurrent-push' (value) parts and then pushed to the remote-write storage concurrently. This may lead to higher throughput on "+
		"the remote-write storage provided it is capable of handling the load. Note: Larger shards count will lead to significant memory usage.")
	flag.IntVar(&conf.concurrentPull, "concurrent-pull", 1, "Concurrent pull enables fetching of data concurrently. "+
		"Each fetch query is divided into 'concurrent-pull' (value) parts and then fetched concurrently. "+
		"This allows higher throughput of read by pulling data faster from the remote-read storage. "+
		"Note: Setting 'concurrent-pull' > 1 will show progress of concurrent fetching of data in the progress-bar and disable real-time transfer rate. "+
		"High 'concurrent-pull' can consume significant memory, so make sure you balance this with your number of migrating series and available memory. "+
		"Also, setting this value too high may cause TLS handshake error on the read storage side or may lead to starvation of fetch requests, "+
		"depending on your network bandwidth.")

	flag.DurationVar(&conf.maxReadDuration, "max-read-duration", defaultMaxReadDuration, "Maximum range of slab that can be achieved through consecutive 'la-increment'. "+
		"This defaults to '2 hours' since assuming the migration to be from Prometheus TSDB. Increase this duration if you are not fetching from Prometheus "+
		"or if you are fine with slow read on Prometheus side post 2 hours slab time-range.")
	flag.DurationVar(&conf.laIncrement, "slab-range-increment", defaultLaIncrement, "Amount of time-range to be incremented in successive slab.")

	flag.StringVar(&conf.readerClientConfig.URL, "reader-url", "", "URL address for the storage where the data is to be read from.")
	flag.DurationVar(&conf.readerClientConfig.Timeout, "reader-timeout", defaultTimeout, "Timeout for fetching data from read storage. "+
		"This timeout is also used to fetch the progress metric.")
	flag.DurationVar(&conf.readerClientConfig.Delay, "reader-retry-delay", defaultRetryDelay, "Duration to wait after a 'read-timeout' "+
		"before prom-migrator retries to fetch the slab. Delay is used only if 'retry' option is set in OnTimeout or OnErr.")
	flag.IntVar(&conf.readerClientConfig.MaxRetry, "reader-max-retries", 0, "Maximum number of retries before erring out. "+
		"Setting this to 0 will make the retry process forever until the process is completed. Note: If you want not to retry, "+
		"change the value of on-timeout or on-error to non-retry options.")
	flag.StringVar(&conf.readerClientConfig.OnTimeoutStr, "reader-on-timeout", "retry", "When a timeout happens during the read process, how should the reader behave. "+
		"Valid options: ['retry', 'skip', 'abort']. "+
		"If 'retry', the reader retries to fetch the current slab after the delay. "+
		"If 'skip', the reader skips the current slab that is being read and moves on to the next slab. "+
		"If 'abort', the migration process will be aborted.")
	flag.StringVar(&conf.readerClientConfig.OnErrStr, "reader-on-error", "abort", "When an error occurs during read process, how should the reader behave. "+
		"Valid options: ['retry', 'skip', 'abort']. "+
		"See 'reader-on-timeout' for more information on the above options. ")
	flag.StringVar(&conf.readerMetricsMatcher, "reader-metrics-matcher", `{__name__=".*"}`, "Metrics vector selector to read data for migration.")

	flag.StringVar(&conf.writerClientConfig.URL, "writer-url", "", "URL address for the storage where the data migration is to be written.")
	flag.DurationVar(&conf.writerClientConfig.Timeout, "writer-timeout", defaultTimeout, "Timeout for pushing data to write storage.")
	flag.DurationVar(&conf.writerClientConfig.Delay, "writer-retry-delay", defaultRetryDelay, "Duration to wait after a 'write-timeout' "+
		"before prom-migrator retries to push the slab. Delay is used only if 'retry' option is set in OnTimeout or OnErr.")
	flag.IntVar(&conf.writerClientConfig.MaxRetry, "writer-max-retries", 0, "Maximum number of retries before erring out. "+
		"Setting this to 0 will make the retry process forever until the process is completed. Note: If you want not to retry, "+
		"change the value of on-timeout or on-error to non-retry options.")
	flag.StringVar(&conf.writerClientConfig.OnTimeoutStr, "writer-on-timeout", "retry", "When a timeout happens during the write process, how should the writer behave. "+
		"Valid options: ['retry', 'skip', 'abort']. "+
		"If 'retry', the writer retries to push the current slab after the delay. "+
		"If 'skip', the writer skips the current slab that is being pushed and moves on to the next slab. "+
		"If 'abort', the migration process will be aborted.")
	flag.StringVar(&conf.writerClientConfig.OnErrStr, "writer-on-error", "abort", "When an error occurs during write process, how should the writer behave. "+
		"Valid options: ['retry', 'skip', 'abort']. "+
		"See 'writer-on-timeout' for more information on the above options. ")

	flag.StringVar(&conf.progressMetricName, "progress-metric-name", progressMetricName, "Prometheus metric name for tracking the last maximum timestamp pushed to the remote-write storage. "+
		"This is used to resume the migration process after a failure.")
	flag.StringVar(&conf.progressMetricURL, "progress-metric-url", "", "URL of the remote storage that contains the progress-metric. "+
		"Note: This url is used to fetch the last pushed timestamp. If you want the migration to resume from where it left, in case of a crash, "+
		"set this to the remote write storage that the migrator is writing along with the progress-enabled.")
	flag.BoolVar(&conf.progressEnabled, "progress-enabled", true, "This flag tells the migrator, whether or not to use the progress mechanism. It is helpful if you want to "+
		"carry out migration with the same time-range. If this is enabled, the migrator will resume the migration from the last time, where it was stopped/interrupted. "+
		"If you do not want any extra metric(s) while migration, you can set this to false. But, setting this to false will disable progress-metric and hence, the ability to resume migration.")

	// Authentication.
	flag.StringVar(&conf.readerAuth.Username, "reader-auth-username", "", "Auth username for remote-read storage.")
	flag.StringVar(&conf.readerAuth.Password, "reader-auth-password", "", "Auth password for remote-read storage. Mutually exclusive with password-file.")
	flag.StringVar(&conf.readerAuth.PasswordFile, "reader-auth-password-file", "", "Auth password-file for remote-read storage. Mutually exclusive with password.")
	flag.StringVar(&conf.readerAuth.BearerToken, "reader-auth-bearer-token", "", "Bearer-token for remote-read storage. "+
		"This should be mutually exclusive with username and password and bearer-token file.")
	flag.StringVar(&conf.readerAuth.BearerTokenFile, "reader-auth-bearer-token-file", "", "Bearer-token file for remote-read storage. "+
		"This should be mutually exclusive with username and password and bearer-token.")

	flag.StringVar(&conf.writerAuth.Username, "writer-auth-username", "", "Auth username for remote-write storage.")
	flag.StringVar(&conf.writerAuth.Password, "writer-auth-password", "", "Auth password for remote-write storage. Mutually exclusive with password-file.")
	flag.StringVar(&conf.writerAuth.PasswordFile, "writer-auth-password-file", "", "Auth password-file for remote-write storage. Mutually exclusive with password.")
	flag.StringVar(&conf.writerAuth.BearerToken, "writer-auth-bearer-token", "", "Bearer-token for remote-write storage. "+
		"This should be mutually exclusive with username and password and bearer-token file.")
	flag.StringVar(&conf.writerAuth.BearerTokenFile, "writer-auth-bearer-token-file", "", "Bearer-token for remote-write storage. "+
		"This should be mutually exclusive with username and password and bearer-token.")

	flag.StringVar(&conf.progressMetricAuth.Username, "progress-metric-auth-username", "", "Read auth username for remote-write storage.")
	flag.StringVar(&conf.progressMetricAuth.Password, "progress-metric-password", "", "Read auth password for remote-write storage. Mutually exclusive with password-file.")
	flag.StringVar(&conf.progressMetricAuth.PasswordFile, "progress-metric-password-file", "", "Read auth password for remote-write storage. Mutually exclusive with password.")
	flag.StringVar(&conf.progressMetricAuth.BearerToken, "progress-metric-bearer-token", "", "Read bearer-token for remote-write storage. "+
		"This should be mutually exclusive with username and password and bearer-token file.")
	flag.StringVar(&conf.progressMetricAuth.BearerTokenFile, "progress-metric-bearer-token-file", "", "Read bearer-token for remote-write storage. "+
		"This should be mutually exclusive with username and password and bearer-token.")

	// TLS configurations.
	// TODO: Update prom-migrator cli param doc.
	// Reader.
	flag.StringVar(&conf.readerAuth.TLSConfig.CAFile, "reader-tls-ca-file", "", "TLS CA file for remote-read component.")
	flag.StringVar(&conf.readerAuth.TLSConfig.CertFile, "reader-tls-cert-file", "", "TLS certificate file for remote-read component.")
	flag.StringVar(&conf.readerAuth.TLSConfig.KeyFile, "reader-tls-key-file", "", "TLS key file for remote-read component.")
	flag.StringVar(&conf.readerAuth.TLSConfig.ServerName, "reader-tls-server-name", "", "TLS server name for remote-read component.")
	flag.BoolVar(&conf.readerAuth.TLSConfig.InsecureSkipVerify, "reader-tls-insecure-skip-verify", false, "TLS insecure skip verify for remote-read component.")

	// Writer.
	flag.StringVar(&conf.writerAuth.TLSConfig.CAFile, "writer-tls-ca-file", "", "TLS CA file for remote-writer component.")
	flag.StringVar(&conf.writerAuth.TLSConfig.CertFile, "writer-tls-cert-file", "", "TLS certificate file for remote-writer component.")
	flag.StringVar(&conf.writerAuth.TLSConfig.KeyFile, "writer-tls-key-file", "", "TLS key file for remote-writer component.")
	flag.StringVar(&conf.writerAuth.TLSConfig.ServerName, "writer-tls-server-name", "", "TLS server name for remote-writer component.")
	flag.BoolVar(&conf.writerAuth.TLSConfig.InsecureSkipVerify, "writer-tls-insecure-skip-verify", false, "TLS insecure skip verify for remote-writer component.")

	// Progress-metric storage.
	flag.StringVar(&conf.progressMetricAuth.TLSConfig.CAFile, "progress-metric-tls-ca-file", "", "TLS CA file for progress-metric component.")
	flag.StringVar(&conf.progressMetricAuth.TLSConfig.CertFile, "progress-metric-tls-cert-file", "", "TLS certificate file for progress-metric component.")
	flag.StringVar(&conf.progressMetricAuth.TLSConfig.KeyFile, "progress-metric-tls-key-file", "", "TLS key file for progress-metric component.")
	flag.StringVar(&conf.progressMetricAuth.TLSConfig.ServerName, "progress-metric-tls-server-name", "", "TLS server name for progress-metric component.")
	flag.BoolVar(&conf.progressMetricAuth.TLSConfig.InsecureSkipVerify, "progress-metric-tls-insecure-skip-verify", false, "TLS insecure skip verify for progress-metric component.")

	_ = flag.CommandLine.Parse(args)
}

func getDefaultClientConfig(url string) utils.ClientConfig {
	return utils.ClientConfig{
		URL:       url,
		Timeout:   defaultTimeout,
		Delay:     defaultRetryDelay,
		OnTimeout: utils.Retry,
		OnErr:     utils.Abort,
	}
}

func parseArgs(args []string) (shouldProceed bool) {
	shouldProceed = true // Some flags like 'version' are just to get information and not proceed the actual execution. We should stop in such cases.
	for _, f := range args {
		f = f[1:]
		switch f {
		case "version":
			shouldProceed = false
			fmt.Println(version.PromMigrator)
		}
	}
	return
}

// stripSingleQuotes removes single quotes from the string input.
func stripSingleQuotes(s string) string {
	if s[0] == '\'' {
		s = s[1:]
	}
	if s[len(s)-1] == '\'' {
		s = s[:len(s)-1]
	}
	return s
}

func parseRFC3339(s string) (int64, error) {
	if s == "" {
		// When 'end' is not set.
		return time.Now().Unix(), nil
	}
	t, err := time.Parse(time.RFC3339, stripSingleQuotes(s))
	if err != nil {
		return -1, fmt.Errorf("cannot convert string flag values to actual timestamps: %w", err)
	}
	return t.Unix(), nil
}

func convertTimeStrFlagsToTs(conf *config) error {
	v, err := strconv.Atoi(conf.start)
	if err != nil {
		// This can be a RFC3339.
		t, parseErr := parseRFC3339(conf.start)
		if parseErr != nil {
			// Neither unix nor RFC3339.
			return fmt.Errorf("'start' flag should be either unix-seconds or RFC3339 compatible only:\n%w\n%s", parseErr, err.Error())
		}
		conf.humanReadableTime = true // It's important to set this, so slabs are formed with respective format for easier debugging.
		v = int(t)
	}
	conf.mintSec = int64(v)
	if conf.end == "" {
		conf.end = fmt.Sprintf("%d", time.Now().Unix())
	}
	v, err = strconv.Atoi(conf.end)
	if err != nil {
		// This can be a RFC3339.
		t, parseErr := parseRFC3339(conf.end)
		if parseErr != nil {
			// Neither unix nor RFC3339.
			return fmt.Errorf("'end' flag should be either unix-seconds or RFC3339 compatible only:\n%w\n%s", parseErr, err.Error())
		}
		v = int(t)
	}
	conf.maxtSec = int64(v)
	// remote-storages tend to respond to time in milliseconds. So, we convert the received values in seconds to milliseconds.
	conf.mint = conf.mintSec * 1000
	conf.maxt = conf.maxtSec * 1000
	return nil
}

func validateConf(conf *config) error {
	if err := convertTimeStrFlagsToTs(conf); err != nil {
		return fmt.Errorf("validate time flags: %w", err)
	}
	if err := utils.ParseClientInfo(&conf.readerClientConfig); err != nil {
		return fmt.Errorf("parsing reader-client info: %w", err)
	}
	if err := utils.ParseClientInfo(&conf.writerClientConfig); err != nil {
		return fmt.Errorf("parsing writer-client info: %w", err)
	}
	switch {
	case conf.start == defaultStartTime:
		return fmt.Errorf("'start' should be provided for the migration to begin")
	case conf.mintSec > conf.maxtSec:
		return fmt.Errorf("invalid input: minimum timestamp value (start) cannot be greater than the maximum timestamp value (end)")
	case conf.progressMetricName != progressMetricName:
		if !regexp.MustCompile(validMetricNameRegex).MatchString(conf.progressMetricName) {
			return fmt.Errorf("invalid metric-name regex match: prom metric must match %s: recieved: %s", validMetricNameRegex, conf.progressMetricName)
		}
	case strings.TrimSpace(conf.readerClientConfig.URL) == "" && strings.TrimSpace(conf.writerClientConfig.URL) == "":
		return fmt.Errorf("remote read storage url and remote write storage url must be specified. Without these, data migration cannot begin")
	case strings.TrimSpace(conf.readerClientConfig.URL) == "":
		return fmt.Errorf("remote read storage url needs to be specified. Without read storage url, data migration cannot begin")
	case strings.TrimSpace(conf.writerClientConfig.URL) == "":
		return fmt.Errorf("remote write storage url needs to be specified. Without write storage url, data migration cannot begin")
	case conf.progressEnabled && strings.TrimSpace(conf.progressMetricURL) == "":
		return fmt.Errorf("invalid input: read url for remote-write storage should be provided when progress metric is enabled. To disable progress metric, use -progress-enabled=false")
	case conf.laIncrement < time.Minute:
		return fmt.Errorf("'slab-range-increment' cannot be less than 1 minute")
	case conf.maxReadDuration < time.Minute:
		return fmt.Errorf("'max-read-duration' cannot be less than 1 minute")
	}

	// Validate auths.
	httpConfig := conf.readerAuth.ToHTTPClientConfig()
	if err := httpConfig.Validate(); err != nil {
		return fmt.Errorf("reader auth validation: %w", err)
	}
	httpConfig = conf.writerAuth.ToHTTPClientConfig()
	if err := httpConfig.Validate(); err != nil {
		return fmt.Errorf("writer auth validation: %w", err)
	}
	httpConfig = conf.progressMetricAuth.ToHTTPClientConfig()
	if err := httpConfig.Validate(); err != nil {
		return fmt.Errorf("progress-metric storage auth validation: %w", err)
	}

	maxSlabSizeBytes, err := bytesize.Parse(conf.maxSlabSize)
	if err != nil {
		return fmt.Errorf("parsing byte-size: %w", err)
	}
	conf.maxSlabSizeBytes = int64(maxSlabSizeBytes)
	return nil
}
