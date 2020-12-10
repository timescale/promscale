package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"regexp"
	"strings"
	"time"

	"github.com/inhies/go-bytesize"
	"github.com/timescale/promscale/pkg/log"
	plan "github.com/timescale/promscale/pkg/migration-tool/planner"
	"github.com/timescale/promscale/pkg/migration-tool/reader"
	"github.com/timescale/promscale/pkg/migration-tool/utils"
	"github.com/timescale/promscale/pkg/migration-tool/writer"
)

const (
	migrationJobName     = "prom-migrator"
	progressMetricName   = "prom_migrator_progress"
	validMetricNameRegex = `^[a-zA-Z_:][a-zA-Z0-9_:]*$`
)

type config struct {
	name               string
	mint               int64
	mintSec            int64
	maxt               int64
	maxtSec            int64
	maxBlockSizeBytes  int64
	maxBlockSize       string
	readURL            string
	writeURL           string
	progressMetricName string
	progressMetricURL  string
	progressEnabled    bool
	readerAuth         utils.Auth
	writerAuth         utils.Auth
}

func main() {
	conf := new(config)
	parseFlags(conf, os.Args[1:])
	if err := log.Init(log.Config{Format: "logfmt", Level: "debug"}); err != nil {
		_, _ = fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	if err := validateConf(conf); err != nil {
		log.Error("msg", "could not parse flags", "error", err)
		os.Exit(1)
	}
	log.Info("msg", fmt.Sprintf("%v+", conf))
	if err := utils.SetAuthStore(utils.Read, conf.readerAuth.ToHTTPClientConfig()); err != nil {
		log.Error("msg", "could not set read-auth in authStore", "error", err)
		os.Exit(1)
	}
	if err := utils.SetAuthStore(utils.Write, conf.readerAuth.ToHTTPClientConfig()); err != nil {
		log.Error("msg", "could not set write-auth in authStore", "error", err)
		os.Exit(1)
	}

	planConfig := &plan.Config{
		Mint:                conf.mint,
		Maxt:                conf.maxt,
		BlockSizeLimitBytes: conf.maxBlockSizeBytes,
	}
	planner, proceed, err := plan.Init(planConfig)
	if err != nil {
		log.Error("msg", "could not create plan", "error", err)
		os.Exit(2)
	}
	if !proceed {
		os.Exit(0)
	}

	var (
		readErrChan  = make(chan error)
		writeErrChan = make(chan error)
		sigBlockRead = make(chan *plan.Block)
	)
	cont, cancelFunc := context.WithCancel(context.Background())
	read, err := reader.New(cont, conf.readURL, planner, sigBlockRead)
	if err != nil {
		log.Error("msg", "could not create reader", "error", err)
		os.Exit(2)
	}
	write, err := writer.New(cont, conf.writeURL, conf.progressMetricName, conf.name, sigBlockRead)
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
	flag.StringVar(&conf.name, "migration-name", migrationJobName, "Name for the current migration that is to be carried out. "+
		"It corresponds to the value of the label 'job' set inside the progress-metric-name.")
	flag.Int64Var(&conf.mintSec, "mint", 0, "Minimum timestamp (in seconds) for carrying out data migration. (inclusive)")
	flag.Int64Var(&conf.maxtSec, "maxt", time.Now().Unix(), "Maximum timestamp (in seconds) for carrying out data migration (exclusive). "+
		"Setting this value less than zero will indicate all data from mint upto now. ")
	flag.StringVar(&conf.maxBlockSize, "max-read-size", "500MB", "(units: B, KB, MB, GB, TB, PB) the maximum size of data that should be read at a single time. "+
		"More the read size, faster will be the migration but higher will be the memory usage. Example: 250MB.")
	flag.StringVar(&conf.readURL, "read-url", "", "URL address for the storage where the data is to be read from.")
	flag.StringVar(&conf.writeURL, "write-url", "", "URL address for the storage where the data migration is to be written.")
	flag.StringVar(&conf.progressMetricName, "progress-metric-name", progressMetricName, "Prometheus metric name for tracking the last maximum timestamp pushed to the remote-write storage. "+
		"This is used to resume the migration process after a failure.")
	flag.StringVar(&conf.progressMetricURL, "progress-metric-url", "", "URL of the remote storage that contains the progress-metric. "+
		"Note: This url is used to fetch the last pushed timestamp. If you want the migration to resume from where it left, in case of a crash, "+
		"set this to the remote write storage that the migrator is writing along with the progress-enabled.")
	flag.BoolVar(&conf.progressEnabled, "progress-enabled", true, "This flag tells the migrator, whether or not to use the progress mechanism. It is helpful if you want to "+
		"carry out migration with the same time-range. If this is enabled, the migrator will resume the migration from the last time, where it was stopped/interrupted. "+
		"If you do not want any extra metric(s) while migration, you can set this to false. But, setting this to false will disble progress-metric and hence, the ability to resume migration.")
	// Authentication.
	// TODO: Auth/password via password_file and bearer_token via bearer_token_file.
	flag.StringVar(&conf.readerAuth.Username, "read-auth-username", "", "Auth username for remote-read storage.")
	flag.StringVar(&conf.readerAuth.Password, "read-auth-password", "", "Auth password for remote-read storage.")
	flag.StringVar(&conf.readerAuth.BearerToken, "read-auth-bearer-token", "", "Bearer token for remote-read storage. "+
		"This should be mutually exclusive with username and password.")
	flag.StringVar(&conf.writerAuth.Username, "write-auth-username", "", "Auth username for remote-write storage.")
	flag.StringVar(&conf.writerAuth.Password, "write-auth-password", "", "Auth password for remote-write storage.")
	flag.StringVar(&conf.writerAuth.BearerToken, "write-auth-bearer-token", "", "Bearer token for remote-write storage. "+
		"This should be mutually exclusive with username and password.")
	_ = flag.CommandLine.Parse(args)
	convertSecFlagToMs(conf)
}

func convertSecFlagToMs(conf *config) {
	// remote-storages tend to respond to time in milliseconds. So, we convert the received values in seconds to milliseconds.
	conf.mint = conf.mintSec * 1000
	conf.maxt = conf.maxtSec * 1000
}

func validateConf(conf *config) error {
	switch {
	case conf.mint == 0:
		return fmt.Errorf("mint should be provided for the migration to begin")
	case conf.mint < 0:
		return fmt.Errorf("invalid mint: %d", conf.mint)
	case conf.maxt < 0:
		return fmt.Errorf("invalid maxt: %d", conf.maxt)
	case conf.mint > conf.maxt:
		return fmt.Errorf("invalid input: minimum timestamp value (mint) cannot be greater than the maximum timestamp value (maxt)")
	case conf.progressMetricName != progressMetricName:
		if !regexp.MustCompile(validMetricNameRegex).MatchString(conf.progressMetricName) {
			return fmt.Errorf("invalid metric-name regex match: prom metric must match %s: recieved: %s", validMetricNameRegex, conf.progressMetricName)
		}
	case strings.TrimSpace(conf.readURL) == "" && strings.TrimSpace(conf.writeURL) == "":
		return fmt.Errorf("remote read storage url and remote write storage url must be specified. Without these, data migration cannot begin")
	case strings.TrimSpace(conf.readURL) == "":
		return fmt.Errorf("remote read storage url needs to be specified. Without read storage url, data migration cannot begin")
	case strings.TrimSpace(conf.writeURL) == "":
		return fmt.Errorf("remote write storage url needs to be specified. Without write storage url, data migration cannot begin")
	case conf.progressEnabled && strings.TrimSpace(conf.progressMetricURL) == "":
		return fmt.Errorf("invalid input: read url for remote-write storage should be provided when progress metric is enabled. To disable progress metric, use -progress-enabled=false")
	}
	httpConfig := conf.readerAuth.ToHTTPClientConfig()
	if err := httpConfig.Validate(); err != nil {
		return fmt.Errorf("reader auth validation: %w", err)
	}
	httpConfig = conf.writerAuth.ToHTTPClientConfig()
	if err := httpConfig.Validate(); err != nil {
		return fmt.Errorf("writer auth validation: %w", err)
	}

	maxBlockSizeBytes, err := bytesize.Parse(conf.maxBlockSize)
	if err != nil {
		return fmt.Errorf("parsing byte-size: %w", err)
	}
	conf.maxBlockSizeBytes = int64(maxBlockSizeBytes)
	return nil
}
