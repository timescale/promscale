package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"regexp"
	"strings"
	"time"

	"github.com/timescale/promscale/pkg/log"
	plan "github.com/timescale/promscale/pkg/migration-tool/planner"
	"github.com/timescale/promscale/pkg/migration-tool/reader"
	"github.com/timescale/promscale/pkg/migration-tool/writer"
)

type config struct {
	name               string
	mint               int64
	maxt               int64
	readURL            string
	writeURL           string
	writerReadURL      string
	progressEnabled    bool
	progressMetricName string
}

const (
	migrationJobName     = "prom-migrator"
	progressMetricName   = "prom_migrator_progress"
	validMetricNameRegex = `^[a-zA-Z_:][a-zA-Z0-9_:]*$`
)

func main() {
	conf := new(config)
	parseFlags(conf, os.Args[1:])
	if err := log.Init(log.Config{Format: "logfmt", Level: "debug"}); err != nil {
		_, _ = fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	log.Info("msg", fmt.Sprintf("%v+", conf))
	if err := validateConf(conf); err != nil {
		log.Error("msg", "could not parse flags", "error", err)
		os.Exit(1)
	}

	planner := &plan.Plan{
		Mint:    conf.mint,
		Maxt:    conf.maxt,
		JobName: conf.name,
		// Progress metric configs.
		ProgressMetricName:        conf.progressMetricName,
		ProgressEnabled:           conf.progressEnabled,
		RemoteWriteStorageReadURL: conf.writerReadURL,
	}
	proceed, err := plan.Init(planner)
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
	}
	write, err := writer.New(cont, conf.writeURL, conf.progressMetricName, conf.name, sigBlockRead)
	if err != nil {
		log.Error("msg", "could not create writer", "error", err)
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
	flag.StringVar(&conf.name, "progress-job-name", migrationJobName, "Name for the current migration that is to be carried out. It corresponds to the value of the label 'job' set inside the progress-metric-name.")
	flag.Int64Var(&conf.mint, "mint", 0, "Minimum timestamp (in seconds) for carrying out data migration.")
	flag.Int64Var(&conf.maxt, "maxt", time.Now().Unix(), "Maximum timestamp (in seconds) for carrying out data migration. Setting this value less than zero will indicate all data from mint upto now. "+
		"Setting mint and maxt less than zero will migrate all data available in the read storage.")
	flag.StringVar(&conf.readURL, "read-url", "", "URL address for the storage where the data is to be read from.")
	flag.StringVar(&conf.writeURL, "write-url", "", "URL address for the storage where the data migration is to be written.")
	flag.StringVar(&conf.progressMetricName, "progress-metric-name", progressMetricName, "Prometheus metric name for tracking the last maximum timestamp pushed to the remote-write storage. "+
		"This is used to resume the migration process after a failure.")
	flag.StringVar(&conf.writerReadURL, "writer-read-url", "", "Read URL of the write storage. This is used to fetch the progress-metric that represents the last pushed maximum timestamp.")
	flag.BoolVar(&conf.progressEnabled, "progress-enabled", true, "This flag tells the migrator, whether or not to use the progress mechanism. It is helpful if you want to "+
		"carry out migration with the same time-range. Without this, the migrator will resume the migration from the last time, where it as stopped.")
	_ = flag.CommandLine.Parse(args)
	optimizeConf(conf)
}

func optimizeConf(conf *config) {
	// remote-storages tend to respond to time in milliseconds. So, we convert the received values in seconds to milliseconds.
	conf.mint *= 1000
	conf.maxt *= 1000
}

func validateConf(conf *config) error {
	switch {
	case conf.mint == 0:
		return fmt.Errorf("mint should be provided for the migration to begin")
	case conf.mint < 0:
		return fmt.Errorf("invalid mint: %d", conf.mint)
	case conf.maxt < 0:
		return fmt.Errorf("invalid maxt: %d", conf.maxt)
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
	case conf.mint > conf.maxt:
		return fmt.Errorf("invalid input: minimum timestamp value (mint) cannot be greater than the maximum timestamp value (maxt)")
	case conf.progressEnabled && strings.TrimSpace(conf.writerReadURL) == "":
		return fmt.Errorf("invalid input: read url for remote-write storage should be provided when progress metric is enabled. To disable progress metric, use -progress-enabled=false")
	}
	return nil
}
