package main

import (
	"flag"
	"fmt"
	"os"
	"strings"
	"sync"

	"github.com/timescale/promscale/pkg/log"
	plan "github.com/timescale/promscale/pkg/migration-tool/planner"
	"github.com/timescale/promscale/pkg/migration-tool/reader"
	"github.com/timescale/promscale/pkg/migration-tool/writer"
)

type config struct {
	name                 string
	mint                 int64
	maxt                 int64
	readURL              string
	writeURL             string
	writerReadURL        string
	ignoreProgressMetric bool
	progressMetricName   string
}

func main() {
	conf := new(config)
	parseFlags(conf, os.Args[1:])
	if err := log.Init(log.Config{Format: "logfmt", Level: "debug"}); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	log.Info("msg", fmt.Sprintf("%v+", conf))
	if err := validateConf(conf); err != nil {
		log.Error("msg", "could not parse flags", "error", err)
		os.Exit(1)
	}

	planner, proceed, err := plan.CreatePlan(conf.mint, conf.maxt, conf.progressMetricName, conf.writerReadURL, conf.ignoreProgressMetric)
	if err != nil {
		log.Error("msg", "could not create plan", "error", err)
		os.Exit(2)
	}
	if !proceed {
		os.Exit(0)
	}

	var (
		wg            sync.WaitGroup
		sigBlockRead  = make(chan *plan.Block)
		sigBlockWrite = make(chan struct{})
	)
	read, err := reader.New(conf.readURL, planner, sigBlockRead, sigBlockWrite)
	if err != nil {
		log.Error("msg", "could not create reader", "error", err)
	}
	write, err := writer.New(conf.writeURL, conf.progressMetricName, conf.name, planner, sigBlockRead, sigBlockWrite)
	if err != nil {
		log.Error("msg", "could not create writer", "error", err)
	}

	wg.Add(2)
	go func() {
		defer wg.Done()
		if err = read.Run(); err != nil {
			log.Error("msg", fmt.Errorf("running reader: %w", err).Error())
			os.Exit(2)
		}
	}()
	go func() {
		defer wg.Done()
		if err = write.Run(); err != nil {
			log.Error("msg", fmt.Errorf("running writer: %w", err).Error())
			os.Exit(2)
		}
	}()
	wg.Wait()
	log.Info("msg", "exiting!")
}

func parseFlags(conf *config, args []string) {
	flag.StringVar(&conf.name, "name", "prom-migrator-default", "Name for the current migration that is to be carried out. It corresponds to the value of the label 'migration-job' set inside the progress-metric-name."+
		"It defaults to 'prom-migrator-default'.")
	flag.Int64Var(&conf.mint, "mint", -1, "Minimum timestamp for carrying out data migration. Setting this value less than zero will indicate all data upto the maxt. "+
		"Setting mint and maxt less than zero will migrate all data available in the read storage.")
	flag.Int64Var(&conf.maxt, "maxt", -1, "Maximum timestamp for carrying out data migration. Setting this value less than zero will indicate all data from mint upto now. "+
		"Setting mint and maxt less than zero will migrate all data available in the read storage.")
	flag.StringVar(&conf.readURL, "read-url", "", "URL address for the storage where the data is to be read from.")
	flag.StringVar(&conf.writeURL, "write-url", "", "URL address for the storage where the data migration is to be written.")
	flag.StringVar(&conf.progressMetricName, "progress-metric-name", "progress_metric", "Prometheus metric name for tracking the last maximum timestamp pushed to the remote-write storage. "+
		"This is used to resume the migration process after a failure.")
	flag.StringVar(&conf.writerReadURL, "writer-read-url", "", "Read URL of the write storage. This is used to fetch the progress-metric that represents the last pushed maximum timestamp.")
	flag.BoolVar(&conf.ignoreProgressMetric, "ignore-progress", false, "This flag tells the migrator, whether or not to ignore the progress-metric. It is helpful if you want to "+
		"carry out migration with the same time-range. Without this, the migrator will resume the migration from the last time, where it as stopped.")
	_ = flag.CommandLine.Parse(args)
}

func validateConf(conf *config) error {
	switch {
	case strings.TrimSpace(conf.readURL) == "" && strings.TrimSpace(conf.writeURL) == "":
		return fmt.Errorf("remote read storage url and remote write storage url must be specified. Without these, data migration cannot begin")
	case strings.TrimSpace(conf.readURL) == "":
		return fmt.Errorf("remote read storage url needs to be specified. Without read storage url, data migration cannot begin")
	case strings.TrimSpace(conf.writeURL) == "":
		return fmt.Errorf("remote write storage url needs to be specified. Without write storage url, data migration cannot begin")
	case conf.mint > conf.maxt:
		return fmt.Errorf("invalid input: minimum timestamp value (mint) cannot be greater than the maximum timestamp value (maxt)")
	}
	return nil
}
