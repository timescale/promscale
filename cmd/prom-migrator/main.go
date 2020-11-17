package main

import (
	"flag"
	"fmt"
	"go.uber.org/atomic"
	"os"
	"strings"
	"sync"

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
	progressMetricName string
}

func main() {
	conf := new(config)
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
	flag.Parse()
	if err := log.Init(log.Config{Format: "logfmt", Level: "debug"}); err != nil {
		fmt.Fprintln(os.Stderr, err)
	}
	log.Info("msg", fmt.Sprintf("%v+", conf))
	if err := validateConf(conf); err != nil {
		log.Error("msg", fmt.Errorf("parsing flags: %w", err).Error())
		os.Exit(1)
	}
	planner, proceed, err := plan.CreatePlan(conf.mint, conf.maxt, conf.progressMetricName, conf.writerReadURL)
	if err != nil {
		log.Error("msg", fmt.Errorf("create-plan: %w", err).Error())
		os.Exit(2)
	}
	if !proceed {
		os.Exit(1)
	}
	var (
		isReaderUp    atomic.Bool
		sigBlockRead  = make(chan struct{})
		sigBlockWrite = make(chan struct{})
	)
	read, err := reader.New(conf.readURL, planner, sigBlockRead, sigBlockWrite)
	if err != nil {
		log.Error("msg", fmt.Errorf("creating reader: %w", err).Error())
	}
	write, err := writer.New(conf.writeURL, conf.progressMetricName, conf.name, planner, sigBlockRead, sigBlockWrite)
	if err != nil {
		log.Error("msg", fmt.Errorf("creating writer: %w", err).Error())
	}
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		if err = read.Run(&wg, &isReaderUp); err != nil {
			log.Error("msg", fmt.Errorf("running reader: %w", err).Error())
			os.Exit(2)
		}
	}()
	go func() {
		if err = write.Run(&wg, &isReaderUp); err != nil {
			log.Error("msg", fmt.Errorf("running writer: %w", err).Error())
			os.Exit(2)
		}
	}()
	wg.Wait()
	log.Info("msg", "exiting!")
}

func validateConf(conf *config) error {
	switch {
	case strings.TrimSpace(conf.readURL) == "":
		return fmt.Errorf("remote read storage url needs to be specified. Without read storage url, data migration cannot begin")
	case strings.TrimSpace(conf.writeURL) == "":
		return fmt.Errorf("remote write storage url needs to be specified. Without write storage url, data migration cannot begin")
	case conf.mint > conf.maxt:
		return fmt.Errorf("invalid input: minimum timestamp value (mint) cannot be greater than the maximum timestamp value (maxt)")
	}
	return nil
}
