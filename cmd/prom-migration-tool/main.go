package main

import (
	"flag"
	"fmt"
	plan "github.com/timescale/promscale/pkg/migration-tool/planner"
	"github.com/timescale/promscale/pkg/migration-tool/reader"
	"github.com/timescale/promscale/pkg/migration-tool/writer"
	"go.uber.org/atomic"
	"os"
	"sync"
)

type config struct {
	mint         int64
	maxt         int64
	readURL      string
	writeURL     string
	plannerPath  string
	inMemoryOnly bool
}

func main() {
	conf := &config{}
	flag.Int64Var(&conf.mint, "mint", -1, "Minimum timestamp for carrying out data migration. Setting this value less than zero will indicate all data upto the maxt. Setting mint and maxt less than zero will migrate all data available in the read storage.")
	flag.Int64Var(&conf.maxt, "maxt", -1, "Maximum timestamp for carrying out data migration. Setting this value less than zero will indicate all data from mint upto now. Setting mint and maxt less than zero will migrate all data available in the read storage.")
	flag.StringVar(&conf.readURL, "read-url", "", "URL address for the storage where the data is to be read from.")
	flag.StringVar(&conf.writeURL, "write-url", "", "URL address for the storage where the data migration is to be written.")
	flag.Parse()
	fmt.Fprintln(os.Stdout, conf)
	if err := validateConf(conf); err != nil {
		fmt.Fprintln(os.Stderr, fmt.Errorf("parsing flags: %w", err).Error())
		os.Exit(1)
	}
	planner, err := plan.CreatePlan(conf.mint, conf.maxt)
	if err != nil {
		fmt.Fprintln(os.Stderr, fmt.Errorf("create-plan: %w", err).Error())
		os.Exit(2)
	}
	var isReaderUp atomic.Bool
	sigBlockRead := make(chan struct{})
	sigBlockWrite := make(chan struct{})
	fmt.Println("one")
	fmt.Println("two")
	read, err := reader.New(conf.readURL, planner, sigBlockRead, sigBlockWrite)
	if err != nil {
		fmt.Fprintln(os.Stderr, fmt.Errorf("creating reader: %w", err).Error())
	}
	write, err := writer.New(conf.writeURL, planner, sigBlockRead, sigBlockWrite)
	if err != nil {
		fmt.Fprintln(os.Stderr, fmt.Errorf("creating writer: %w", err).Error())
	}
	//go func() {
	//	for {
	//		sigBlockWrite <- struct{}{}
	//		time.Sleep(time.Second * 1)
	//	}
	//}()
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		fmt.Println("launching reader")
		if err = read.Run(&wg, &isReaderUp); err != nil {
			fmt.Fprintln(os.Stderr, fmt.Errorf("running reader: %w", err).Error())
			os.Exit(2)
		}
	}()
	go func() {
		if err = write.Run(&wg, &isReaderUp); err != nil {
			fmt.Fprintln(os.Stderr, fmt.Errorf("running writer: %w", err).Error())
			os.Exit(2)
		}
	}()
	sigBlockWrite <- struct{}{}
	wg.Wait()
	fmt.Println("completed, exiting")
}

func validateConf(conf *config) error {
	if conf.readURL == "" {
		return fmt.Errorf("remote read storage url needs to be specified. Without read storage url, data migration cannot begin")
	}
	//if conf.writeURL == "" {
	//	return fmt.Errorf("remote write storage url needs to be specified. Without write storage url, data migration cannot begin")
	//}
	return nil
}
