// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package main

import (
	"flag"
	"fmt"
	"io/ioutil"
	"net/url"
	"os"
	"time"

	"gopkg.in/yaml.v2"

	"github.com/hashicorp/go-hclog"
	"github.com/jaegertracing/jaeger/plugin/storage/grpc"
	"github.com/jaegertracing/jaeger/plugin/storage/grpc/shared"
	"github.com/timescale/promscale/cmd/promscale-jaeger-plugin/plugin"
)

type config struct {
	Host    string        `yaml:"promscale-host,omitempty"`
	Port    int           `yaml:"promscale-port,omitempty"`
	Timeout time.Duration `yaml:"timeout,omitempty"`
}

var defaultConfig = &config{
	Host:    "http://localhost",
	Port:    9201,
	Timeout: time.Minute,
}

func main() {
	logger := hclog.New(&hclog.LoggerOptions{
		Level:      hclog.Warn,
		Name:       "promscale-jaeger-plugin",
		JSONFormat: true,
	})

	var configAddr string
	flag.StringVar(&configAddr, "config", "", "Configuration file address.")
	flag.Parse()

	bSlice, err := ioutil.ReadFile(configAddr)
	if err != nil {
		logger.Error("promscale-jaeger-plugin: Invalid configuration file address, exiting.")
		os.Exit(1)
	}

	conf := new(config)
	if err = yaml.Unmarshal(bSlice, conf); err != nil {
		logger.Error("promscale-jaeger-plugin: Unmarshalling configuration file contents", "err", err.Error())
		os.Exit(1)
	}

	logger.Warn("configuration", "conf", fmt.Sprintf("%v", conf))

	applyUnsetConfigFields(conf, defaultConfig)
	logger.Warn("host port", "conf", conf)

	parsedUrl, err := url.Parse(fmt.Sprintf("%s:%d", conf.Host, conf.Port))
	if err != nil {
		logger.Error("promscale-jaeger-plugin: Parsing configuration Url", "err", err.Error())
		os.Exit(1)
	}

	promscalePlugin := plugin.New(parsedUrl.String(), logger, conf.Timeout)
	grpc.Serve(&shared.PluginServices{Store: promscalePlugin})
}

func applyUnsetConfigFields(conf, defaultConf *config) {
	var (
		emptyString   string
		emptyInt      int
		emptyDuration time.Duration
	)
	if conf.Host == emptyString {
		conf.Host = defaultConf.Host
	}
	if conf.Port == emptyInt {
		conf.Port = defaultConf.Port
	}
	if conf.Timeout == emptyDuration {
		conf.Timeout = defaultConf.Timeout
	}
}
