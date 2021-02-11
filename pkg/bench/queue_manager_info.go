package bench

import (
	"fmt"
	"net/url"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	common_config "github.com/prometheus/common/config"
	"github.com/prometheus/common/promlog"
	"github.com/prometheus/prometheus/config"
	"github.com/timescale/promscale/pkg/remote"
)

type qmInfo struct {
	qm        *remote.QueueManager
	samplesIn *remote.EwmaRate
	highestTs *remote.MaxTimestamp
}

func (qmi *qmInfo) run() {
	ticker := time.NewTicker(shardUpdateDuration)
	defer ticker.Stop()
	for range ticker.C {
		qmi.samplesIn.Tick()
		fmt.Println("Samples in rate", qmi.samplesIn.Rate(), "Samples out rate", qmi.qm.SamplesOut.Rate())
	}
}

func getQM(conf *BenchConfig) (*qmInfo, error) {
	url, err := url.Parse(conf.WriteEndpoint)
	if err != nil {
		return nil, err
	}

	rwConf := config.DefaultRemoteWriteConfig

	rwConf.URL = &common_config.URL{
		URL: url,
	}
	rwConf.HTTPClientConfig = common_config.HTTPClientConfig{}

	c, err := remote.NewWriteClient("client", &remote.ClientConfig{
		URL:              rwConf.URL,
		Timeout:          rwConf.RemoteTimeout,
		HTTPClientConfig: rwConf.HTTPClientConfig,
	})
	if err != nil {
		return nil, err
	}

	qmi := &qmInfo{
		samplesIn: remote.NewEwmaRate(ewmaWeight, shardUpdateDuration),
		highestTs: remote.NewMaxTimestamp(
			prometheus.NewGauge(prometheus.GaugeOpts{
				Namespace: "ns",
				Subsystem: "ss",
				Name:      "highest_timestamp_in_seconds",
				Help:      "Highest timestamp that has come into the remote storage via the Appender interface, in seconds since epoch.",
			})),
	}

	logLevel := &promlog.AllowedLevel{}
	err = logLevel.Set("info")
	if err != nil {
		return nil, err
	}

	qmi.qm = remote.NewQueueManager(
		remote.NewQueueManagerMetrics(nil, "", ""),
		nil,
		nil,
		promlog.New(&promlog.Config{Level: logLevel}),
		"",
		qmi.samplesIn,
		rwConf.QueueConfig,
		rwConf.MetadataConfig,
		nil,
		nil,
		c,
		defaultFlushDeadline,
		remote.NewPool(),
		qmi.highestTs,
		nil,
	)

	go qmi.run()
	return qmi, nil
}
