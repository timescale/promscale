package bench

import (
	"fmt"
	"net/url"
	"os"
	"text/tabwriter"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	common_config "github.com/prometheus/common/config"
	"github.com/prometheus/common/promlog"
	"github.com/timescale/promscale/pkg/remote"
)

type qmInfo struct {
	qm         *remote.QueueManager
	samplesIn  *remote.EwmaRate
	samplesWal *remote.EwmaRate
	highestTs  *remote.MaxTimestamp
	ticker     *time.Ticker
}

func (qmi *qmInfo) run() {
	qmi.ticker = time.NewTicker(shardUpdateDuration)
	w := new(tabwriter.Writer)
	w.Init(os.Stdout, 8, 8, 1, '\t', tabwriter.AlignRight)
	for range qmi.ticker.C {
		qmi.samplesIn.Tick()
		qmi.samplesWal.Tick()
		fmt.Fprintf(w, "Samples in rate \t%.0f\tSamples wal rate\t%.0f\tSamples out rate\t%.0f\t\n", qmi.samplesIn.Rate(), qmi.samplesWal.Rate(), qmi.qm.SamplesOut.Rate())
		w.Flush()
	}
}

func (qmi *qmInfo) markStoppedSend() {
	//Stop the ticker to prevent in rate from going down after exhaustion.
	//this simulates a continued insert rate at the last rate from the point of view of dynamic shard logic
	qmi.ticker.Stop()
}

func getQM(conf *BenchConfig) (*qmInfo, error) {
	url, err := url.Parse(conf.WriteEndpoint)
	if err != nil {
		return nil, err
	}

	rwConf := conf.RemoteWriteConfig

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
		samplesIn:  remote.NewEwmaRate(ewmaWeight, shardUpdateDuration),
		samplesWal: remote.NewEwmaRate(ewmaWeight, shardUpdateDuration),
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
