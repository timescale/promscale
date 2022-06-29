package bench

import (
	"fmt"
	"net/url"
	"os"
	"sync/atomic"
	"text/tabwriter"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	common_config "github.com/prometheus/common/config"
	"github.com/prometheus/common/promlog"
	"github.com/timescale/promscale/pkg/remote"
)

type qmInfo struct {
	qm              *remote.QueueManager
	samplesIn       *remote.EwmaRate
	samplesWal      *remote.EwmaRate
	highestTs       *remote.MaxTimestamp
	ticker          *time.Ticker
	metricsRegistry *prometheus.Registry
	timeLagRealMax  int64
}

func (qmi *qmInfo) run() {
	qmi.ticker = time.NewTicker(shardUpdateDuration)
	w := new(tabwriter.Writer)
	w.Init(os.Stdout, 8, 8, 1, '\t', tabwriter.AlignRight)
	for range qmi.ticker.C {
		qmi.samplesIn.Tick()
		qmi.samplesWal.Tick()

		mfs, err := qmi.metricsRegistry.Gather()
		if err != nil {
			fmt.Printf("Error Gathering metrics from registry")
		}

		timeLagSecondsReal := int64(0)
		timeLagSecondsRel := int64(0)
		for _, mf := range mfs {
			if mf.GetName() == "prometheus_remote_storage_queue_highest_sent_timestamp_seconds" {
				highestSent := mf.GetMetric()[0].GetGauge().GetValue()
				now := time.Now().Unix()

				highestAppended := qmi.highestTs.Get()

				timeLagSecondsReal = now - int64(highestSent)
				timeLagSecondsRel = int64(highestAppended) - int64(highestSent)

				if timeLagSecondsReal > qmi.timeLagRealMax {
					qmi.timeLagRealMax = timeLagSecondsReal
				}
			}
		}

		needChunks := atomic.LoadInt32(&needNextChunks)
		fetchChunks := atomic.LoadInt32(&fetchChunks)
		asyncFetchChunks := atomic.LoadInt32(&asyncFetchChunks)
		enqueued := atomic.LoadInt32(&enqueued)
		dequeued := atomic.LoadInt32(&dequeued)

		fmt.Fprintf(w, "Samples in rate \t%.0f\tSamples wal rate\t%.0f\tSamples out rate\t%.0f\tTime Lag (s)\t%d[%d]\t %d %d %d %d %d %d %d %d %d %d\n", qmi.samplesIn.Rate(), qmi.samplesWal.Rate(), qmi.qm.SamplesOut.Rate(), timeLagSecondsReal, timeLagSecondsRel,
			needChunks-fetchChunks, enqueued-dequeued, atomic.LoadInt32(&syncFetchChunks), atomic.LoadInt32(&waitInRotate), fetchChunks-asyncFetchChunks, needNextChunks-enqueued, enqueued, dequeued, atomic.LoadInt32(&asyncFetchChunks), atomic.LoadInt32(&rateControlSleeps))

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

	reg := prometheus.NewRegistry()

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
		metricsRegistry: reg,
	}

	logLevel := &promlog.AllowedLevel{}
	err = logLevel.Set("info")
	if err != nil {
		return nil, err
	}

	qmi.qm = remote.NewQueueManager(
		remote.NewQueueManagerMetrics(reg, "", ""),
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
