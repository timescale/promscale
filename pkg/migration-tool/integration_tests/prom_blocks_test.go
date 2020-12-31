package integration_tests

import (
	"context"
	"io/ioutil"
	"testing"
	"time"

	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/prompb"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/stretchr/testify/assert"
)

const blockDuration = int64(1 * time.Minute / time.Millisecond)

func generatePromTSDBBlocks(t *testing.T, ts []prompb.TimeSeries) (promTSDBDirPath string) {
	promTSDBDirPath, err := ioutil.TempDir("", "ps_tests")
	assert.NoError(t, err)
	opts := tsdb.DefaultOptions()
	opts.WALCompression = false
	opts.MinBlockDuration = blockDuration
	opts.MaxBlockDuration = blockDuration
	opts.RetentionDuration = int64(30 * 12 * 30 * 24 * time.Hour / time.Millisecond)
	db, err := tsdb.Open(promTSDBDirPath, nil, nil, opts)
	assert.NoError(t, err)
	defer db.Close()
	app := db.Appender(context.Background())
	hashes := make([]uint64, len(ts))
	for i := 0; i < len(ts[0].Samples); i++ {
		for j, s := range ts {
			if hashes[j] == 0 {
				ref, err := app.Add(promblabelsToLabels(s.Labels), s.Samples[i].Timestamp, s.Samples[i].Value)
				assert.NoError(t, err)
				hashes[j] = ref
				continue
			}
			assert.NoError(t, app.AddFast(hashes[j], s.Samples[i].Timestamp, s.Samples[i].Value))
		}
	}
	assert.NoError(t, app.Commit())
	return
}

func promblabelsToLabels(lbs []prompb.Label) labels.Labels {
	l := make(labels.Labels, len(lbs))
	for i, lbl := range lbs {
		l[i].Name = lbl.Name
		l[i].Value = lbl.Value
	}
	return l
}
