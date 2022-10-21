package end_to_end_tests

import (
	"context"
	"fmt"
	"os"
	"runtime"

	"github.com/prometheus/prometheus/model/exemplar"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/timescale/promscale/pkg/prompb"
)

// generatePrometheusWAL generates Prometheus WAL with exemplars. It returns the path where
// the WAL was generated along with exact timeseries which can be used by caller to
// ingest elsewhere, where the requirement of timeseries has to be same with the ones
// used to generate WAL.
func generatePrometheusWAL(withExemplars bool) ([]prompb.TimeSeries, string, error) {
	tmpDir := ""

	if runtime.GOOS == "darwin" {
		// Docker on Mac lacks access to default os tmp dir - "/var/folders/random_number"
		// so switch to cross-user tmp dir
		tmpDir = "/tmp"
	}
	dbStoragePath := "prom_db_test_storage"
	if withExemplars {
		dbStoragePath = dbStoragePath + "_with_exemplars"
	}
	dbPath, err := os.MkdirTemp(tmpDir, dbStoragePath)
	if err != nil {
		return nil, "", err
	}
	var snapPath string
	if !withExemplars {
		// Take snapshots only when exemplars are not to be inserted.
		// Otherwise, snapshots will remove them.
		snapPath, err = os.MkdirTemp(tmpDir, "prom_snaptest_storage")
		if err != nil {
			return nil, "", err
		}
	}

	st, err := tsdb.Open(dbPath, nil, nil, &tsdb.Options{
		RetentionDuration:     15 * 24 * 60 * 60 * 1000, // 15 days in milliseconds
		NoLockfile:            true,
		EnableExemplarStorage: true,
		MaxExemplars:          10000000,
	}, nil)
	if err != nil {
		return nil, "", err
	}

	var (
		app = st.Appender(context.Background())
		tts []prompb.TimeSeries
	)
	if withExemplars {
		tts = generateRecentLargeTimeseries()
	} else {
		tts = generateLargeTimeseries()
		if *extendedTest {
			// Only apply generatedRealTimeseries for non-exemplar based
			// Prometheus WAL, since these are not required for exemplar based tests.
			tts = append(tts, generateRealTimeseries()...)
		}
	}
	var copyTts []prompb.TimeSeries
	if withExemplars {
		tts = insertExemplars(tts, 2)
		copyTts = tts[:]
	}

	var ref *storage.SeriesRef
	appendExemplar := func(app storage.Appender, ref storage.SeriesRef, lbls labels.Labels, e prompb.Exemplar) error {
		if _, err := app.AppendExemplar(ref, lbls, prompbExemplarToExemplar(e)); err != nil {
			return fmt.Errorf("append exemplar: %w", err)
		}
		return nil
	}

	for _, ts := range tts {
		ref = nil
		builder := labels.Builder{}

		for _, l := range ts.Labels {
			builder.Set(l.Name, l.Value)
		}

		var (
			lbls    = builder.Labels(nil)
			tempRef storage.SeriesRef
			err     error
		)

		for i, s := range ts.Samples {
			if ref == nil || *ref == 0 {
				tempRef, err = app.Append(tempRef, lbls, s.Timestamp, s.Value)
				if err != nil {
					return nil, "", err
				}
				if withExemplars && i < len(ts.Exemplars) {
					if err = appendExemplar(app, tempRef, lbls, ts.Exemplars[i]); err != nil {
						return nil, "", err
					}
				}
				ref = &tempRef
				continue
			}

			if withExemplars && i < len(ts.Exemplars) {
				if err = appendExemplar(app, tempRef, lbls, ts.Exemplars[i]); err != nil {
					return nil, "", err
				}
			}
			_, err = app.Append(*ref, lbls, s.Timestamp, s.Value)

			if err != nil {
				return nil, "", err
			}
		}
	}

	if err := app.Commit(); err != nil {
		return nil, "", err
	}
	if !withExemplars {
		if err := st.Snapshot(snapPath, true); err != nil {
			return nil, "", err
		}
		if err := os.Mkdir(snapPath+"/wal", 0700); err != nil {
			return nil, "", err
		}
		if err := st.Close(); err != nil {
			return nil, "", err
		}
	}

	return copyTts, dbPath, nil
}

func prompbExemplarToExemplar(pe prompb.Exemplar) exemplar.Exemplar {
	exemplarLabelsBuilder := labels.Builder{}
	for _, el := range pe.Labels {
		exemplarLabelsBuilder.Set(el.Name, el.Value)
	}
	return exemplar.Exemplar{
		Labels: exemplarLabelsBuilder.Labels(nil),
		Value:  pe.Value,
		Ts:     pe.Timestamp,
		HasTs:  true,
	}
}
