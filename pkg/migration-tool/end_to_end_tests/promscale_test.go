package end_to_end_tests

import (
	"fmt"
	"math"
	"net/http/httptest"
	"os"
	"sync"
	"testing"

	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/prometheus/prometheus/prompb"
	"github.com/prometheus/prometheus/util/testutil"
	"github.com/timescale/promscale/pkg/api"
	plan "github.com/timescale/promscale/pkg/migration-tool/planner"
	"github.com/timescale/promscale/pkg/migration-tool/reader"
	"github.com/timescale/promscale/pkg/migration-tool/writer"
	"github.com/timescale/promscale/pkg/pgclient"
	"github.com/timescale/promscale/pkg/util"
)

func TestMigrationWithinMultiplePromscaleDB(t *testing.T) {
	fmt.Println("ran")
	config := &pgclient.Config{
		AsyncAcks:       false,
		ReportInterval:  0,
		SeriesCacheSize: 0,
	}
	mockMetrics := api.InitMetrics()
	elector := new(util.Elector)
	withDB(t, "promscale-read-db", func(db *pgxpool.Pool, t testing.TB) {
		ts := generateRealTimeseries()
		mint, maxt := getMinMax(ts)
		//fmt.Println("mint-global", mint, "maxt-global", maxt)
		//ingestor, err := pgmodel.NewPgxIngestor(db)
		//testutil.Ok(t, err)
		//defer ingestor.Close()
		//_, err = ingestor.Ingest(copyMetrics(ts), pgmodel.NewWriteRequest())
		//testutil.Ok(t, err)
		fmt.Println("passed here")

		readClient, err := pgclient.NewClientWithPool(config, 30, db)
		testutil.Ok(t, err)
		readHandler := api.Read(readClient, mockMetrics)
		remoteReadStorageServer := httptest.NewServer(readHandler)
		defer remoteReadStorageServer.Close()
		withDB(t, "promscale-write-db", func(db2 *pgxpool.Pool, t testing.TB) {
			writeClient, err := pgclient.NewClientWithPool(config, 30, db2)
			testutil.Ok(t, err)
			writeHandler := api.Write(writeClient, elector, mockMetrics)
			remoteWriteStorageServer := httptest.NewServer(writeHandler)
			defer remoteWriteStorageServer.Close()

			conf := struct {
				name                 string
				mint                 int64
				maxt                 int64
				readURL              string
				writeURL             string
				writerReadURL        string
				progressMetricName   string
				ignoreProgressMetric bool
			}{
				name:                 "ci-migration",
				mint:                 mint,
				maxt:                 maxt,
				readURL:              fmt.Sprintf("%s/read", remoteReadStorageServer.URL),
				writeURL:             fmt.Sprintf("%s/write", remoteWriteStorageServer.URL),
				writerReadURL:        fmt.Sprintf("%s/read", remoteWriteStorageServer.URL),
				progressMetricName:   "progress_metric",
				ignoreProgressMetric: false,
			}

			var (
				wg            sync.WaitGroup
				sigBlockRead  = make(chan *plan.Block)
				sigBlockWrite = make(chan struct{})
			)

			planner, _, err := plan.CreatePlan(conf.mint, conf.maxt, conf.progressMetricName, conf.writerReadURL, conf.ignoreProgressMetric)
			testutil.Ok(t, err)
			read, err := reader.New(conf.readURL, planner, sigBlockRead, sigBlockWrite)
			testutil.Ok(t, err)
			write, err := writer.New(conf.writeURL, conf.progressMetricName, conf.name, planner, sigBlockRead, sigBlockWrite)
			testutil.Ok(t, err)
			wg.Add(2)
			go func() {
				defer wg.Done()
				if err = read.Run(); err != nil {
					fmt.Fprintln(os.Stderr, fmt.Errorf("running reader: %w", err).Error())
					t.Fail()
				}
			}()
			go func() {
				defer wg.Done()
				if err = write.Run(); err != nil {
					fmt.Fprintln(os.Stderr, fmt.Errorf("running writer: %w", err).Error())
					t.Fail()
				}
			}()
			wg.Wait()
		})
	})
}

func getMinMax(ts []prompb.TimeSeries) (mint, maxt int64) {
	mint = math.MaxInt64
	maxt = -1

	for _, s := range ts {
		for _, sample := range s.Samples {
			if sample.Timestamp > maxt {
				maxt = sample.Timestamp
			}
			if sample.Timestamp < mint {
				mint = sample.Timestamp
			}
		}
	}
	return
}
