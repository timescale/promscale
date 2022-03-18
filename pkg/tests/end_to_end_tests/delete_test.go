// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package end_to_end_tests

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sort"
	"strconv"
	"testing"
	"time"

	"github.com/jackc/pgconn"
	"github.com/jackc/pgerrcode"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/stretchr/testify/require"
	"github.com/timescale/promscale/pkg/internal/testhelpers"
	pgDel "github.com/timescale/promscale/pkg/pgmodel/delete"
	ingstr "github.com/timescale/promscale/pkg/pgmodel/ingestor"
	"github.com/timescale/promscale/pkg/pgmodel/model"
	"github.com/timescale/promscale/pkg/pgxconn"
)

type deleteStr struct {
	name           string
	matchers       string
	expectedReturn string
	start          string
	end            string
}

func TestDeleteWithMetricNameEQL(t *testing.T) {
	if *useMultinode && !*extendedTest {
		t.Skip("delete tests run in extended mode only for multi-node configuration")
	}
	matchers := []deleteStr{
		// Normal matchers.
		{
			name:           "demo_api_http_requests_in_progress",
			matchers:       `{__name__="demo_api_http_requests_in_progress"}`,
			expectedReturn: `[demo_api_http_requests_in_progress] 3 2314 0`,
		},
		{
			name:           "demo_disk_total_bytes",
			matchers:       `{__name__="demo_disk_total_bytes"}`,
			expectedReturn: `[demo_disk_total_bytes] 3 2314 0`,
		},
		{
			name:           "go_memstats_buck_hash_sys_bytes",
			matchers:       `{__name__="go_memstats_buck_hash_sys_bytes"}`,
			expectedReturn: `[go_memstats_buck_hash_sys_bytes] 3 2314 0`,
		},
		{
			name:           "http_request_duration_microseconds_count",
			matchers:       `{__name__="http_request_duration_microseconds_count"}`,
			expectedReturn: `[http_request_duration_microseconds_count] 3 2314 0`,
		},
		{
			name:           "go_threads",
			matchers:       `{__name__="go_threads"}`,
			expectedReturn: `[go_threads] 3 2314 0`,
		},
		{
			name:           "scrape_series_added",
			matchers:       `{__name__="scrape_series_added"}`,
			expectedReturn: `[scrape_series_added] 3 2314 0`,
		},
		{
			name:           "up",
			matchers:       `{__name__="up"}`,
			expectedReturn: `[up] 3 2314 0`,
		},
		{
			name:           "demo_api_request_duration_seconds_bucket",
			matchers:       `{__name__="demo_api_request_duration_seconds_bucket"}`,
			expectedReturn: `[demo_api_request_duration_seconds_bucket] 754 581568 0`,
		},
	}

	matchersSmall := []deleteStr{
		{
			name:           "metric_1",
			matchers:       `{__name__="metric_1"}`,
			expectedReturn: `[metric_1] 3 5001 0`,
		},
		{
			name:           "metric_3",
			matchers:       `{__name__="metric_3"}`,
			expectedReturn: `[metric_3] 2 3334 0`,
		},
	}

	withDB(t, *testDatabase, func(dbOwner *pgxpool.Pool, t testing.TB) {
		db := testhelpers.PgxPoolWithRole(t, *testDatabase, "prom_modifier")
		defer db.Close()

		ts := generateLargeTimeseries()
		if *extendedTest {
			ts = generateRealTimeseries()
		}

		ingestor, err := ingstr.NewPgxIngestorForTests(pgxconn.NewPgxConn(db), nil)
		if err != nil {
			t.Fatal(err)
		}
		defer ingestor.Close()
		if _, _, err := ingestor.Ingest(context.Background(), newWriteRequestWithTs(copyMetrics(ts))); err != nil {
			t.Fatal(err)
		}
		err = ingestor.CompleteMetricCreation(context.Background())
		if err != nil {
			t.Fatal(err)
		}

		pgDelete := &pgDel.PgDelete{Conn: pgxconn.NewPgxConn(db)}
		if !*extendedTest {
			matchers = matchersSmall
		}
		for _, m := range matchers {
			var countBeforeDelete, countAfterDelete int
			matcher, err := getMatchers(m.matchers)
			require.NoError(t, err)
			parsedStartTime, err := parseTime(m.start, model.MinTime)
			require.NoError(t, err)
			parsedEndTime, err := parseTime(m.end, model.MaxTime)
			require.NoError(t, err)
			err = db.QueryRow(context.Background(), fmt.Sprintf("select count(*) from prom_data.\"%s\"", m.name)).Scan(&countBeforeDelete)
			require.NoError(t, err)
			touchedMetrics, deletedSeriesIDs, _, err := pgDelete.DeleteSeries(matcher, parsedStartTime, parsedEndTime)
			require.NoError(t, err)
			err = db.QueryRow(context.Background(), fmt.Sprintf("select count(*) from prom_data.\"%s\"", m.name)).Scan(&countAfterDelete)
			require.NoError(t, err)
			require.Equal(t, m.expectedReturn, fmt.Sprintf("%v %v %v %v", touchedMetrics, len(deletedSeriesIDs), countBeforeDelete, countAfterDelete), "expected returns does not match in", m.name)
			require.True(t, countBeforeDelete != countAfterDelete, "samples count should not be similar: before %d | after %d in", countBeforeDelete, countAfterDelete, m.name)
		}
	})
}

func TestDeleteWithCompressedChunks(t *testing.T) {
	if *useMultinode && !*extendedTest {
		t.Skip("delete tests run in extended mode only for multi-node configuration")
	}
	if !*useTimescaleDB {
		t.Skip("skipping delete tests with compression: compression tests cannot run if timescaledb is not installed.")
	}
	matchers := []deleteStr{
		// Normal matchers.
		{
			name:           "demo_api_http_requests_in_progress",
			matchers:       `{__name__="demo_api_http_requests_in_progress"}`,
			expectedReturn: `[demo_api_http_requests_in_progress] 3`,
		},
		{
			name:           "demo_disk_total_bytes",
			matchers:       `{__name__="demo_disk_total_bytes"}`,
			expectedReturn: `[demo_disk_total_bytes] 3`,
		},
		{
			name:           "go_memstats_buck_hash_sys_bytes",
			matchers:       `{__name__="go_memstats_buck_hash_sys_bytes"}`,
			expectedReturn: `[go_memstats_buck_hash_sys_bytes] 3`,
		},
		{
			name:           "http_request_duration_microseconds_count",
			matchers:       `{__name__="http_request_duration_microseconds_count"}`,
			expectedReturn: `[http_request_duration_microseconds_count] 3`,
		},
		{
			name:           "go_threads",
			matchers:       `{__name__="go_threads"}`,
			expectedReturn: `[go_threads] 3`,
		},
		{
			name:           "scrape_series_added",
			matchers:       `{__name__="scrape_series_added"}`,
			expectedReturn: `[scrape_series_added] 3`,
		},
		{
			name:           "up",
			matchers:       `{__name__="up"}`,
			expectedReturn: `[up] 3`,
		},
		{
			name:           "demo_api_request_duration_seconds_bucket",
			matchers:       `{__name__="demo_api_request_duration_seconds_bucket"}`,
			expectedReturn: `[demo_api_request_duration_seconds_bucket] 754`,
		},
	}

	matchersSmall := []deleteStr{
		{
			name:           "metric_1",
			matchers:       `{__name__="metric_1"}`,
			expectedReturn: `[metric_1] 3`,
		},
		{
			name:           "metric_2",
			matchers:       `{__name__="metric_2"}`,
			expectedReturn: `[metric_2] 3`,
		},
	}

	withDB(t, *testDatabase, func(dbOwner *pgxpool.Pool, t testing.TB) {
		db := testhelpers.PgxPoolWithRole(t, *testDatabase, "prom_modifier")
		defer db.Close()

		ts := generateLargeTimeseries()
		if *extendedTest {
			ts = generateRealTimeseries()
		}
		ingestor, err := ingstr.NewPgxIngestorForTests(pgxconn.NewPgxConn(db), nil)
		if err != nil {
			t.Fatal(err)
		}
		defer ingestor.Close()
		if _, _, err := ingestor.Ingest(context.Background(), newWriteRequestWithTs(copyMetrics(ts))); err != nil {
			t.Fatal(err)
		}
		err = ingestor.CompleteMetricCreation(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if !*extendedTest {
			matchers = matchersSmall
		}
		for _, m := range matchers {
			var tableName string
			err = dbOwner.QueryRow(context.Background(), "SELECT table_name from _prom_catalog.metric WHERE metric_name=$1", m.name).Scan(&tableName)
			require.NoError(t, err)
			_, err = dbOwner.Exec(context.Background(), fmt.Sprintf("SELECT compress_chunk(i) from show_chunks('prom_data.\"%s\"') i;", tableName))
			if err != nil {
				var pgErr *pgconn.PgError
				if errors.As(err, &pgErr) && pgErr.SQLState() == pgerrcode.DuplicateObject {
					// Already compressed (could happen if policy already ran). This is fine.
				} else {
					t.Fatal(err)
				}
			}
			pgDelete := &pgDel.PgDelete{Conn: pgxconn.NewPgxConn(db)}
			matcher, err := getMatchers(m.matchers)
			require.NoError(t, err)
			parsedStartTime, err := parseTime(m.start, model.MinTime)
			require.NoError(t, err)
			parsedEndTime, err := parseTime(m.end, model.MaxTime)
			require.NoError(t, err)
			touchedMetrics, deletedSeriesIDs, _, err := pgDelete.DeleteSeries(matcher, parsedStartTime, parsedEndTime)
			require.NoError(t, err)
			sort.Strings(touchedMetrics)
			require.Equal(t, m.expectedReturn, fmt.Sprintf("%v %v", touchedMetrics, len(deletedSeriesIDs)), "expected returns does not match in", m.name)
		}
	})
}

func TestDeleteWithMetricNameEQLRegex(t *testing.T) {
	if *useMultinode && !*extendedTest {
		t.Skip("delete tests run in extended mode only for multi-node configuration")
	}
	matchers := []deleteStr{
		// Normal regex matchers.
		{
			name:           "demo_api_http_requests_regex",
			matchers:       `{__name__=~"demo_api_http_requests.*"}`,
			expectedReturn: `[demo_api_http_requests_in_progress] 3`,
		},
		{
			name:           "demo_disk_regex",
			matchers:       `{__name__=~"demo_disk.*"}`,
			expectedReturn: `[demo_disk_total_bytes demo_disk_usage_bytes] 6`,
		},
		{
			name:           "go_memstats_regex",
			matchers:       `{__name__=~"go_memstat.*"}`,
			expectedReturn: `[go_memstats_alloc_bytes go_memstats_alloc_bytes_total go_memstats_buck_hash_sys_bytes go_memstats_frees_total go_memstats_gc_cpu_fraction go_memstats_gc_sys_bytes go_memstats_heap_alloc_bytes go_memstats_heap_idle_bytes go_memstats_heap_inuse_bytes go_memstats_heap_objects go_memstats_heap_released_bytes go_memstats_heap_sys_bytes go_memstats_last_gc_time_seconds go_memstats_lookups_total go_memstats_mallocs_total go_memstats_mcache_inuse_bytes go_memstats_mcache_sys_bytes go_memstats_mspan_inuse_bytes go_memstats_mspan_sys_bytes go_memstats_next_gc_bytes go_memstats_other_sys_bytes go_memstats_stack_inuse_bytes go_memstats_stack_sys_bytes go_memstats_sys_bytes] 72`,
		},
		{
			name:           "http_regex combined with go_regex",
			matchers:       `{__name__=~"http_.*|go_.*"}`,
			expectedReturn: `[go_gc_duration_seconds go_gc_duration_seconds_count go_gc_duration_seconds_sum go_goroutines go_memstats_alloc_bytes go_memstats_alloc_bytes_total go_memstats_buck_hash_sys_bytes go_memstats_frees_total go_memstats_gc_cpu_fraction go_memstats_gc_sys_bytes go_memstats_heap_alloc_bytes go_memstats_heap_idle_bytes go_memstats_heap_inuse_bytes go_memstats_heap_objects go_memstats_heap_released_bytes go_memstats_heap_sys_bytes go_memstats_last_gc_time_seconds go_memstats_lookups_total go_memstats_mallocs_total go_memstats_mcache_inuse_bytes go_memstats_mcache_sys_bytes go_memstats_mspan_inuse_bytes go_memstats_mspan_sys_bytes go_memstats_next_gc_bytes go_memstats_other_sys_bytes go_memstats_stack_inuse_bytes go_memstats_stack_sys_bytes go_memstats_sys_bytes go_threads http_request_duration_microseconds http_request_duration_microseconds_count http_request_duration_microseconds_sum http_request_size_bytes http_request_size_bytes_count http_request_size_bytes_sum http_requests_total http_response_size_bytes http_response_size_bytes_count http_response_size_bytes_sum] 147`,
		},
		{
			name:           "scrape_regex",
			matchers:       `{__name__=~"scrape_.*"}`,
			expectedReturn: `[scrape_duration_seconds scrape_samples_post_metric_relabeling scrape_samples_scraped scrape_series_added] 12`,
		},
		{
			name:           "demo_regex",
			matchers:       `{__name__=~"demo_.*"}`,
			expectedReturn: `[demo_api_http_requests_in_progress demo_api_request_duration_seconds_bucket demo_api_request_duration_seconds_count demo_api_request_duration_seconds_sum demo_batch_last_run_duration_seconds demo_batch_last_run_processed_bytes demo_batch_last_run_timestamp_seconds demo_batch_last_success_timestamp_seconds demo_cpu_usage_seconds_total demo_disk_total_bytes demo_disk_usage_bytes demo_num_cpus] 845`,
		},
		{
			name:           "delete_all_regex",
			matchers:       `{__name__=~".*"}`,
			expectedReturn: `[demo_api_http_requests_in_progress demo_api_request_duration_seconds_bucket demo_api_request_duration_seconds_count demo_api_request_duration_seconds_sum demo_batch_last_run_duration_seconds demo_batch_last_run_processed_bytes demo_batch_last_run_timestamp_seconds demo_batch_last_success_timestamp_seconds demo_cpu_usage_seconds_total demo_disk_total_bytes demo_disk_usage_bytes demo_num_cpus go_gc_duration_seconds go_gc_duration_seconds_count go_gc_duration_seconds_sum go_goroutines go_memstats_alloc_bytes go_memstats_alloc_bytes_total go_memstats_buck_hash_sys_bytes go_memstats_frees_total go_memstats_gc_cpu_fraction go_memstats_gc_sys_bytes go_memstats_heap_alloc_bytes go_memstats_heap_idle_bytes go_memstats_heap_inuse_bytes go_memstats_heap_objects go_memstats_heap_released_bytes go_memstats_heap_sys_bytes go_memstats_last_gc_time_seconds go_memstats_lookups_total go_memstats_mallocs_total go_memstats_mcache_inuse_bytes go_memstats_mcache_sys_bytes go_memstats_mspan_inuse_bytes go_memstats_mspan_sys_bytes go_memstats_next_gc_bytes go_memstats_other_sys_bytes go_memstats_stack_inuse_bytes go_memstats_stack_sys_bytes go_memstats_sys_bytes go_threads http_request_duration_microseconds http_request_duration_microseconds_count http_request_duration_microseconds_sum http_request_size_bytes http_request_size_bytes_count http_request_size_bytes_sum http_requests_total http_response_size_bytes http_response_size_bytes_count http_response_size_bytes_sum process_cpu_seconds_total process_max_fds process_open_fds process_resident_memory_bytes process_start_time_seconds process_virtual_memory_bytes scrape_duration_seconds scrape_samples_post_metric_relabeling scrape_samples_scraped scrape_series_added up] 1025`,
		},
	}

	matchersSmall := []deleteStr{
		{
			name:           "normal regex",
			matchers:       `{foo=~"ba.*"}`,
			expectedReturn: `[METRIC_4 metric_1 metric_2] 7`,
		},
		{
			name:           "two regex",
			matchers:       `{instance=~"1.*", foo=~"ba.+"}`,
			expectedReturn: `[metric_1 metric_2] 2`,
		},
	}

	withDB(t, *testDatabase, func(dbOwner *pgxpool.Pool, t testing.TB) {
		db := testhelpers.PgxPoolWithRole(t, *testDatabase, "prom_modifier")
		defer db.Close()

		ts := generateLargeTimeseries()
		if *extendedTest {
			ts = generateRealTimeseries()
		}
		ingestor, err := ingstr.NewPgxIngestorForTests(pgxconn.NewPgxConn(db), nil)
		if err != nil {
			t.Fatal(err)
		}
		defer ingestor.Close()
		if _, _, err := ingestor.Ingest(context.Background(), newWriteRequestWithTs(copyMetrics(ts))); err != nil {
			t.Fatal(err)
		}
		err = ingestor.CompleteMetricCreation(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		pgDelete := &pgDel.PgDelete{Conn: pgxconn.NewPgxConn(db)}
		if !*extendedTest {
			matchers = matchersSmall
		}
		for _, m := range matchers {
			matcher, err := getMatchers(m.matchers)
			require.NoError(t, err)
			parsedStartTime, err := parseTime(m.start, model.MinTime)
			require.NoError(t, err)
			parsedEndTime, err := parseTime(m.end, model.MaxTime)
			require.NoError(t, err)
			touchedMetrics, deletedSeriesIDs, _, err := pgDelete.DeleteSeries(matcher, parsedStartTime, parsedEndTime)
			require.NoError(t, err)
			sort.Strings(touchedMetrics)
			require.Equal(t, m.expectedReturn, fmt.Sprintf("%v %v", touchedMetrics, len(deletedSeriesIDs)), "expected returns does not match in", m.name)
			if *extendedTest && m.name == "delete_all_regex" {
				require.Equal(t, 1025, len(deletedSeriesIDs), "delete all series does not match in", m.name)
				require.Equal(t, 62, len(touchedMetrics), "delete all metrics does not match in", m.name)
			}
		}
	})
}

func TestDeleteMixins(t *testing.T) {
	if *useMultinode && !*extendedTest {
		t.Skip("delete tests run in extended mode only for multi-node configuration")
	}
	matchers := []deleteStr{
		// Normal regex matchers.
		{
			name:           "demo_instance",
			matchers:       `{job="demo", instance="demo.promlabs.com:10000"}`,
			expectedReturn: `[demo_api_http_requests_in_progress demo_api_request_duration_seconds_bucket demo_api_request_duration_seconds_count demo_api_request_duration_seconds_sum demo_batch_last_run_duration_seconds demo_batch_last_run_processed_bytes demo_batch_last_run_timestamp_seconds demo_batch_last_success_timestamp_seconds demo_cpu_usage_seconds_total demo_disk_total_bytes demo_disk_usage_bytes demo_num_cpus go_gc_duration_seconds go_gc_duration_seconds_count go_gc_duration_seconds_sum go_goroutines go_memstats_alloc_bytes go_memstats_alloc_bytes_total go_memstats_buck_hash_sys_bytes go_memstats_frees_total go_memstats_gc_cpu_fraction go_memstats_gc_sys_bytes go_memstats_heap_alloc_bytes go_memstats_heap_idle_bytes go_memstats_heap_inuse_bytes go_memstats_heap_objects go_memstats_heap_released_bytes go_memstats_heap_sys_bytes go_memstats_last_gc_time_seconds go_memstats_lookups_total go_memstats_mallocs_total go_memstats_mcache_inuse_bytes go_memstats_mcache_sys_bytes go_memstats_mspan_inuse_bytes go_memstats_mspan_sys_bytes go_memstats_next_gc_bytes go_memstats_other_sys_bytes go_memstats_stack_inuse_bytes go_memstats_stack_sys_bytes go_memstats_sys_bytes go_threads http_request_duration_microseconds http_request_duration_microseconds_count http_request_duration_microseconds_sum http_request_size_bytes http_request_size_bytes_count http_request_size_bytes_sum http_requests_total http_response_size_bytes http_response_size_bytes_count http_response_size_bytes_sum process_cpu_seconds_total process_max_fds process_open_fds process_resident_memory_bytes process_start_time_seconds process_virtual_memory_bytes scrape_duration_seconds scrape_samples_post_metric_relabeling scrape_samples_scraped scrape_series_added up] 379`,
		},
		{
			name:           "demo_instance_regex",
			matchers:       `{job="demo", instance=~"demo.promlabs.com.*"}`,
			expectedReturn: `[demo_api_http_requests_in_progress demo_api_request_duration_seconds_bucket demo_api_request_duration_seconds_count demo_api_request_duration_seconds_sum demo_batch_last_run_duration_seconds demo_batch_last_run_processed_bytes demo_batch_last_run_timestamp_seconds demo_batch_last_success_timestamp_seconds demo_cpu_usage_seconds_total demo_disk_total_bytes demo_disk_usage_bytes demo_num_cpus go_gc_duration_seconds go_gc_duration_seconds_count go_gc_duration_seconds_sum go_goroutines go_memstats_alloc_bytes go_memstats_alloc_bytes_total go_memstats_buck_hash_sys_bytes go_memstats_frees_total go_memstats_gc_cpu_fraction go_memstats_gc_sys_bytes go_memstats_heap_alloc_bytes go_memstats_heap_idle_bytes go_memstats_heap_inuse_bytes go_memstats_heap_objects go_memstats_heap_released_bytes go_memstats_heap_sys_bytes go_memstats_last_gc_time_seconds go_memstats_lookups_total go_memstats_mallocs_total go_memstats_mcache_inuse_bytes go_memstats_mcache_sys_bytes go_memstats_mspan_inuse_bytes go_memstats_mspan_sys_bytes go_memstats_next_gc_bytes go_memstats_other_sys_bytes go_memstats_stack_inuse_bytes go_memstats_stack_sys_bytes go_memstats_sys_bytes go_threads http_request_duration_microseconds http_request_duration_microseconds_count http_request_duration_microseconds_sum http_request_size_bytes http_request_size_bytes_count http_request_size_bytes_sum http_requests_total http_response_size_bytes http_response_size_bytes_count http_response_size_bytes_sum process_cpu_seconds_total process_max_fds process_open_fds process_resident_memory_bytes process_start_time_seconds process_virtual_memory_bytes scrape_duration_seconds scrape_samples_post_metric_relabeling scrape_samples_scraped scrape_series_added up] 1025`,
		},
		{
			name:           "less_than_equal",
			matchers:       `{le="0.0001"}`,
			expectedReturn: `[demo_api_request_duration_seconds_bucket] 29`,
		},
		{
			name:           "less_than_equal_regex",
			matchers:       `{le=~"0.*"}`,
			expectedReturn: `[demo_api_request_duration_seconds_bucket] 667`,
		},
		{
			name:           "http_method",
			matchers:       `{method="POST"}`,
			expectedReturn: `[demo_api_request_duration_seconds_bucket demo_api_request_duration_seconds_count demo_api_request_duration_seconds_sum] 336`,
		},
		{
			name:           "http_methods_regex",
			matchers:       `{method=~".*"}`,
			expectedReturn: `[demo_api_http_requests_in_progress demo_api_request_duration_seconds_bucket demo_api_request_duration_seconds_count demo_api_request_duration_seconds_sum demo_batch_last_run_duration_seconds demo_batch_last_run_processed_bytes demo_batch_last_run_timestamp_seconds demo_batch_last_success_timestamp_seconds demo_cpu_usage_seconds_total demo_disk_total_bytes demo_disk_usage_bytes demo_num_cpus go_gc_duration_seconds go_gc_duration_seconds_count go_gc_duration_seconds_sum go_goroutines go_memstats_alloc_bytes go_memstats_alloc_bytes_total go_memstats_buck_hash_sys_bytes go_memstats_frees_total go_memstats_gc_cpu_fraction go_memstats_gc_sys_bytes go_memstats_heap_alloc_bytes go_memstats_heap_idle_bytes go_memstats_heap_inuse_bytes go_memstats_heap_objects go_memstats_heap_released_bytes go_memstats_heap_sys_bytes go_memstats_last_gc_time_seconds go_memstats_lookups_total go_memstats_mallocs_total go_memstats_mcache_inuse_bytes go_memstats_mcache_sys_bytes go_memstats_mspan_inuse_bytes go_memstats_mspan_sys_bytes go_memstats_next_gc_bytes go_memstats_other_sys_bytes go_memstats_stack_inuse_bytes go_memstats_stack_sys_bytes go_memstats_sys_bytes go_threads http_request_duration_microseconds http_request_duration_microseconds_count http_request_duration_microseconds_sum http_request_size_bytes http_request_size_bytes_count http_request_size_bytes_sum http_requests_total http_response_size_bytes http_response_size_bytes_count http_response_size_bytes_sum process_cpu_seconds_total process_max_fds process_open_fds process_resident_memory_bytes process_start_time_seconds process_virtual_memory_bytes scrape_duration_seconds scrape_samples_post_metric_relabeling scrape_samples_scraped scrape_series_added up] 1025`,
		},
		{
			name:           "quantile",
			matchers:       `{quantile="0.99"}`,
			expectedReturn: `[http_request_duration_microseconds http_request_size_bytes http_response_size_bytes] 9`,
		},
		{
			name:           "quantiles_multiple",
			matchers:       `{quantile=~"0.99|0.25|0.5"}`,
			expectedReturn: `[go_gc_duration_seconds http_request_duration_microseconds http_request_size_bytes http_response_size_bytes] 24`,
		},
		{
			name:           "quantiles_regex",
			matchers:       `{quantile=~".*"}`,
			expectedReturn: `[demo_api_http_requests_in_progress demo_api_request_duration_seconds_bucket demo_api_request_duration_seconds_count demo_api_request_duration_seconds_sum demo_batch_last_run_duration_seconds demo_batch_last_run_processed_bytes demo_batch_last_run_timestamp_seconds demo_batch_last_success_timestamp_seconds demo_cpu_usage_seconds_total demo_disk_total_bytes demo_disk_usage_bytes demo_num_cpus go_gc_duration_seconds go_gc_duration_seconds_count go_gc_duration_seconds_sum go_goroutines go_memstats_alloc_bytes go_memstats_alloc_bytes_total go_memstats_buck_hash_sys_bytes go_memstats_frees_total go_memstats_gc_cpu_fraction go_memstats_gc_sys_bytes go_memstats_heap_alloc_bytes go_memstats_heap_idle_bytes go_memstats_heap_inuse_bytes go_memstats_heap_objects go_memstats_heap_released_bytes go_memstats_heap_sys_bytes go_memstats_last_gc_time_seconds go_memstats_lookups_total go_memstats_mallocs_total go_memstats_mcache_inuse_bytes go_memstats_mcache_sys_bytes go_memstats_mspan_inuse_bytes go_memstats_mspan_sys_bytes go_memstats_next_gc_bytes go_memstats_other_sys_bytes go_memstats_stack_inuse_bytes go_memstats_stack_sys_bytes go_memstats_sys_bytes go_threads http_request_duration_microseconds http_request_duration_microseconds_count http_request_duration_microseconds_sum http_request_size_bytes http_request_size_bytes_count http_request_size_bytes_sum http_requests_total http_response_size_bytes http_response_size_bytes_count http_response_size_bytes_sum process_cpu_seconds_total process_max_fds process_open_fds process_resident_memory_bytes process_start_time_seconds process_virtual_memory_bytes scrape_duration_seconds scrape_samples_post_metric_relabeling scrape_samples_scraped scrape_series_added up] 1025`,
		},
		// Negative regex matchers.
		{
			name:           "demo_instance_empty",
			matchers:       `{job!="demo", instance="demo.promlabs.com:10000"}`,
			expectedReturn: `[] 0`,
		},
		{
			name:           "demo_instance_regex_empty",
			matchers:       `{job!="demo", instance=~"demo.promlabs.com.*"}`,
			expectedReturn: `[] 0`,
		},
		{
			name:           "http_methods_regex_empty",
			matchers:       `{method!~".*"}`,
			expectedReturn: `[] 0`,
		},
		{
			name:           "quantiles_regex_empty",
			matchers:       `{quantile!~".*"}`,
			expectedReturn: `[] 0`,
		},
		{
			name:           "less_than_equal_regex",
			matchers:       `{le!~"0.*"}`,
			expectedReturn: `[demo_api_http_requests_in_progress demo_api_request_duration_seconds_bucket demo_api_request_duration_seconds_count demo_api_request_duration_seconds_sum demo_batch_last_run_duration_seconds demo_batch_last_run_processed_bytes demo_batch_last_run_timestamp_seconds demo_batch_last_success_timestamp_seconds demo_cpu_usage_seconds_total demo_disk_total_bytes demo_disk_usage_bytes demo_num_cpus go_gc_duration_seconds go_gc_duration_seconds_count go_gc_duration_seconds_sum go_goroutines go_memstats_alloc_bytes go_memstats_alloc_bytes_total go_memstats_buck_hash_sys_bytes go_memstats_frees_total go_memstats_gc_cpu_fraction go_memstats_gc_sys_bytes go_memstats_heap_alloc_bytes go_memstats_heap_idle_bytes go_memstats_heap_inuse_bytes go_memstats_heap_objects go_memstats_heap_released_bytes go_memstats_heap_sys_bytes go_memstats_last_gc_time_seconds go_memstats_lookups_total go_memstats_mallocs_total go_memstats_mcache_inuse_bytes go_memstats_mcache_sys_bytes go_memstats_mspan_inuse_bytes go_memstats_mspan_sys_bytes go_memstats_next_gc_bytes go_memstats_other_sys_bytes go_memstats_stack_inuse_bytes go_memstats_stack_sys_bytes go_memstats_sys_bytes go_threads http_request_duration_microseconds http_request_duration_microseconds_count http_request_duration_microseconds_sum http_request_size_bytes http_request_size_bytes_count http_request_size_bytes_sum http_requests_total http_response_size_bytes http_response_size_bytes_count http_response_size_bytes_sum process_cpu_seconds_total process_max_fds process_open_fds process_resident_memory_bytes process_start_time_seconds process_virtual_memory_bytes scrape_duration_seconds scrape_samples_post_metric_relabeling scrape_samples_scraped scrape_series_added up] 358`,
		},
		{
			name:           "http_method",
			matchers:       `{method!="POST"}`,
			expectedReturn: `[demo_api_http_requests_in_progress demo_api_request_duration_seconds_bucket demo_api_request_duration_seconds_count demo_api_request_duration_seconds_sum demo_batch_last_run_duration_seconds demo_batch_last_run_processed_bytes demo_batch_last_run_timestamp_seconds demo_batch_last_success_timestamp_seconds demo_cpu_usage_seconds_total demo_disk_total_bytes demo_disk_usage_bytes demo_num_cpus go_gc_duration_seconds go_gc_duration_seconds_count go_gc_duration_seconds_sum go_goroutines go_memstats_alloc_bytes go_memstats_alloc_bytes_total go_memstats_buck_hash_sys_bytes go_memstats_frees_total go_memstats_gc_cpu_fraction go_memstats_gc_sys_bytes go_memstats_heap_alloc_bytes go_memstats_heap_idle_bytes go_memstats_heap_inuse_bytes go_memstats_heap_objects go_memstats_heap_released_bytes go_memstats_heap_sys_bytes go_memstats_last_gc_time_seconds go_memstats_lookups_total go_memstats_mallocs_total go_memstats_mcache_inuse_bytes go_memstats_mcache_sys_bytes go_memstats_mspan_inuse_bytes go_memstats_mspan_sys_bytes go_memstats_next_gc_bytes go_memstats_other_sys_bytes go_memstats_stack_inuse_bytes go_memstats_stack_sys_bytes go_memstats_sys_bytes go_threads http_request_duration_microseconds http_request_duration_microseconds_count http_request_duration_microseconds_sum http_request_size_bytes http_request_size_bytes_count http_request_size_bytes_sum http_requests_total http_response_size_bytes http_response_size_bytes_count http_response_size_bytes_sum process_cpu_seconds_total process_max_fds process_open_fds process_resident_memory_bytes process_start_time_seconds process_virtual_memory_bytes scrape_duration_seconds scrape_samples_post_metric_relabeling scrape_samples_scraped scrape_series_added up] 689`,
		},
		{
			name:           "quantile",
			matchers:       `{quantile!="0.99"}`,
			expectedReturn: `[demo_api_http_requests_in_progress demo_api_request_duration_seconds_bucket demo_api_request_duration_seconds_count demo_api_request_duration_seconds_sum demo_batch_last_run_duration_seconds demo_batch_last_run_processed_bytes demo_batch_last_run_timestamp_seconds demo_batch_last_success_timestamp_seconds demo_cpu_usage_seconds_total demo_disk_total_bytes demo_disk_usage_bytes demo_num_cpus go_gc_duration_seconds go_gc_duration_seconds_count go_gc_duration_seconds_sum go_goroutines go_memstats_alloc_bytes go_memstats_alloc_bytes_total go_memstats_buck_hash_sys_bytes go_memstats_frees_total go_memstats_gc_cpu_fraction go_memstats_gc_sys_bytes go_memstats_heap_alloc_bytes go_memstats_heap_idle_bytes go_memstats_heap_inuse_bytes go_memstats_heap_objects go_memstats_heap_released_bytes go_memstats_heap_sys_bytes go_memstats_last_gc_time_seconds go_memstats_lookups_total go_memstats_mallocs_total go_memstats_mcache_inuse_bytes go_memstats_mcache_sys_bytes go_memstats_mspan_inuse_bytes go_memstats_mspan_sys_bytes go_memstats_next_gc_bytes go_memstats_other_sys_bytes go_memstats_stack_inuse_bytes go_memstats_stack_sys_bytes go_memstats_sys_bytes go_threads http_request_duration_microseconds http_request_duration_microseconds_count http_request_duration_microseconds_sum http_request_size_bytes http_request_size_bytes_count http_request_size_bytes_sum http_requests_total http_response_size_bytes http_response_size_bytes_count http_response_size_bytes_sum process_cpu_seconds_total process_max_fds process_open_fds process_resident_memory_bytes process_start_time_seconds process_virtual_memory_bytes scrape_duration_seconds scrape_samples_post_metric_relabeling scrape_samples_scraped scrape_series_added up] 1016`,
		},
		{
			name:           "quantiles_multiple",
			matchers:       `{quantile!~"0.99|0.25|0.5"}`,
			expectedReturn: `[demo_api_http_requests_in_progress demo_api_request_duration_seconds_bucket demo_api_request_duration_seconds_count demo_api_request_duration_seconds_sum demo_batch_last_run_duration_seconds demo_batch_last_run_processed_bytes demo_batch_last_run_timestamp_seconds demo_batch_last_success_timestamp_seconds demo_cpu_usage_seconds_total demo_disk_total_bytes demo_disk_usage_bytes demo_num_cpus go_gc_duration_seconds go_gc_duration_seconds_count go_gc_duration_seconds_sum go_goroutines go_memstats_alloc_bytes go_memstats_alloc_bytes_total go_memstats_buck_hash_sys_bytes go_memstats_frees_total go_memstats_gc_cpu_fraction go_memstats_gc_sys_bytes go_memstats_heap_alloc_bytes go_memstats_heap_idle_bytes go_memstats_heap_inuse_bytes go_memstats_heap_objects go_memstats_heap_released_bytes go_memstats_heap_sys_bytes go_memstats_last_gc_time_seconds go_memstats_lookups_total go_memstats_mallocs_total go_memstats_mcache_inuse_bytes go_memstats_mcache_sys_bytes go_memstats_mspan_inuse_bytes go_memstats_mspan_sys_bytes go_memstats_next_gc_bytes go_memstats_other_sys_bytes go_memstats_stack_inuse_bytes go_memstats_stack_sys_bytes go_memstats_sys_bytes go_threads http_request_duration_microseconds http_request_duration_microseconds_count http_request_duration_microseconds_sum http_request_size_bytes http_request_size_bytes_count http_request_size_bytes_sum http_requests_total http_response_size_bytes http_response_size_bytes_count http_response_size_bytes_sum process_cpu_seconds_total process_max_fds process_open_fds process_resident_memory_bytes process_start_time_seconds process_virtual_memory_bytes scrape_duration_seconds scrape_samples_post_metric_relabeling scrape_samples_scraped scrape_series_added up] 1001`,
		},
	}

	matchersSmall := []deleteStr{
		{
			name:           "normal regex",
			matchers:       `{foo=~".*"}`,
			expectedReturn: `[METRIC_4 metric_1 metric_2 metric_3] 9`,
		},
		{
			name:           "two regex",
			matchers:       `{instance!~"1.*", foo=~"ba.+"}`,
			expectedReturn: `[METRIC_4 metric_1 metric_2] 5`,
		},
	}

	withDB(t, *testDatabase, func(dbOwner *pgxpool.Pool, t testing.TB) {
		db := testhelpers.PgxPoolWithRole(t, *testDatabase, "prom_modifier")
		defer db.Close()

		ts := generateLargeTimeseries()
		if *extendedTest {
			ts = generateRealTimeseries()
		}
		ingestor, err := ingstr.NewPgxIngestorForTests(pgxconn.NewPgxConn(db), nil)
		if err != nil {
			t.Fatal(err)
		}
		defer ingestor.Close()
		if _, _, err := ingestor.Ingest(context.Background(), newWriteRequestWithTs(copyMetrics(ts))); err != nil {
			t.Fatal(err)
		}
		err = ingestor.CompleteMetricCreation(context.Background())
		if err != nil {
			t.Fatal(err)
		}

		pgDelete := &pgDel.PgDelete{Conn: pgxconn.NewPgxConn(db)}
		if !*extendedTest {
			matchers = matchersSmall
		}
		for _, m := range matchers {
			matcher, err := getMatchers(m.matchers)
			require.NoError(t, err)
			parsedStartTime, err := parseTime(m.start, model.MinTime)
			require.NoError(t, err)
			parsedEndTime, err := parseTime(m.end, model.MaxTime)
			require.NoError(t, err)
			touchedMetrics, deletedSeriesIDs, _, err := pgDelete.DeleteSeries(matcher, parsedStartTime, parsedEndTime)
			require.NoError(t, err)
			sort.Strings(touchedMetrics)
			require.Equal(t, m.expectedReturn, fmt.Sprintf("%v %v", touchedMetrics, len(deletedSeriesIDs)), "expected returns does not match in", m.name)
		}
	})
}

var (
	minTime          = time.Unix(math.MinInt64/1000+62135596801, 0).UTC()
	maxTime          = time.Unix(math.MaxInt64/1000-62135596801, 999999999).UTC()
	minTimeFormatted = minTime.Format(time.RFC3339Nano)
	maxTimeFormatted = maxTime.Format(time.RFC3339Nano)
)

func parseTime(s string, d time.Time) (time.Time, error) {
	if s == "" {
		return d, nil
	}
	if t, err := strconv.ParseFloat(s, 64); err == nil {
		s, ns := math.Modf(t)
		ns = math.Round(ns*1000) / 1000
		return time.Unix(int64(s), int64(ns*float64(time.Second))).UTC(), nil
	}
	if t, err := time.Parse(time.RFC3339Nano, s); err == nil {
		return t, nil
	}

	// Stdlib's time parser can only handle 4 digit years. As a workaround until
	// that is fixed we want to at least support our own boundary times.
	// Context: https://github.com/prometheus/client_golang/issues/614
	// Upstream issue: https://github.com/golang/go/issues/20555
	switch s {
	case minTimeFormatted:
		return minTime, nil
	case maxTimeFormatted:
		return maxTime, nil
	}
	return time.Time{}, fmt.Errorf("cannot parse %q to a valid timestamp", s)
}

func getMatchers(s string) ([]*labels.Matcher, error) {
	matchers, err := parser.ParseMetricSelector(s)
	if err != nil {
		return nil, err
	}
	return matchers, nil
}
