// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package end_to_end_tests

import (
	"context"
	"fmt"
	"sort"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/timescale/promscale/pkg/pgmodel/model"
)

const (
	labelCount      = 5
	metricName      = "benchmark_metric"
	otherMetricName = "other_benchmark_metric"
)

func BenchmarkGetSeriesIDForKeyValueArrayExistingSeries(b *testing.B) {
	b.StopTimer()
	withDB(b, "bench_1", func(db *pgxpool.Pool, t testing.TB) {
		_, _, err := createMetricTableName(db, metricName)
		if err != nil {
			t.Fatal(err)
		}

		keys, values := generateKeysAndValues(b.N+labelCount, "label")
		for n := 0; n < b.N; n++ {
			err = getSeriesIDForKeyValueArray(db, metricName, keys[n:n+labelCount], values[n:n+labelCount])
			if err != nil {
				t.Fatal(err)
			}
		}

		var bench *testing.B
		var ok bool
		if bench, ok = t.(*testing.B); !ok {
			t.Fatal("Not a benchmarking instance, stopping benchmark")
		}

		bench.ResetTimer()
		bench.StartTimer()
		for n := 0; n < b.N; n++ {
			err = getSeriesIDForKeyValueArray(db, metricName, keys[n:n+labelCount], values[n:n+labelCount])
			if err != nil {
				t.Fatal(err)
			}
		}
		bench.StopTimer()
	})
}

func BenchmarkGetSeriesIDForKeyValueArrayNewSeriesExistingLabels(b *testing.B) {
	b.StopTimer()
	withDB(b, "bench_2", func(db *pgxpool.Pool, t testing.TB) {
		_, _, err := createMetricTableName(db, metricName)
		if err != nil {
			t.Fatal(err)
		}

		keys, values := generateKeysAndValues(b.N+labelCount, "label")
		for n := 0; n < b.N; n++ {
			err = getSeriesIDForKeyValueArray(db, otherMetricName, keys[n:n+labelCount], values[n:n+labelCount])
			if err != nil {
				t.Fatal(err)
			}
		}

		var bench *testing.B
		var ok bool
		if bench, ok = t.(*testing.B); !ok {
			t.Fatal("Not a benchmarking instance, stopping benchmark")
		}

		bench.ResetTimer()
		bench.StartTimer()
		for n := 0; n < b.N; n++ {
			err = getSeriesIDForKeyValueArray(db, metricName, keys[n:n+labelCount], values[n:n+labelCount])
			if err != nil {
				t.Fatal(err)
			}
		}
		bench.StopTimer()
	})
}

func BenchmarkGetSeriesIDForKeyValueArrayNewMetric(b *testing.B) {
	b.StopTimer()
	withDB(b, "bench_2", func(db *pgxpool.Pool, t testing.TB) {
		_, _, err := createMetricTableName(db, metricName)
		if err != nil {
			t.Fatal(err)
		}

		keys, values := generateKeysAndValues(labelCount, "label")
		for n := 0; n < b.N; n++ {
			err = getSeriesIDForKeyValueArray(db, otherMetricName, keys, values)
			if err != nil {
				t.Fatal(err)
			}
		}

		var bench *testing.B
		var ok bool
		if bench, ok = t.(*testing.B); !ok {
			t.Fatal("Not a benchmarking instance, stopping benchmark")
		}

		//preload many metrics so that we are testing performance when many metrics exists
		for n := 0; n < 1000; n++ {
			err = getSeriesIDForKeyValueArray(db, fmt.Sprintf("%s_warmup_%d", metricName, n), keys, values)
			if err != nil {
				t.Fatal(err)
			}
		}

		bench.ResetTimer()
		bench.StartTimer()
		for n := 0; n < b.N; n++ {
			err = getSeriesIDForKeyValueArray(db, fmt.Sprintf("%s_%d", metricName, n), keys, values)
			if err != nil {
				t.Fatal(err)
			}
		}
		bench.StopTimer()
	})
}

func BenchmarkGetSeriesIDForKeyValueArrayNewSeriesNewLabels(b *testing.B) {
	b.StopTimer()
	withDB(b, "bench_3", func(db *pgxpool.Pool, t testing.TB) {
		_, _, err := createMetricTableName(db, metricName)
		if err != nil {
			t.Fatal(err)
		}

		keys, values := generateKeysAndValues(b.N+labelCount, "label")

		var bench *testing.B
		var ok bool
		if bench, ok = t.(*testing.B); !ok {
			t.Fatal("Not a benchmarking instance, stopping benchmark")
		}

		bench.ResetTimer()
		bench.StartTimer()
		for n := 0; n < b.N; n++ {
			err = getSeriesIDForKeyValueArray(db, metricName, keys[n:n+labelCount], values[n:n+labelCount])
			if err != nil {
				t.Fatal(err)
			}
		}
		bench.StopTimer()
	})
}

func BenchmarkGetSeriesIDForKeyValueArrayNewSeriesNewLabelsBatch(b *testing.B) {
	b.StopTimer()
	withDB(b, "bench_3", func(db *pgxpool.Pool, t testing.TB) {
		metricID, tableName, err := createMetricTableName(db, metricName)
		if err != nil {
			t.Fatal(err)
		}

		keys, values := generateKeysAndValuesNoOverlap(b.N, "label")

		var bench *testing.B
		var ok bool
		if bench, ok = t.(*testing.B); !ok {
			t.Fatal("Not a benchmarking instance, stopping benchmark")
		}

		bench.ResetTimer()
		bench.StartTimer()
		err = getSeriesIDForKeyValueArrayBatchUsingLabelArrays(db, metricID, metricName, tableName, b.N, keys, values)
		if err != nil {
			t.Fatal(err)
		}
		bench.StopTimer()
	})
}

func BenchmarkKeyValueArrayToLabelArrayCreateNewLabels(b *testing.B) {
	b.StopTimer()
	withDB(b, "bench_5", func(db *pgxpool.Pool, t testing.TB) {
		_, _, err := createMetricTableName(db, metricName)
		if err != nil {
			t.Fatal(err)
		}

		keys, values := generateKeysAndValues(b.N+labelCount, "label")

		var bench *testing.B
		var ok bool
		if bench, ok = t.(*testing.B); !ok {
			t.Fatal("Not a benchmarking instance, stopping benchmark")
		}

		bench.ResetTimer()
		bench.StartTimer()
		for n := 0; n < b.N; n++ {
			err = keyValueArrayToLabelArray(db, metricName, keys[n:n+labelCount], values[n:n+labelCount])
			if err != nil {
				t.Fatal(err)
			}
		}
		bench.StopTimer()
	})
}

func BenchmarkKeyValueArrayToLabelArrayExistingLabels(b *testing.B) {
	b.StopTimer()
	withDB(b, "bench_6", func(db *pgxpool.Pool, t testing.TB) {
		_, _, err := createMetricTableName(db, metricName)
		if err != nil {
			t.Fatal(err)
		}

		keys, values := generateKeysAndValues(b.N+labelCount, "label")
		for n := 0; n < b.N; n++ {
			err = getSeriesIDForKeyValueArray(db, metricName, keys[n:n+labelCount], values[n:n+labelCount])
			if err != nil {
				t.Fatal(err)
			}
		}

		var bench *testing.B
		var ok bool
		if bench, ok = t.(*testing.B); !ok {
			t.Fatal("Not a benchmarking instance, stopping benchmark")
		}

		bench.ResetTimer()
		bench.StartTimer()
		for n := 0; n < b.N; n++ {
			err = keyValueArrayToLabelArray(db, metricName, keys[n:n+labelCount], values[n:n+labelCount])
			if err != nil {
				t.Fatal(err)
			}
		}
		bench.StopTimer()
	})
}

func BenchmarkKeyValueArrayToLabelArrayCreateNewLabelKeys(b *testing.B) {
	b.StopTimer()
	withDB(b, "bench_7", func(db *pgxpool.Pool, t testing.TB) {

		_, _, err := createMetricTableName(db, metricName)
		if err != nil {
			t.Fatal(err)
		}

		_, _, err = createMetricTableName(db, otherMetricName)
		if err != nil {
			t.Fatal(err)
		}

		keys, values := generateKeysAndValues(b.N+labelCount, "label")
		for n := 0; n < b.N; n++ {
			err = getSeriesIDForKeyValueArray(db, otherMetricName, keys[n:n+labelCount], values[n:n+labelCount])
			if err != nil {
				t.Fatal(err)
			}
		}

		keys, values = generateKeysAndValues(b.N+labelCount, "label_new")

		var bench *testing.B
		var ok bool
		if bench, ok = t.(*testing.B); !ok {
			t.Fatal("Not a benchmarking instance, stopping benchmark")
		}

		bench.ResetTimer()
		bench.StartTimer()
		for n := 0; n < b.N; n++ {
			err = keyValueArrayToLabelArray(db, metricName, keys[n:n+labelCount], values[n:n+labelCount])
			if err != nil {
				t.Fatal(err)
			}
		}
		bench.StopTimer()
	})
}

func BenchmarkGetOrCreateMetricTableName(b *testing.B) {
	b.StopTimer()
	withDB(b, "bench_8", func(db *pgxpool.Pool, t testing.TB) {
		metricNames, _ := generateKeysAndValues(b.N, "metric")

		var bench *testing.B
		var ok bool
		if bench, ok = t.(*testing.B); !ok {
			t.Fatal("Not a benchmarking instance, stopping benchmark")
		}

		bench.ResetTimer()
		bench.StartTimer()
		var err error
		for n := 0; n < b.N; n++ {
			_, _, err = createMetricTableName(db, metricNames[n])
			if err != nil {
				t.Fatal(err)
			}
		}
		bench.StopTimer()
	})
}

func keyValueArrayToLabelArray(db *pgxpool.Pool, metricName string, keys []string, values []string) error {
	var labelArray []int
	return db.QueryRow(context.Background(), "SELECT _prom_catalog.get_or_create_label_array($1, $2, $3)", metricName, keys, values).Scan(&labelArray)
}

func createMetricTableName(db *pgxpool.Pool, name string) (metricID int64, tableName string, err error) {
	err = db.QueryRow(context.Background(), "SELECT id, table_name FROM _prom_catalog.get_or_create_metric_table_name($1)", name).Scan(&metricID, &tableName)
	return metricID, tableName, err
}

func getSeriesIDForKeyValueArray(db *pgxpool.Pool, metricName string, keys []string, values []string) error {
	var tableName string
	var seriesIDKeyVal int
	return db.QueryRow(context.Background(), "SELECT * FROM _prom_catalog.get_or_create_series_id_for_kv_array($1, $2, $3)", metricName, append([]string{"__name__"}, keys...), append([]string{metricName}, values...)).Scan(&tableName, &seriesIDKeyVal)
}

type labels_r struct {
	key_index int32
	labels_id int64
}

type labels_u struct {
	key   string
	value string
}

func makeUniqueLabels2(allKeys [][]string, allValues [][]string) ([]string, []string, map[labels_u]*labels_r) {
	unique := make(map[labels_u]*labels_r, len(allKeys))
	seriesLen := len(allKeys)
	for series := 0; series < seriesLen; series++ {
		labelLen := len(allKeys[series])
		for label := 0; label < labelLen; label++ {
			unique[labels_u{allKeys[series][label], allValues[series][label]}] = &labels_r{}
		}
	}
	elems := len(unique)
	lbls := make([]labels_u, elems)
	index := 0
	for ls := range unique {
		lbls[index] = ls
		index++
	}
	sort.Slice(lbls, func(i, j int) bool {
		return lbls[i].key < lbls[j].key || (lbls[i].key == lbls[j].key && lbls[i].value < lbls[j].value)
	})

	labelKeys, labelValues := make([]string, elems), make([]string, elems)
	index = 0
	for ls := range lbls {
		labelKeys[index] = lbls[ls].key
		labelValues[index] = lbls[ls].value
		index++
	}
	return labelKeys, labelValues, unique
}

func getSeriesIDForKeyValueArrayBatchUsingLabelArrays(db *pgxpool.Pool, metricID int64, metricName, tableName string, N int, allKeys [][]string, allValues [][]string) error {
	var seriesIDKeyVal int
	if N != len(allKeys) {
		panic(fmt.Sprintf("unexpected %v %v", N, len(allKeys)))
	}

	var labelArrayOID uint32
	err := db.QueryRow(context.Background(), `select 'prom_api.label_array'::regtype::oid`).Scan(&labelArrayOID)
	if err != nil {
		panic(err)
	}

	lkeys, lvals, unique := makeUniqueLabels2(allKeys, allValues)
	start := time.Now()
	rows, err := db.Query(context.Background(), `
		SELECT
		coalesce(lkp.pos,
		  _prom_catalog.get_or_create_label_key_pos($1, kv.key)),
		coalesce(l.id,
		  _prom_catalog.get_or_create_label_id(kv.key, kv.value)),
		 kv.key,
		 kv.value
	    FROM unnest($2::text[], $3::text[]) AS kv(key, value)
		LEFT JOIN _prom_catalog.label l
		   ON (l.key = kv.key AND l.value = kv.value)
		LEFT JOIN _prom_catalog.label_key_position lkp
		   ON
		   (
			  lkp.metric_name = $1 AND
			  lkp.key = kv.key
		   )
		`, metricName, lkeys, lvals)
	if err != nil {
		return err
	}
	defer rows.Close()
	maxIdx := 0
	for rows.Next() {
		lr := labels_r{}
		lu := labels_u{}
		err := rows.Scan(&lr.key_index, &lr.labels_id, &lu.key, &lu.value)
		if err != nil {
			return err
		}
		_, ok := unique[lu]
		if !ok {
			fmt.Println(lu)
			panic("here")
		}
		*unique[lu] = lr
		if int(lr.key_index) > maxIdx {
			maxIdx = int(lr.key_index)
		}
	}
	took := time.Since(start)
	nLabels := len(unique)
	fmt.Println("Labels DB time", took, "per item:", took/time.Duration(nLabels), "N-labels", nLabels)

	labelArraySet := make([][]int32, 0, N)
	for n := 0; n < N; n++ {
		keys := allKeys[n]
		vals := allValues[n]
		lArray := make([]int32, maxIdx)
		maxKey := 0
		for i := range keys {
			uKey := labels_u{keys[i], vals[i]}
			uRes, ok := unique[uKey]
			if !ok {
				panic("here")
			}
			if uRes.labels_id == 0 {
				fmt.Println(uKey, uRes)
				panic("here2")
			}
			sliceKey := int(uRes.key_index) - 1
			lArray[sliceKey] = int32(uRes.labels_id)
			if sliceKey > maxKey {
				maxKey = sliceKey
			}
		}
		lArray = lArray[:maxKey+1]

		labelArraySet = append(labelArraySet, lArray)
	}

	count := 0

	if true {
		labelArrayArray := model.SliceToArrayOfLabelArray(labelArraySet)
		start = time.Now()
		res, err := db.Query(
			context.Background(),
			"SELECT _prom_catalog.get_or_create_series_id_for_label_array($1, $2, l.a), l.nr FROM unnest($3::prom_api.label_array[]) WITH ORDINALITY l(a, nr) ORDER BY a",
			metricID,
			tableName,
			labelArrayArray,
		)
		if err != nil {
			panic(err)
		}
		defer res.Close()

		for res.Next() {
			var index int64
			err := res.Scan(&seriesIDKeyVal, &index)
			if err != nil {
				panic(err)
			}
			count++
		}
	} else {
		batch := &pgx.Batch{}
		for _, labelArray := range labelArraySet {
			batch.Queue("SELECT * FROM _prom_catalog.get_or_create_series_id_for_label_array($1, $2)", metricName, labelArray)
		}
		start = time.Now()
		br := db.SendBatch(context.Background(), batch)
		defer br.Close()
		for range labelArraySet {
			err := br.QueryRow().Scan(&tableName, &seriesIDKeyVal)
			if err != nil {
				panic(err)
			}
			count++
		}

	}
	took = time.Since(start)
	fmt.Println("DB time", took, "per item:", took/time.Duration(N), "N", N)
	if count != N {
		panic("Count doesn't match")
	}
	return nil
}

/*
*   Insert series using key value array. This method is slower and no longer used.
*   We keep the code around so as to be able to test against it
*



func makeUniqueLabels(allKeys [][]string, allValues [][]string) ([]string, []string) {
	unique := make(map[labels_u]bool, len(allKeys))
	seriesLen := len(allKeys)
	for series := 0; series < seriesLen; series++ {
		labelLen := len(allKeys[series])
		for label := 0; label < labelLen; label++ {
			unique[labels_u{allKeys[series][label], allValues[series][label]}] = true
		}
	}
	elems := len(unique)
	lbls := make([]*labels_u, elems)
	index := 0
	for ls := range unique {
		lbls[index] = &ls
		index++
	}
	sort.Slice(lbls, func(i, j int) bool {
		return lbls[i].key < lbls[j].key || (lbls[i].key == lbls[j].key && lbls[i].value < lbls[j].value)
	})

	labelKeys, labelValues := make([]string, elems), make([]string, elems)
	index = 0
	for ls := range lbls {
		labelKeys[index] = lbls[ls].key
		labelValues[index] = lbls[ls].value
		index++
	}
	return labelKeys, labelValues
}


func getSeriesIDForKeyValueArrayBatchUsingKeyValueArray(db *pgxpool.Pool, metricName string, N int, allKeys [][]string, allValues [][]string) error {
	var tableName string
	var seriesIDKeyVal int
	txn := true
	preMakeLabels := false
	if N != len(allKeys) {
		panic(fmt.Sprintf("unexpected %v %v", N, len(allKeys)))
	}

	batch := &pgx.Batch{}

	if preMakeLabels {
		lkeys, lvals := makeUniqueLabels(allKeys, allValues)
		fmt.Println("lkeys", len(lkeys))
		if txn {
			batch.Queue("BEGIN;")
		}
		batch.Queue(`
		WITH cte AS (
		SELECT
		coalesce(lkp.pos,
		  _prom_catalog.get_or_create_label_key_pos($1, kv.key)) idx,
		coalesce(l.id,
		  _prom_catalog.get_or_create_label_id(kv.key, kv.value)) val
	    FROM unnest($2::text[], $3::text[]) AS kv(key, value)
		LEFT JOIN _prom_catalog.label l
		   ON (l.key = kv.key AND l.value = kv.value)
		LEFT JOIN _prom_catalog.label_key_position lkp
		   ON
		   (
			  lkp.metric_name = $1 AND
			  lkp.key = kv.key
		   )
		) SELECT count(*) FROM cte
		`, metricName, append([]string{"__name__"}, lkeys...), append([]string{metricName}, lvals...))
		if txn {
			batch.Queue("COMMIT;")
		}
	}

	for n := 0; n < N; n++ {
		if txn {
			batch.Queue("BEGIN;")
		}
		batch.Queue("SELECT * FROM _prom_catalog.get_or_create_series_id_for_kv_array($1, $2, $3)", metricName, append([]string{"__name__"}, allKeys[n]...), append([]string{metricName}, allValues[n]...))
		if txn {
			batch.Queue("COMMIT;")
		}
	}

	br := db.SendBatch(context.Background(), batch)
	defer br.Close()

	if preMakeLabels {
		if txn {
			// BEGIN;
			_, err := br.Exec()
			if err != nil {
				return err
			}
		}

		count := 0
		err := br.QueryRow().Scan(&count)
		if err != nil {
			return err
		}
		fmt.Println("labels", count)

		if txn {
			// COMMIT;
			_, err := br.Exec()
			if err != nil {
				return err
			}
		}

	}

	for n := 0; n < N; n++ {
		if txn {
			// BEGIN;
			_, err := br.Exec()
			if err != nil {
				return err
			}
		}

		err := br.QueryRow().Scan(&tableName, &seriesIDKeyVal)
		if err != nil {
			return err
		}

		if txn {
			// COMMIT;
			_, err = br.Exec()
			if err != nil {
				return err
			}
		}
	}
	return nil
}
*/

func generateKeysAndValues(count int, prefix string) ([]string, []string) {
	keys, values := make([]string, count), make([]string, count)

	for i := 0; i < count; i++ {
		keys[i] = fmt.Sprintf("%s_key_%d", prefix, i%labelCount)
		values[i] = fmt.Sprintf("%s_value_%d", prefix, i)
	}

	return keys, values
}

func generateKeysAndValuesNoOverlap(count int, prefix string) ([][]string, [][]string) {
	keys, values := make([][]string, count), make([][]string, count)

	for series := 0; series < count; series++ {
		keys[series], values[series] = make([]string, labelCount+1), make([]string, labelCount+1)
		keys[series][0] = "__name__"
		values[series][0] = metricName
		for label := 1; label < labelCount+1; label++ {
			keys[series][label] = fmt.Sprintf("%s_key_%d", prefix, label)
			if label == 1 {
				values[series][label] = fmt.Sprintf("%s_value_%d", prefix, series)
			} else {
				values[series][label] = fmt.Sprintf("%s_value_%d", prefix, label)
			}
		}
	}

	return keys, values
}
