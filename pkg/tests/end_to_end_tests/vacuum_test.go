package end_to_end_tests

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/stretchr/testify/require"
	"github.com/timescale/promscale/pkg/pgxconn"
	"github.com/timescale/promscale/pkg/vacuum"
)

type uncompressedChunk struct {
	id     int
	schema string
	table  string
}

type compressedChunk struct {
	id int
}

func TestVacuumBlocking(t *testing.T) {
	if !*useTimescaleDB {
		t.Skip("vacuum engine needs TimescaleDB support")
	}
	testVacuum(t, func(e *vacuum.Engine) {
		e.RunOnce()
	})
}

func TestVacuumNonBlocking(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	if !*useTimescaleDB {
		t.Skip("vacuum engine needs TimescaleDB support")
	}
	testVacuum(t, func(e *vacuum.Engine) {
		go e.Start()
		time.Sleep(20 * time.Second)
		e.Stop()
	})
}

func testVacuum(t *testing.T, do func(e *vacuum.Engine)) {
	/*
		create two hypertables with compression enabled but the policy set that it doesn't compress automatically
		load enough samples to create a bunch of chunks
		choose some chunks, compress them, and see if the show up in the view
		run the vacuum engine
		make sure the chunks disappear from the view
	*/
	var ctx = context.Background()
	databaseName := fmt.Sprintf("%s_vacuum", *testDatabase)
	withDB(t, databaseName, func(db *pgxpool.Pool, tb testing.TB) {
		createMetric(t, ctx, db, "metric1")
		createMetric(t, ctx, db, "metric2")
		loadSamples(t, ctx, db, "metric1")
		loadSamples(t, ctx, db, "metric2")
		chunksExist(t, ctx, db)
		viewEmpty(t, ctx, db)
		const count = 5
		uncompressed := chooseUncompressedChunks(t, ctx, db, count)
		compressChunks(t, ctx, db, uncompressed)
		compressed := view(t, ctx, db)
		require.Equal(t, count, len(compressed), "Expected to find %d compressed chunks but got %d", count, len(compressed))
		engine := vacuum.NewEngine(pgxconn.NewPgxConn(db), time.Second, count)
		do(engine)
		viewEmpty(t, ctx, db)
	})
}

func createMetric(t *testing.T, ctx context.Context, db *pgxpool.Pool, name string) {
	_, err := db.Exec(ctx, fmt.Sprintf("create table %s(id int, t timestamptz not null, val double precision)", name))
	if err != nil {
		t.Fatalf("Failed to create hypertable %s: %v", name, err)
	}
	_, err = db.Exec(ctx, "select create_hypertable($1::regclass, 't', chunk_time_interval=>'5 minutes'::interval)", name)
	if err != nil {
		t.Fatalf("Failed to create hypertable %s: %v", name, err)
	}
	_, err = db.Exec(ctx, fmt.Sprintf("alter table %s set (timescaledb.compress, timescaledb.compress_segmentby = 'id')", name))
	if err != nil {
		t.Fatalf("Failed to create hypertable %s: %v", name, err)
	}
	_, err = db.Exec(ctx, "select add_compression_policy($1, INTERVAL '7 days')", name)
	if err != nil {
		t.Fatalf("Failed to set compression policy on hypertable %s: %v", name, err)
	}
}

func loadSamples(t *testing.T, ctx context.Context, db *pgxpool.Pool, name string) {
	d := time.Now().Truncate(time.Minute)
	for i := 0; i < 25; i++ {
		_, err := db.Exec(ctx, fmt.Sprintf(`
			insert into %s (id, t, val)
			select x, $1, random()
			from generate_series(1, 5) x
			`, name), d)
		if err != nil {
			t.Fatalf("Failed to load samples into hypertable %s: %v", name, err)
		}
		d = d.Add(time.Minute * 5)
	}
}

func chunksExist(t *testing.T, ctx context.Context, db *pgxpool.Pool) {
	var nbr int64
	err := db.QueryRow(ctx, "select count(*) from _timescaledb_catalog.chunk").Scan(&nbr)
	if err != nil {
		t.Fatalf("failed to count chunks: %v", err)
	}
	require.GreaterOrEqual(t, nbr, int64(10), "Expected a bunch of chunks but got %d", nbr)
}

func viewEmpty(t *testing.T, ctx context.Context, db *pgxpool.Pool) {
	var nbr int64
	err := db.QueryRow(ctx, "select count(*) from _ps_catalog.chunks_to_freeze").Scan(&nbr)
	if err != nil {
		t.Fatalf("Failed to count chunks: %v", err)
	}
	require.Equal(t, int64(0), nbr, "Expected view to return zero results but got %d", nbr)
}

func chooseUncompressedChunks(t *testing.T, ctx context.Context, db *pgxpool.Pool, count int) []uncompressedChunk {
	rows, err := db.Query(ctx, `
		select id, schema_name, table_name
		from _timescaledb_catalog.chunk
		where compressed_chunk_id is null
		and table_name not like 'compress_%'
		order by random()
		limit $1
	`, count)
	if err != nil {
		t.Fatalf("Failed to select random uncompressed chunks: %v", err)
	}
	defer rows.Close()
	chunks := make([]uncompressedChunk, 0)
	for rows.Next() {
		var chunk uncompressedChunk
		err := rows.Scan(&chunk.id, &chunk.schema, &chunk.table)
		if err != nil {
			t.Fatalf("Failed to scan uncompressed chunk: %v", err)
		}
		chunks = append(chunks, chunk)
	}
	require.Equal(t, count, len(chunks), "Expected %d uncompressed chunks but got %d", count, len(chunks))
	return chunks
}

func compressChunks(t *testing.T, ctx context.Context, db *pgxpool.Pool, chunks []uncompressedChunk) {
	for _, chunk := range chunks {
		_, err := db.Exec(ctx, "select compress_chunk(format('%I.%I', $1::text, $2::text)::regclass)", chunk.schema, chunk.table)
		if err != nil {
			t.Fatalf("Failed to compress chunk %s.%s: %v", chunk.schema, chunk.table, err)
		}
	}
}

func view(t *testing.T, ctx context.Context, db *pgxpool.Pool) []compressedChunk {
	rows, err := db.Query(ctx, "select id from _ps_catalog.chunks_to_freeze")
	if err != nil {
		t.Fatalf("Failed to select from _ps_catalog.chunks_to_freeze: %v", err)
	}
	defer rows.Close()
	chunks := make([]compressedChunk, 0)
	for rows.Next() {
		var chunk compressedChunk
		err := rows.Scan(&chunk.id)
		if err != nil {
			t.Fatalf("Failed to scan uncompressed chunk: %v", err)
		}
		chunks = append(chunks, chunk)
	}
	return chunks
}
