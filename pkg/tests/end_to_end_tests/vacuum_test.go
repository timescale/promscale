package end_to_end_tests

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
	"github.com/timescale/promscale/pkg/pgxconn"
	"github.com/timescale/promscale/pkg/vacuum"
)

const (
	sqlChunksToFreeze = `
	SELECT %s 
	FROM _ps_catalog.compressed_chunks_to_freeze
	WHERE coalesce(last_vacuum, '-infinity'::timestamptz) < now() - interval '15 minutes' 
	AND pg_catalog.age(relfrozenxid) > current_setting('vacuum_freeze_min_age')::bigint
	`
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
	testVacuum(t, func(e *vacuum.Engine, db *pgxpool.Pool) {
		e.Run(context.Background())
	})
}

func TestVacuumNonBlocking(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	testVacuum(t, func(e *vacuum.Engine, db *pgxpool.Pool) {
		go e.Start()
		// wait until view returns 0 or a timeout elapses, whichever comes first
		wait(t, db)
		t.Log("Stopping the vacuum engine...")
		e.Stop()
	})
}

func wait(t *testing.T, db *pgxpool.Pool) {
	const first = 10
	time.Sleep(first * time.Second)
	for i := 0; i < 60; i++ {
		nbr := countChunksToFreeze(t, context.Background(), db)
		if nbr == 0 {
			t.Logf("Zero chunks to freeze after %d seconds.", first+i)
			return
		}
		time.Sleep(time.Second)
	}
}

func testVacuum(t *testing.T, do func(e *vacuum.Engine, db *pgxpool.Pool)) {
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
		setVacuumFreezeMinAge(t, ctx, db)
		createMetric(t, ctx, db, "metric1")
		createMetric(t, ctx, db, "metric2")
		loadSamples(t, ctx, db, "metric1")
		loadSamples(t, ctx, db, "metric2")
		chunksExist(t, ctx, db)
		nbr := countChunksToFreeze(t, ctx, db)
		require.Equal(t, int64(0), nbr, "Expected view to return zero before compressing chunks results but got %d", nbr)
		const count = 5
		uncompressed := chooseUncompressedChunks(t, ctx, db, count)
		compressChunks(t, ctx, db, uncompressed)
		compressed := view(t, ctx, db)
		require.Equal(t, count, len(compressed), "Expected view to return %d compressed chunks before running engine but got %d", count, len(compressed))
		engine, err := vacuum.NewEngine(pgxconn.NewPgxConn(db), time.Second, count)
		require.NoError(t, err, "Failed to create vacuum engine")
		do(engine, db)
		nbr = countChunksToFreeze(t, ctx, db)
		require.Equal(t, int64(0), nbr, "Expected view to return zero after running engine results but got %d", nbr)
	})
}

func setVacuumFreezeMinAge(t *testing.T, ctx context.Context, db *pgxpool.Pool) {
	_, err := db.Exec(ctx, "set vacuum_freeze_min_age to 0")
	if err != nil {
		t.Fatalf("Failed to set vacuum_freeze_min_age to 0: %v", err)
	}
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

func countChunksToFreeze(t *testing.T, ctx context.Context, db *pgxpool.Pool) int64 {
	var nbr int64
	err := db.QueryRow(ctx, fmt.Sprintf(sqlChunksToFreeze, "count(*)")).Scan(&nbr)
	if err != nil {
		t.Fatalf("Failed to count chunks: %v", err)
	}
	return nbr
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
	rows, err := db.Query(ctx, fmt.Sprintf(sqlChunksToFreeze, "id"))
	if err != nil {
		t.Fatalf("Failed to select from _ps_catalog.compressed_chunks_to_freeze: %v", err)
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
