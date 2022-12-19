// Package vacuum implements a background engine that vacuums
// compressed chunks.
//
// The goal of the vacuum engine is to help prevent transaction id
// wraparound which can be a problem for high transaction workloads.
//
// Once a chunk is compressed, it is unlikely to be modified.
// Therefore, a compressed chunk should only need to be vacuumed once
// and never again. We want to vacuum a compressed chunk as soon as
// possible in hopes of freezing all the pages for the chunk. We want
// to freeze so that we don't risk burning through transaction ids too
// quickly under heavy ingest.
//
// The vacuum engine periodically wakes up. If it can grab an advisory
// lock (only one vacuum engine runs at a time per database regardless
// of the number of connectors), it will look for compressed chunks
// that are not fully frozen.
//
// If chunks needing freezing are found, it uses the maximum
// transaction age of the chunks in the set to determine how many
// workers to use to vacuum the chunks. The closer the maximum
// transaction age is to autovacuum_freeze_max_age the more workers are
// used (up to a maximum number). This is done to throttle how much
// database CPU is consumed by vacuuming.
//
// We ignore chunks that have been vacuumed in the last 15 minutes.
// We ignore chunks with a transaction id age less than
// vacuum_freeze_min_age.
//
// We grab a list of chunks and then let workers pull from this list.
// Vacuuming may take a while, so there is a decent chance that the
// postgres autovacuum engine may vacuum a chunk before we get to it.
// We get the autovacuum count when we produce the list. Just before
// vacuuming a chunk, we check the autovacuum count again to see if it
// has increased. If it has, the autovacuum engine beat us to the
// chunk, and we skip it.
//
// Additionally, we have seen instances in which compresses chunks are
// missing statistics. Autovacuum ignores tables that are missing
// statistics. Analyzing these tables does not help since we rarely
// modify chunks after they are compressed. Therefore, these chunks are
// ignored until they pass the vacuum_freeze_max_age. This can be bad
// for performance. So, the vacuum engine also looks for these chunks
// and vacuums them. We only use one worker for these.
package vacuum

import (
	"context"
	"flag"
	"fmt"
	"math"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/timescale/promscale/pkg/log"
	"github.com/timescale/promscale/pkg/pgxconn"
	"github.com/timescale/promscale/pkg/util"
)

var (
	numberVacuumConnections = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: util.PromNamespace,
		Subsystem: "vacuum",
		Name:      "num_connections",
		Help:      "Number of database connections currently in use by the vacuum engine. One taken up by the advisory lock, the rest by vacuum commands.",
	})
	vacuumErrorsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: util.PromNamespace,
		Subsystem: "vacuum",
		Name:      "errors_total",
		Help:      "Total number of errors encountered by the Promscale vacuum engine while vacuuming tables.",
	},
		[]string{"workload"},
	)
	tablesVacuumedTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: util.PromNamespace,
		Subsystem: "vacuum",
		Name:      "tables_vacuumed_total",
		Help:      "Total number of compressed chunks vacuumed by the Promscale vacuum engine.",
	},
		[]string{"workload"},
	)
	tablesToVacuum = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: util.PromNamespace,
		Subsystem: "vacuum",
		Name:      "tables_to_vacuum_total",
		Help:      "Number of compressed chunks needing a vacuum detected on this iteration of the engine. This will never exceed 1000 even if there are more to vacuum.",
	},
		[]string{"workload"},
	)
	vacuumDurationSeconds = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: util.PromNamespace,
		Subsystem: "vacuum",
		Name:      "vacuum_duration_seconds",
		Help:      "Time spent vacuuming chunks.",
	},
		[]string{"workload"},
	)
)

func init() {
	prometheus.MustRegister(tablesVacuumedTotal, vacuumErrorsTotal, tablesToVacuum, numberVacuumConnections, vacuumDurationSeconds)
}

const (
	sqlAboveVersion12    = "SELECT current_setting('server_version_num')::int >= 130000"
	sqlAcquireLock       = "SELECT _prom_catalog.lock_for_vacuum_engine()"
	sqlVacuumFmt12       = "VACUUM (FREEZE, ANALYZE) %s"             // for postgres v12
	sqlVacuumFmt         = "VACUUM (FREEZE, ANALYZE, PARALLEL 1) %s" // for postgres > v12
	sqlReleaseLock       = "SELECT _prom_catalog.unlock_for_vacuum_engine()"
	sqlGetVacuumSettings = `
	SELECT
		current_setting('vacuum_freeze_min_age')::bigint as vacuum_freeze_min_age,
		current_setting('autovacuum_freeze_max_age')::bigint as autovacuum_freeze_max_age
	`
	sqlListChunksToFreeze = `
	SELECT 
		format('%I.%I', schema_name, table_name) AS schema_table,
		pg_stat_get_autovacuum_count(format('%I.%I', schema_name, table_name)::regclass::oid) AS autovacuum_count,
		pg_catalog.age(relfrozenxid) AS age
	FROM _ps_catalog.compressed_chunks_to_freeze 
	WHERE coalesce(last_vacuum, '-infinity'::timestamptz) < now() - interval '15 minutes' 
	AND pg_catalog.age(relfrozenxid) > current_setting('vacuum_freeze_min_age')::bigint
	ORDER BY pg_catalog.age(relfrozenxid) DESC --oldest first
	LIMIT 1000`
	sqlListChunksMissingStats = `
	SELECT 
		format('%I.%I', schema_name, table_name) AS schema_table,
		0 AS autovacuum_count,
		pg_catalog.age(relfrozenxid) AS age
	FROM _ps_catalog.compressed_chunks_missing_stats
	-- older than 20% of vacuum_freeze_min_age 
	WHERE pg_catalog.age(relfrozenxid) > (current_setting('vacuum_freeze_min_age')::bigint / 5)
	ORDER BY pg_catalog.age(relfrozenxid) DESC --oldest first
	LIMIT 1000
	`
	// finds out how many times the autovacuum engine has vacuumed a chunk
	sqlGetAutovacuumCount = "SELECT pg_catalog.pg_stat_get_autovacuum_count($1::regclass::oid)"
	// delay - on each iteration of the engine, we will sleep a bit before querying
	// a new list of chunks to give the ones last vacuumed time to have their stats updated
	delay          = 10 * time.Second
	defaultDisable = false
	// defaultRunFrequency is how often the engine wakes up and looks for work
	defaultRunFrequency = 10 * time.Minute
	// defaultParallelism is the maximum number of concurrent workers used to vacuum chunks
	defaultMaxParallelism = 4
	minParallelism        = 1
)

type Config struct {
	Disable        bool
	RunFrequency   time.Duration
	MaxParallelism int
}

func ParseFlags(fs *flag.FlagSet, cfg *Config) *Config {
	fs.BoolVar(&cfg.Disable, "vacuum.disable", defaultDisable, "disables the vacuum engine")
	fs.DurationVar(&cfg.RunFrequency, "vacuum.run-frequency", defaultRunFrequency, "how often should the vacuum engine run")
	fs.IntVar(&cfg.MaxParallelism, "vacuum.parallelism", defaultMaxParallelism, "the maximum number of goroutines/connections used to vacuum")
	return cfg
}

func Validate(cfg *Config) error {
	if cfg.Disable {
		return nil
	}
	if cfg.MaxParallelism < minParallelism {
		return fmt.Errorf("vacuum.parallelism must be at least %d: %d", minParallelism, cfg.MaxParallelism)
	}
	if cfg.RunFrequency <= 0 {
		return fmt.Errorf("vacuum.run-frequency must be positive: %d", cfg.RunFrequency)
	}
	return nil
}

// Engine periodically vacuums compressed chunks
type Engine struct {
	runFreq        time.Duration
	pool           pgxconn.PgxConn
	maxParallelism int
	mu             sync.Mutex
	kill           func()
	vacuumSQL      string
}

// NewEngine creates a new Engine
func NewEngine(pool pgxconn.PgxConn, runFreq time.Duration, maxParallelism int) (*Engine, error) {
	e := &Engine{
		runFreq:        runFreq,
		pool:           pool,
		maxParallelism: maxParallelism,
	}
	if sql, err := e.getVacuumSQL(); err != nil {
		log.Error("msg", "failed to determine the appropriate vacuum command based on postgres version", "error", err)
		return nil, err
	} else {
		e.vacuumSQL = sql
	}
	return e, nil
}

// getVacuumSQL gets the appropriate VACUUM SQL statement based on the Postgres version.
// Version 12 does not support the PARALLEL option to the VACUUM command
func (e *Engine) getVacuumSQL() (string, error) {
	var aboveVersion12 bool
	err := e.pool.QueryRow(context.Background(), sqlAboveVersion12).Scan(&aboveVersion12)
	if err != nil {
		return "", err
	}
	if aboveVersion12 {
		return sqlVacuumFmt, nil
	}
	return sqlVacuumFmt12, nil
}

// Start starts the Engine
// Blocks forever unless Stop is called
func (e *Engine) Start() {
	var execute, kill = every(e.runFreq, e.Run)
	func() {
		e.mu.Lock()
		defer e.mu.Unlock()
		e.kill = kill
	}()
	execute() // blocks forever unless kill is called
}

// Stop stops the engine if it is running
func (e *Engine) Stop() {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.kill != nil {
		e.kill()
	}
}

// every executes a task periodically
// returns an execute function which when called will block
// and execute task periodically, and a kill function which will
// terminate the execution function if called
func every(every time.Duration, task func(ctx context.Context)) (execute, kill func()) {
	var ticker = time.NewTicker(every)
	var once sync.Once
	var ctx, cancel = context.WithCancel(context.Background())

	kill = func() {
		once.Do(func() {
			ticker.Stop()
			cancel()
		})
	}

	execute = func() {
		defer kill()
		// loop forever, or until the done signal is received
		for {
			select {
			case <-ticker.C:
				task(ctx)
			case <-ctx.Done():
				return
			}
		}
	}
	return
}

type chunk struct {
	name            string
	autovacuumCount int64
	age             int64
}

type workload struct {
	name         string
	workload     string
	query        string
	scaleWorkers bool
	stop         bool
}

// Run attempts vacuum a batch of compressed chunks
func (e *Engine) Run(ctx context.Context) {
	const locking = "locking"
	// grab a database connection and attempt to acquire an advisory lock
	// if we get the lock, we'll hold it on this connection while the vacuum
	// work is done
	con, err := e.pool.Acquire(ctx)
	if err != nil {
		log.Error("msg", "failed to acquire a db connection", "error", err)
		vacuumErrorsTotal.WithLabelValues(locking).Inc()
		return
	}
	defer con.Release() // return the connection to the pool when finished with it
	acquired := false
	err = con.QueryRow(ctx, sqlAcquireLock).Scan(&acquired)
	if err != nil {
		log.Error("msg", "failed to attempt to acquire advisory lock", "error", err)
		vacuumErrorsTotal.WithLabelValues(locking).Inc()
		return
	}
	if !acquired {
		log.Debug("msg", "vacuum engine did not acquire advisory lock")
		return
	}
	numberVacuumConnections.Set(1)
	// release the advisory lock when we're done
	defer func() {
		// don't use the passed context.
		// we need to release the lock even if the context was cancelled
		_, err := con.Exec(context.Background(), sqlReleaseLock)
		if err != nil {
			log.Error("msg", "vacuum engine failed to release advisory lock", "error", err)
			vacuumErrorsTotal.WithLabelValues(locking).Inc()
		}
		numberVacuumConnections.Set(0)
	}()

	// okay, we have the advisory lock, let's do some work...

	chunksToFreeze := workload{
		name:         "compressed chunks to freeze",
		workload:     "compressed-chunks-to-freeze",
		query:        sqlListChunksToFreeze,
		scaleWorkers: true,
		stop:         false,
	}
	chunksMissingStats := workload{
		name:         "compressed chunks missing stats",
		workload:     "compressed-chunks-missing-stats",
		query:        sqlListChunksMissingStats,
		scaleWorkers: false,
		stop:         false,
	}

	for i := 0; true; i++ {
		// continue looping until both workloads were ready
		// to stop on their last iterations
		if chunksToFreeze.stop && chunksMissingStats.stop {
			break
		}

		// every 3rd iteration is chunksMissingStats
		w := &chunksToFreeze
		if i%3 == 0 {
			w = &chunksMissingStats
		}
		w.stop = false

		// find some chunks to vacuum
		chunks, err := e.listChunks(ctx, con, w.query)
		if err != nil {
			log.Error("msg", fmt.Sprintf("failed to list %s", w.name), "error", err)
			vacuumErrorsTotal.WithLabelValues(w.workload).Inc()
			w.stop = true
			continue
		}
		tablesToVacuum.WithLabelValues(w.workload).Set(float64(len(chunks)))
		if len(chunks) == 0 {
			log.Debug("msg", fmt.Sprintf("zero %s", w.name))
			w.stop = true
			continue
		}
		log.Debug("msg", w.name, "count", len(chunks))

		// how many workers should we use?
		numWorkers := 1
		if w.scaleWorkers {
			numWorkers, err = e.calcNumWorkers(ctx, con, chunks)
			if err != nil {
				log.Error("msg", "failed to calc num workers", "error", err)
				vacuumErrorsTotal.WithLabelValues(w.workload).Inc()
				w.stop = true
				continue
			}
		}

		// vacuum the chunks
		runWorkers(ctx, w.workload, numWorkers, chunks, e.worker)
		log.Debug("msg", "vacuum workers finished. delaying before next iteration...")
		// in some cases, have seen it take up to 10 seconds for the stats to be updated post vacuum
		time.Sleep(delay)
	}
}

// listChunks identifies chunks which need to be vacuumed
func (e *Engine) listChunks(ctx context.Context, con *pgxpool.Conn, sql string) ([]*chunk, error) {
	rows, err := con.Query(ctx, sql)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	chunks := make([]*chunk, 0)
	for rows.Next() {
		c := chunk{}
		err := rows.Scan(&c.name, &c.autovacuumCount, &c.age)
		if err != nil {
			return nil, err
		}
		chunks = append(chunks, &c)
	}
	return chunks, nil
}

func (e *Engine) getVacuumSettings(ctx context.Context, con *pgxpool.Conn) (vacuumFreezeMinAge, autovacuumFreezeMaxAge int64, err error) {
	err = con.QueryRow(ctx, sqlGetVacuumSettings).Scan(&vacuumFreezeMinAge, &autovacuumFreezeMaxAge)
	if err != nil {
		return 0, 0, err
	}
	return
}

// interpolate figures out how many workers to used based on the oldest transaction id age of the
// chunks in our batch. The age should be between vacuumFreezeMinAge and autovacuumFreezeMaxAge.
// The closer the maxChunkAge is to the autovacuumFreezeMaxAge, the more workers we assign. If
// maxChunkAge is == vacuumFreezeMinAge, then we just use 1 worker.
//
//	       vacuumFreezeMinAge
//	        |              maxChunkAge
//	        |               |       autovacuumFreezeMaxAge
//	0-------|---------------X--------|------
//	        1000            6789     11000
//
// In this example, maxChunkAge is 57.89% of the way between vacuumFreezeMinAge and
// autovacuumFreezeMaxAge. If maxParallelism is 5, then we will use 3 workers.
func interpolate(vacuumFreezeMinAge, autovacuumFreezeMaxAge, maxChunkAge, maxParallelism float64) int {
	if maxParallelism == 1 {
		return 1
	}
	if maxChunkAge <= vacuumFreezeMinAge {
		// this "shouldn't happen" but just in case
		return 1
	}
	if autovacuumFreezeMaxAge <= maxChunkAge {
		return int(maxParallelism)
	}
	if autovacuumFreezeMaxAge <= vacuumFreezeMinAge {
		// this "shouldn't happen" but just in case
		return int(maxParallelism)
	}
	percent := (maxChunkAge - vacuumFreezeMinAge) / (autovacuumFreezeMaxAge - vacuumFreezeMinAge)
	workers := math.Trunc(maxParallelism*percent) + 1
	// bounds check. workers should be between 1 and maxParallelism
	if workers > maxParallelism {
		workers = maxParallelism
	} else if workers < 1 {
		workers = 1
	}
	return int(workers)
}

// calcNumWorkers calculates the number of workers to use to vacuum chunks. The closer the oldest chunk
// age gets to autovacuumFreezeMaxAge, the more workers we will use
func (e *Engine) calcNumWorkers(ctx context.Context, con *pgxpool.Conn, chunks []*chunk) (int, error) {
	vacuumFreezeMinAge, autovacuumFreezeMaxAge, err := e.getVacuumSettings(ctx, con)
	if err != nil {
		log.Error("msg", "failed to get vacuum settings", "error", err)
		return 0, err
	}
	// never ought to have an empty slice, but just in case...
	if len(chunks) == 0 {
		return 1, nil
	}
	var maxChunkAge = chunks[0].age // query ordered by oldest age first
	w := interpolate(float64(vacuumFreezeMinAge), float64(autovacuumFreezeMaxAge), float64(maxChunkAge), float64(e.maxParallelism))
	if len(chunks) < w {
		// don't spin up more workers than we could possibly use
		w = len(chunks)
	}
	return w, nil
}

// runWorkers kicks off a number of goroutines to work on the chunks in parallel
// blocks until the workers complete
func runWorkers(ctx context.Context, workload string, numWorkers int, chunks []*chunk, worker func(context.Context, string, int, <-chan *chunk)) {
	todo := make(chan *chunk, len(chunks))
	var wg sync.WaitGroup
	wg.Add(numWorkers)
	for id := 0; id < numWorkers; id++ {
		go func(ctx context.Context, workload string, id int, todo <-chan *chunk) {
			defer wg.Done()
			worker(ctx, workload, id, todo)
		}(ctx, workload, id, todo)
	}
	for _, c := range chunks {
		todo <- c
	}
	close(todo)
	wg.Wait()
}

// getAutovacuumCount gets the current number of times autovacuum has vacuumed a table
func (e *Engine) getAutovacuumCount(ctx context.Context, con *pgxpool.Conn, name string) (autovacuumCount int64, err error) {
	err = con.QueryRow(ctx, sqlGetAutovacuumCount, name).Scan(&autovacuumCount)
	if err != nil {
		return 0, err
	}
	return
}

// worker pulls chunks from a channel and vacuums them
func (e *Engine) worker(ctx context.Context, workload string, id int, todo <-chan *chunk) {
	con, err := e.pool.Acquire(ctx)
	if err != nil {
		log.Error("msg", "failed to acquire a database connection", "worker", id, "error", err)
		vacuumErrorsTotal.WithLabelValues(workload).Inc()
		return
	}
	numberVacuumConnections.Inc()
	defer func() {
		con.Release()
		numberVacuumConnections.Dec()
	}()

	for c := range todo {
		// see if the current autovacuum count is higher than the count that we got
		// when we first listed chunks to vacuum. if it is higher, then the autovacuum
		// engine got to this chunk before we did. skip it
		autovacuumCount, err := e.getAutovacuumCount(ctx, con, c.name)
		if err != nil {
			log.Error("msg", "failed to get current autovacuum count", "chunk", c.name, "worker", id, "error", err)
			vacuumErrorsTotal.WithLabelValues(workload).Inc()
			continue
		}
		if c.autovacuumCount < autovacuumCount {
			log.Debug("msg", "autovacuum has vacuumed this chunk before we got to it. skipping.", "chunk", c.name, "worker", id)
			continue
		}
		// vacuum the chunk
		log.Debug("msg", "vacuuming a chunk", "worker", id, "chunk", c.name)
		sql := fmt.Sprintf(e.vacuumSQL, c.name)
		before := time.Now()
		_, err = con.Exec(ctx, sql)
		elapsed := time.Since(before).Seconds()
		if err != nil {
			log.Error("msg", "failed to vacuum chunk", "chunk", c.name, "worker", id, "error", err)
			vacuumErrorsTotal.WithLabelValues(workload).Inc()
		} else {
			vacuumDurationSeconds.WithLabelValues(workload).Observe(elapsed)
			tablesVacuumedTotal.WithLabelValues(workload).Inc()
		}
	}
}
