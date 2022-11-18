package vacuum

import (
	"context"
	"flag"
	"fmt"
	"sync"
	"time"

	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/timescale/promscale/pkg/log"
	"github.com/timescale/promscale/pkg/pgxconn"
	"github.com/timescale/promscale/pkg/util"
)

var (
	tablesVacuumedTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: util.PromNamespace,
		Subsystem: "vacuum",
		Name:      "tables_vacuumed_total",
		Help:      "Total number of tables vacuumed by the Promscale vacuum engine.",
	})
	vacuumErrorsTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: util.PromNamespace,
		Subsystem: "vacuum",
		Name:      "vacuum_errors_total",
		Help:      "Total number of errors encountered by the Promscale vacuum engine while vacuuming tables.",
	})
	tablesNeedingVacuum = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: util.PromNamespace,
		Subsystem: "vacuum",
		Name:      "tables_needing_vacuum",
		Help:      "Number of tables needing a vacuum detected on this iteration of the engine. This will never exceed 1000 even if there are more to vacuum.",
	})
	numberVacuumConnections = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: util.PromNamespace,
		Subsystem: "vacuum",
		Name:      "number_vacuum_connections",
		Help:      "Number of database connections currently in use by the vacuum engine. One taken up by the advisory lock, the rest by vacuum commands.",
	})
)

func init() {
	prometheus.MustRegister(tablesVacuumedTotal, vacuumErrorsTotal, tablesNeedingVacuum, numberVacuumConnections)
}

const (
	sqlAcquireLock       = "SELECT _prom_catalog.lock_for_vacuum_engine()"
	sqlVacuumFmt         = "VACUUM (FREEZE, ANALYZE) %s"
	sqlReleaseLock       = "SELECT _prom_catalog.unlock_for_vacuum_engine()"
	sqlGetVacuumSettings = `
	SELECT 
		max(s.setting::bigint) filter (where s.name = 'vacuum_freeze_min_age') as vacuum_freeze_min_age,
		max(s.setting::bigint) filter (where s.name = 'autovacuum_freeze_max_age') as autovacuum_freeze_max_age
	FROM pg_catalog.pg_settings s
	WHERE s.name IN ('vacuum_freeze_min_age', 'autovacuum_freeze_max_age')
	`
	sqlListChunks = `
	SELECT 
		format('%I.%I', schema_name, table_name) AS schema_table,
		pg_stat_get_autovacuum_count(format('%I.%I', schema_name, table_name)::regclass::oid) AS autovacuum_count,
		pg_catalog.age(relfrozenxid) as age
	FROM _ps_catalog.chunks_to_freeze 
	WHERE coalesce(last_vacuum, '-infinity'::timestamptz) < now() - interval '15 minutes' 
	AND pg_catalog.age(relfrozenxid) > (SELECT setting::bigint FROM pg_settings WHERE name = 'vacuum_freeze_min_age')
	LIMIT 1000`
	delay               = 10 * time.Second
	defaultDisable      = false
	defaultRunFrequency = 10 * time.Minute
	defaultParallelism  = 4
	minParallelism      = 1
)

type Config struct {
	Disable      bool
	RunFrequency time.Duration
	Parallelism  int
}

func ParseFlags(fs *flag.FlagSet, cfg *Config) *Config {
	fs.BoolVar(&cfg.Disable, "vacuum.disable", defaultDisable, "disables the vacuum engine")
	fs.DurationVar(&cfg.RunFrequency, "vacuum.run-frequency", defaultRunFrequency, "how often should the vacuum engine run")
	fs.IntVar(&cfg.Parallelism, "vacuum.parallelism", defaultParallelism, "how many goroutines/connections should be used to vacuum")
	return cfg
}

func Validate(cfg *Config) error {
	if cfg.Disable {
		return nil
	}
	if cfg.Parallelism < minParallelism {
		return fmt.Errorf("vacuum.parallelism must be at least %d: %d", minParallelism, cfg.Parallelism)
	}
	if cfg.RunFrequency <= 0 {
		return fmt.Errorf("vacuum.run-frequency must be positive: %d", cfg.RunFrequency)
	}
	return nil
}

// Engine periodically vacuums compressed chunks
type Engine struct {
	runFreq     time.Duration
	pool        pgxconn.PgxConn
	parallelism int
	mu          sync.Mutex
	kill        func()
}

// NewEngine creates a new Engine
func NewEngine(pool pgxconn.PgxConn, runFreq time.Duration, parallelism int) *Engine {
	return &Engine{
		runFreq:     runFreq,
		pool:        pool,
		parallelism: parallelism,
	}
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

// Run attempts vacuum a batch of compressed chunks
func (e *Engine) Run(ctx context.Context) {
	con, err := e.pool.Acquire(ctx)
	if err != nil {
		log.Error("msg", "failed to acquire a db connection", "error", err)
		return
	}
	defer con.Release() // return the connection to the pool when finished with it
	acquired := false
	err = con.QueryRow(ctx, sqlAcquireLock).Scan(&acquired)
	if err != nil {
		log.Error("msg", "failed to attempt to acquire advisory lock", "error", err)
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
		}
		numberVacuumConnections.Set(0)
	}()
	// we limit ourselves to batches of 1000 chunks
	// since we already have the advisory lock, continue to vacuum batches as needed until none left
	for {
		chunks, err := e.listChunks(ctx, con)
		if err != nil {
			log.Error("msg", "failed to list chunks for vacuuming", "error", err)
			return
		}
		tablesNeedingVacuum.Set(float64(len(chunks)))
		if len(chunks) == 0 {
			log.Info("msg", "zero compressed chunks need to be vacuumed")
			return
		}
		log.Info("msg", "compressed chunks need to be vacuumed", "count", len(chunks))
		numWorkers, err := e.calcNumWorkers(ctx, con, chunks)
		if err != nil {
			log.Error("msg", "failed to calc num workers", "error", err)
			return
		}
		runWorkers(ctx, numWorkers, chunks, e.worker)
		// in some cases, have seen it take up to 10 seconds for the stats to be updated post vacuum
		time.Sleep(delay)
	}
}

// listChunks identifies chunks which need to be vacuumed
func (e *Engine) listChunks(ctx context.Context, con *pgxpool.Conn) ([]*chunk, error) {
	rows, err := con.Query(ctx, sqlListChunks)
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

func interpolate(vacuumFreezeMinAge, autovacuumFreezeMaxAge, maxChunkAge, maxParallelism int64) int {
	if maxChunkAge <= vacuumFreezeMinAge {
		return 1
	}
	agePerWorker := (autovacuumFreezeMaxAge - vacuumFreezeMinAge) / maxParallelism
	workers := (maxChunkAge - vacuumFreezeMinAge) / agePerWorker
	if workers > maxParallelism {
		workers = maxParallelism
	} else if workers < 1 {
		workers = 1
	}
	return int(workers)
}

func (e *Engine) calcNumWorkers(ctx context.Context, con *pgxpool.Conn, chunks []*chunk) (int, error) {
	vacuumFreezeMinAge, autovacuumFreezeMaxAge, err := e.getVacuumSettings(ctx, con)
	if err != nil {
		log.Error("msg", "failed to get vacuum settings", "error", err)
		return 0, err
	}
	var maxChunkAge int64 = 0
	for _, c := range chunks {
		if c.age > maxChunkAge {
			maxChunkAge = c.age
		}
	}
	w := interpolate(vacuumFreezeMinAge, autovacuumFreezeMaxAge, maxChunkAge, int64(e.parallelism))
	if len(chunks) < w {
		// don't spin up more workers than we could possibly use
		w = len(chunks)
	}
	return w, nil
}

// runWorkers kicks off a number of goroutines to work on the chunks in parallel
// blocks until the workers complete
func runWorkers(ctx context.Context, numWorkers int, chunks []*chunk, worker func(context.Context, int, <-chan *chunk)) {
	todo := make(chan *chunk, len(chunks))
	var wg sync.WaitGroup
	wg.Add(numWorkers)
	for id := 0; id < numWorkers; id++ {
		go func(ctx context.Context, id int, todo <-chan *chunk) {
			defer wg.Done()
			worker(ctx, id, todo)
		}(ctx, id, todo)
	}
	for _, c := range chunks {
		todo <- c
	}
	close(todo)
	wg.Wait()
}

// worker pulls chunks from a channel and vacuums them
func (e *Engine) worker(ctx context.Context, id int, todo <-chan *chunk) {
	for c := range todo {
		log.Debug("msg", "vacuuming a chunk", "worker", id, "chunk", c.name)
		sql := fmt.Sprintf(sqlVacuumFmt, c.name)
		numberVacuumConnections.Inc()
		_, err := e.pool.Exec(ctx, sql)
		numberVacuumConnections.Dec()
		if err != nil {
			log.Error("msg", "failed to vacuum chunk", "chunk", c.name, "worker", id, "error", err)
			vacuumErrorsTotal.Inc()
			// don't return error here. attempt to vacuum other chunks. keep working
		} else {
			tablesVacuumedTotal.Inc()
		}
	}
}
