package vacuum

import (
	"context"
	"flag"
	"fmt"
	"sync"
	"time"

	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/timescale/promscale/pkg/log"
	"github.com/timescale/promscale/pkg/pgxconn"
)

const (
	sqlStart            = "SELECT pg_try_advisory_lock(1982010619820106)"
	sqlListChunks       = "SELECT format('%I.%I', schema_name, table_name) from _ps_catalog.chunks_to_freeze LIMIT 1000"
	sqlVacuumFmt        = "VACUUM (FREEZE, ANALYZE) %s"
	sqlStop             = "SELECT pg_advisory_unlock(1982010619820106)"
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
	var execute, kill = every(e.runFreq, e.RunOnce)
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
func every(every time.Duration, task func()) (execute, kill func()) {
	var ticker = time.NewTicker(every)
	var once sync.Once
	var die = make(chan struct{}, 1)

	kill = func() {
		once.Do(func() {
			ticker.Stop()
			close(die)
		})
	}

	execute = func() {
		defer kill() // be sure to clean up our ticker and channel
		// loop forever, or until the die signal is received
		for {
			select {
			case <-ticker.C:
				task()
			case <-die:
				return
			}
		}
	}
	return
}

// RunOnce attempts vacuum a batch of compressed chunks
func (e *Engine) RunOnce() {
	con, err := e.pool.Acquire(context.Background())
	if err != nil {
		log.Error("msg", "failed to acquire a db connection", "error", err)
		return
	}
	defer con.Release() // return the connection to the pool when finished with it
	acquired := false
	err = con.QueryRow(context.Background(), sqlStart).Scan(&acquired)
	if err != nil {
		log.Error("msg", "failed to attempt to acquire advisory lock", "error", err)
		return
	}
	if !acquired {
		log.Info("msg", "vacuum engine did not acquire advisory lock")
		return
	}
	// release the advisory lock when we're done
	defer func() {
		_, err := con.Exec(context.Background(), sqlStop)
		if err != nil {
			log.Error("msg", "vacuum engine failed to release advisory lock", "error", err)
		}
	}()
	// we limit ourselves to batches of 1000 chunks
	// since we already have the advisory lock, continue to vacuum batches as needed until none left
	for {
		chunks, err := e.listChunks(con)
		if err != nil {
			log.Error("msg", "failed to list chunks for vacuuming", "error", err)
			return
		}
		if len(chunks) == 0 {
			log.Info("msg", "zero compressed chunks need to be vacuumed")
			return
		}
		log.Info("msg", "compressed chunks need to be vacuumed", "count", len(chunks))
		p := e.parallelism
		if len(chunks) < p {
			// don't spin up more workers than we could possibly use
			// if parallelism is 6, but we only have 5 chunks to work on, use 5 workers
			p = len(chunks)
		}
		runWorkers(p, chunks, e.worker)
		// in some cases, have seen it take up to 10 seconds for the stats to be updated post vacuum
		time.Sleep(delay)
	}
}

// listChunks identifies chunks which need to be vacuumed
func (e *Engine) listChunks(con *pgxpool.Conn) ([]string, error) {
	rows, err := con.Query(context.Background(), sqlListChunks)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	tables := make([]string, 0)
	for rows.Next() {
		var table string
		err := rows.Scan(&table)
		if err != nil {
			return nil, err
		}
		tables = append(tables, table)
	}
	return tables, nil
}

// runWorkers kicks off a number of goroutines to work on the chunks in parallel
// blocks until the workers complete
func runWorkers(parallelism int, chunks []string, worker func(int, <-chan string)) {
	todo := make(chan string, len(chunks))
	var wg sync.WaitGroup
	wg.Add(parallelism)
	for id := 0; id < parallelism; id++ {
		go func(id int, todo <-chan string) {
			defer wg.Done()
			worker(id, todo)
		}(id, todo)
	}
	for _, chunk := range chunks {
		todo <- chunk
	}
	close(todo)
	wg.Wait()
}

// worker pulls chunks from a channel and vacuums them
func (e *Engine) worker(id int, todo <-chan string) {
	for chunk := range todo {
		log.Info("msg", "vacuuming a chunk", "worker", id, "chunk", chunk)
		sql := fmt.Sprintf(sqlVacuumFmt, chunk)
		_, err := e.pool.Exec(context.Background(), sql)
		if err != nil {
			log.Error("msg", "failed to vacuum chunk", "chunk", chunk, "worker", id, "error", err)
			// don't return error here. attempt to vacuum other chunks. keep working
		}
	}
}
