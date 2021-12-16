package dataset

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v4"
	"github.com/timescale/promscale/pkg/log"
	"github.com/timescale/promscale/pkg/pgmodel/common/schema"
	"gopkg.in/yaml.v2"
)

const defaultChunkInterval = 8 * time.Hour

var setDefaultChunkIntervalSQL = fmt.Sprintf("SELECT %s.set_default_chunk_interval($1)", schema.Prom)

type Config struct {
	Metrics Metrics `yaml:"metrics"`
}

type Metrics struct {
	ChunkInterval time.Duration `yaml:"default_chunk_interval"`
}

func NewConfig(contents string) (cfg Config, err error) {
	err = yaml.Unmarshal([]byte(contents), &cfg)
	return cfg, err
}

func (c *Config) Apply(conn *pgx.Conn) error {
	if c.Metrics.ChunkInterval <= 0 {
		c.Metrics.ChunkInterval = defaultChunkInterval
	}

	log.Info("msg", fmt.Sprintf("Setting dataset default chunk interval to %s", c.Metrics.ChunkInterval))

	_, err := conn.Exec(context.Background(), setDefaultChunkIntervalSQL, c.Metrics.ChunkInterval)
	return err
}
