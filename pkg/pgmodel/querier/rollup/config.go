package rollup

import "time"

type Config struct {
	schemaName string
	interval   time.Duration
	managerRef *Manager
}

func (c *Config) Interval() time.Duration {
	return c.interval
}

func (c *Config) SchemaName() string {
	return c.schemaName
}
