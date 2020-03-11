package main

import (
	"time"
)

type LoadConfig struct {
	Format     string
	DataSource *DataSourceConfig `yaml:"data-source,omitempty" mapstructure:"data-source"`
	Loader     *LoaderConfig     `yaml:"loader,omitempty"`
}

type LoaderConfig struct {
	Runner     *RunnerConfig
	DBSpecific interface{} `yaml:"db-specific" mapstructure:"db-specific"`
}

type RunnerConfig struct {
	DBName          string `yaml:"db-name" mapstructure:"db-name"`
	BatchSize       uint   `yaml:"batch-size" mapstructure:"batch-size"`
	Workers         uint
	Limit           uint64
	DoLoad          bool          `yaml:"do-load" mapstructure:"do-load"`
	DoCreateDB      bool          `yaml:"do-create-db" mapstructure:"do-create-db"`
	DoAbortOnExist  bool          `yaml:"do-abort-on-exist" mapstructure:"do-abort-on-exist"`
	ReportingPeriod time.Duration `yaml:"reporting-period" mapstructure:"reporting-period"`
	Seed            int64
	HashWorkers     bool `yaml:"hash-workers" mapstructure:"hash-workers"`
}

type DataSourceConfig struct {
	Type      string
	File      *FileDataSourceConfig
	Simulator *SimulatorDataSourceConfig
}

type FileDataSourceConfig struct {
	Location string `yaml:"location"`
}

type SimulatorDataSourceConfig struct {
	Use                   string `yaml:"use-case" mapstructure:"use-case"`
	Scale                 uint64
	TimeStart             string `yaml:"timestamp-start" mapstructure:"timestamp-start"`
	TimeEnd               string `yaml:"timestamp-end" mapstructure:"timestamp-end"`
	Seed                  int64
	Debug                 int           `yaml:"debug,omitempty"`
	Limit                 uint64        `yaml:"max-data-points" mapstructure:"max-data-points"`
	LogInterval           time.Duration `yaml:"log-interval" mapstructure:"log-interval"`
	MaxMetricCountPerHost uint64        `yaml:"max-metric-count" mapstructure:"max-metric-count"`
}
