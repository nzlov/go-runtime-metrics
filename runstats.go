package runstats

import (
	"context"
	"log"
	"os"
	"time"

	influxdb2 "github.com/influxdata/influxdb-client-go/v2"
	"github.com/influxdata/influxdb-client-go/v2/api"
	"github.com/nzlov/go-runtime-metrics/collector"
	"github.com/pkg/errors"
)

const (
	defaultHost               = "localhost:8086"
	defaultMeasurement        = "go.runtime"
	defaultBucket             = "go"
	defaultOrg                = "metrics"
	defaultCollectionInterval = 10 * time.Second
)

// A configuration with default values.
var DefaultConfig = &Config{}

type Config struct {
	// InfluxDb host:port pair.
	// Default is "localhost:8086".
	Host string `json:"host" yaml:"host" mapstructure:"host"`

	// Token.
	Token string `json:"token" yaml:"token" mapstructure:"token"`

	// Org.
	Org string `json:"org" yaml:"org" mapstructure:"org"`

	// Bucket.
	Bucket string `json:"bucket" yaml:"bucket" mapstructure:"bucket"`

	// Measurement to write points to.
	// Default is "go.runtime.<hostname>".
	Measurement string `json:"measurement" yaml:"measurement" mapstructure:"measurement"`

	// Interval at which to collect points.
	// Default is 10 seconds
	CollectionInterval time.Duration `json:"collection_interval" yaml:"collection_interval" mapstructure:"collection_interval"`

	// Disable collecting CPU Statistics. cpu.*
	// Default is false
	DisableCpu bool `json:"disable_cpu" yaml:"disable_cpu" mapstructure:"disable_cpu"`

	// Disable collecting Memory Statistics. mem.*
	DisableMem bool `json:"disable_mem" yaml:"disable_mem" mapstructure:"disable_mem"`

	// Disable collecting GC Statistics (requires Memory be not be disabled). mem.gc.*
	DisableGc bool `json:"disable_gc" yaml:"disable_gc" mapstructure:"disable_gc"`
}

func (config *Config) init() (*Config, error) {
	if config == nil {
		config = DefaultConfig
	}

	if config.Org == "" {
		config.Org = defaultOrg
	}
	if config.Bucket == "" {
		config.Bucket = defaultBucket
	}

	if config.Host == "" {
		config.Host = defaultHost
	}

	if config.Measurement == "" {
		config.Measurement = defaultMeasurement

		if hn, err := os.Hostname(); err != nil {
			config.Measurement += ".unknown"
		} else {
			config.Measurement += "." + hn
		}
	}

	if config.CollectionInterval == 0 {
		config.CollectionInterval = defaultCollectionInterval
	}

	return config, nil
}

func RunCollector(ctx context.Context, config *Config) (*RunStats, error) {
	var err error
	if config, err = config.init(); err != nil {
		return nil, err
	}

	// Make client
	client := influxdb2.NewClient(config.Host, config.Token)
	// always close client at the end

	if err != nil {
		return nil, errors.Wrap(err, "failed to create influxdb client")
	}

	// Ping InfluxDB to ensure there is a connection
	if _, err := client.Ready(context.Background()); err != nil {
		return nil, errors.Wrap(err, "influxdb no ready")
	}

	_runStats := &RunStats{
		client: client,
		config: config,
		write:  client.WriteAPI(config.Org, config.Bucket),
	}

	_collector := collector.New(_runStats.onNewPoint)
	_collector.PauseDur = config.CollectionInterval
	_collector.EnableCPU = !config.DisableCpu
	_collector.EnableMem = !config.DisableMem
	_collector.EnableGC = !config.DisableGc

	go _collector.Run()

	return _runStats, nil
}

type RunStats struct {
	logger Logger
	client influxdb2.Client
	config *Config
	write  api.WriteAPI
}

func (r *RunStats) Logger(log Logger) {
	r.logger = log
}

func (r *RunStats) onNewPoint(fields collector.Fields) {
	r.write.WritePoint(influxdb2.NewPoint(r.config.Measurement, fields.Tags(), fields.Values(), time.Now()))
}

type Logger interface {
	Println(v ...interface{})
	Fatalln(v ...interface{})
}

type DefaultLogger struct{}

func (*DefaultLogger) Println(v ...interface{}) {}
func (*DefaultLogger) Fatalln(v ...interface{}) { log.Fatalln(v...) }
