// Copyright 2017 Percona LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package collector

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"

	"github.com/shatteredsilicon/mongodb_exporter/collector/mongod"
	"github.com/shatteredsilicon/mongodb_exporter/collector/mongos"
	"github.com/shatteredsilicon/mongodb_exporter/shared"
)

const namespace = "mongodb"

// MongodbCollectorOpts is the options of the mongodb collector.
type MongodbCollectorOpts struct {
	ClientOpts               *options.ClientOptions
	TLSConnection            bool
	TLSCertificateFile       string
	TLSPrivateKeyFile        string
	TLSCaFile                string
	TLSHostnameValidation    bool
	DBPoolLimit              int
	CollectDatabaseMetrics   bool
	CollectCollectionMetrics bool
	CollectTopMetrics        bool
	CollectIndexUsageStats   bool
	SocketTimeout            time.Duration
	SyncTimeout              time.Duration
}

func (in *MongodbCollectorOpts) toMongoClientOps() *options.ClientOptions {
	serverAPI := options.ServerAPI(options.ServerAPIVersion1)
	return in.ClientOpts.SetServerAPIOptions(serverAPI).
		SetSocketTimeout(in.SocketTimeout).
		SetTimeout(in.SyncTimeout).
		SetMaxPoolSize(uint64(in.DBPoolLimit)).
		SetReadPreference(readpref.Nearest()).
		SetDirect(true)
}

// MongodbCollector is in charge of collecting mongodb's metrics.
type MongodbCollector struct {
	Opts *MongodbCollectorOpts

	scrapesTotal              prometheus.Counter
	scrapeErrorsTotal         prometheus.Counter
	lastScrapeError           prometheus.Gauge
	lastScrapeDurationSeconds prometheus.Gauge
	mongoUp                   prometheus.Gauge

	mongoClientLock sync.Mutex
	mongoClient     *mongo.Client
}

// NewMongodbCollector returns a new instance of a MongodbCollector.
func NewMongodbCollector(opts *MongodbCollectorOpts) *MongodbCollector {
	exporter := &MongodbCollector{
		Opts: opts,

		scrapesTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: "exporter",
			Name:      "scrapes_total",
			Help:      "Total number of times MongoDB was scraped for metrics.",
		}),
		scrapeErrorsTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: "exporter",
			Name:      "scrape_errors_total",
			Help:      "Total number of times an error occurred scraping a MongoDB.",
		}),
		lastScrapeError: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace,
			Subsystem: "exporter",
			Name:      "last_scrape_error",
			Help:      "Whether the last scrape of metrics from MongoDB resulted in an error (1 for error, 0 for success).",
		}),
		lastScrapeDurationSeconds: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace,
			Subsystem: "exporter",
			Name:      "last_scrape_duration_seconds",
			Help:      "Duration of the last scrape of metrics from MongoDB.",
		}),
		mongoUp: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "up",
			Help:      "Whether MongoDB is up.",
		}),
	}

	return exporter
}

// getMongoClient returns the cached *mongo.Client or creates a new client and returns it.
// Use sync.Mutex to avoid race condition around session creation.
func (exporter *MongodbCollector) getMongoClient() (*mongo.Client, error) {
	exporter.mongoClientLock.Lock()
	defer exporter.mongoClientLock.Unlock()

	var err error
	if exporter.mongoClient == nil {
		exporter.mongoClient, err = shared.MongoClient(context.TODO(), exporter.Opts.toMongoClientOps())
	}

	return exporter.mongoClient, err
}

// Close cleanly closes the mongo session if it exists.
func (exporter *MongodbCollector) Close() error {
	exporter.mongoClientLock.Lock()
	defer exporter.mongoClientLock.Unlock()

	if exporter.mongoClient != nil {
		return exporter.mongoClient.Disconnect(context.Background())
	}

	return nil
}

// Describe sends the super-set of all possible descriptors of metrics collected by this Collector
// to the provided channel and returns once the last descriptor has been sent.
// Part of prometheus.Collector interface.
func (exporter *MongodbCollector) Describe(ch chan<- *prometheus.Desc) {
	// We cannot know in advance what metrics the exporter will generate
	// from MongoDB. So we use the poor man's describe method: Run a collect
	// and send the descriptors of all the collected metrics. The problem
	// here is that we need to connect to the MongoDB. If it is currently
	// unavailable, the descriptors will be incomplete. Since this is a
	// stand-alone exporter and not used as a library within other code
	// implementing additional metrics, the worst that can happen is that we
	// don't detect inconsistent metrics created by this exporter
	// itself. Also, a change in the monitored MongoDB instance may change the
	// exported metrics during the runtime of the exporter.

	metricCh := make(chan prometheus.Metric)
	doneCh := make(chan struct{})

	go func() {
		for m := range metricCh {
			ch <- m.Desc()
		}
		close(doneCh)
	}()

	exporter.Collect(metricCh)
	close(metricCh)
	<-doneCh
}

// Collect is called by the Prometheus registry when collecting metrics.
// Part of prometheus.Collector interface.
func (exporter *MongodbCollector) Collect(ch chan<- prometheus.Metric) {
	exporter.scrape(ch)

	exporter.scrapesTotal.Collect(ch)
	exporter.scrapeErrorsTotal.Collect(ch)
	exporter.lastScrapeError.Collect(ch)
	exporter.lastScrapeDurationSeconds.Collect(ch)
	exporter.mongoUp.Collect(ch)
}

func (exporter *MongodbCollector) scrape(ch chan<- prometheus.Metric) {
	exporter.scrapesTotal.Inc()
	var err error
	defer func(begun time.Time) {
		exporter.lastScrapeDurationSeconds.Set(time.Since(begun).Seconds())
		if err == nil {
			exporter.lastScrapeError.Set(0)
		} else {
			exporter.scrapeErrorsTotal.Inc()
			exporter.lastScrapeError.Set(1)
		}
	}(time.Now())

	mongoClient, err := exporter.getMongoClient()
	if err != nil || mongoClient == nil {
		err = fmt.Errorf("Can't create mongo client to %s", shared.RedactMongoUri(exporter.Opts.ClientOpts.GetURI()))
		slog.Error(err.Error())
		exporter.mongoUp.Set(0)
		return
	}

	_, err = shared.MongoClientServerVersion(context.Background(), mongoClient)
	if err != nil {
		slog.Error(fmt.Sprintf("Problem gathering the mongo server version: %s", err))
		exporter.mongoUp.Set(0)
		return
	}
	exporter.mongoUp.Set(1)

	var nodeType string
	nodeType, err = shared.MongoClientNodeType(context.Background(), mongoClient)
	if err != nil {
		slog.Error(fmt.Sprintf("Problem gathering the mongo node type: %s", err))
		return
	}

	switch {
	case nodeType == "mongos":
		exporter.collectMongos(context.Background(), mongoClient, ch)
	case nodeType == "mongod":
		exporter.collectMongod(context.Background(), mongoClient, ch)
	case nodeType == "replset":
		exporter.collectMongodReplSet(context.Background(), mongoClient, ch)
	default:
		err = fmt.Errorf("Unrecognized node type %s", nodeType)
		slog.Error(err.Error())
	}
}

func (exporter *MongodbCollector) collectMongos(ctx context.Context, client *mongo.Client, ch chan<- prometheus.Metric) {
	serverStatus := mongos.GetServerStatus(ctx, client)
	if serverStatus != nil {
		serverStatus.Export(ch)
	}

	shardingStatus := mongos.GetShardingStatus(ctx, client)
	if shardingStatus != nil {
		shardingStatus.Export(ch)
	}

	if exporter.Opts.CollectDatabaseMetrics {
		dbStatList := mongos.GetDatabaseStatList(ctx, client)
		if dbStatList != nil {
			dbStatList.Export(ch)
		}
	}

	if exporter.Opts.CollectCollectionMetrics {
		collStatList := mongos.GetCollectionStatList(ctx, client)
		if collStatList != nil {
			collStatList.Export(ch)
		}
	}
}

func (exporter *MongodbCollector) collectMongod(ctx context.Context, client *mongo.Client, ch chan<- prometheus.Metric) {
	serverStatus := mongod.GetServerStatus(ctx, client)
	if serverStatus != nil {
		serverStatus.Export(ch)
	}

	if exporter.Opts.CollectDatabaseMetrics {
		dbStatList := mongod.GetDatabaseStatList(ctx, client)
		if dbStatList != nil {
			dbStatList.Export(ch)
		}
	}

	if exporter.Opts.CollectCollectionMetrics {
		collStatList := mongod.GetCollectionStatList(ctx, client)
		if collStatList != nil {
			collStatList.Export(ch)
		}
	}

	if exporter.Opts.CollectTopMetrics {
		topStatus := mongod.GetTopStatus(ctx, client)
		if topStatus != nil {
			topStatus.Export(ch)
		}
	}

	if exporter.Opts.CollectIndexUsageStats {
		indexStatList := mongod.GetIndexUsageStatList(ctx, client)
		if indexStatList != nil {
			indexStatList.Export(ch)
		}
	}
}

func (exporter *MongodbCollector) collectMongodReplSet(ctx context.Context, client *mongo.Client, ch chan<- prometheus.Metric) {
	exporter.collectMongod(ctx, client, ch)

	replSetStatus := mongod.GetReplSetStatus(ctx, client)
	if replSetStatus != nil {
		replSetStatus.Export(ch)
	}

	oplogStatus := mongod.GetOplogStatus(ctx, client)
	if oplogStatus != nil {
		oplogStatus.Export(ch)
	}
}

// check interface
var _ prometheus.Collector = (*MongodbCollector)(nil)
