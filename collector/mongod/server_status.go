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

package mongod

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
)

var (
	versionInfo = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: Namespace,
		Subsystem: "version",
		Name:      "info",
		Help:      "Software version information for mongodb process.",
	}, []string{"mongodb"})
	instanceUptimeSecondsDesc = prometheus.NewDesc(
		prometheus.BuildFQName(Namespace, "instance", "uptime_seconds"),
		"The value of the uptime field corresponds to the number of seconds that the mongos or mongod process has been active.",
		nil,
		nil,
	)
	instanceUptimeEstimateSecondsDesc = prometheus.NewDesc(
		prometheus.BuildFQName(Namespace, "instance", "uptime_estimate_seconds"),
		"uptimeEstimate provides the uptime as calculated from MongoDB's internal course-grained time keeping system.",
		nil,
		nil,
	)
	instanceLocalTimeDesc = prometheus.NewDesc(
		prometheus.BuildFQName(Namespace, "instance", "local_time"),
		"The localTime value is the current time, according to the server, in UTC specified in an ISODate format.",
		nil,
		nil,
	)
)

// ServerStatus keeps the data returned by the serverStatus() method.
type ServerStatus struct {
	Version        string    `bson:"version"`
	Uptime         float64   `bson:"uptime"`
	UptimeEstimate float64   `bson:"uptimeEstimate"`
	LocalTime      time.Time `bson:"localTime"`

	Asserts *AssertsStats `bson:"asserts"`

	Dur *DurStats `bson:"dur"`

	BackgroundFlushing *FlushStats `bson:"backgroundFlushing"`

	Connections *ConnectionStats `bson:"connections"`

	ExtraInfo *ExtraInfo `bson:"extra_info"`

	GlobalLock *GlobalLockStats `bson:"globalLock"`

	IndexCounter *IndexCounterStats `bson:"indexCounters"`

	Locks LockStatsMap `bson:"locks,omitempty"`

	Network *NetworkStats `bson:"network"`

	Opcounters     *OpcountersStats     `bson:"opcounters"`
	OpcountersRepl *OpcountersReplStats `bson:"opcountersRepl"`
	Mem            *MemStats            `bson:"mem"`
	Metrics        *MetricsStats        `bson:"metrics"`

	Cursors *Cursors `bson:"cursors"`

	StorageEngine *StorageEngineStats `bson:"storageEngine"`
	InMemory      *WiredTigerStats    `bson:"inMemory"`
	RocksDb       *RocksDbStats       `bson:"rocksdb"`
	WiredTiger    *WiredTigerStats    `bson:"wiredTiger"`
}

// Export exports the server status to be consumed by prometheus.
func (status *ServerStatus) Export(ch chan<- prometheus.Metric) {
	versionInfo.WithLabelValues(status.Version).Set(1)
	ch <- prometheus.MustNewConstMetric(instanceUptimeSecondsDesc, prometheus.CounterValue, status.Uptime)
	ch <- prometheus.MustNewConstMetric(instanceUptimeEstimateSecondsDesc, prometheus.CounterValue, status.Uptime)
	ch <- prometheus.MustNewConstMetric(instanceLocalTimeDesc, prometheus.CounterValue, float64(status.LocalTime.Unix()))
	versionInfo.Collect(ch)

	if status.Asserts != nil {
		status.Asserts.Export(ch)
	}
	if status.Dur != nil {
		status.Dur.Export(ch)
	}
	if status.BackgroundFlushing != nil {
		status.BackgroundFlushing.Export(ch)
	}
	if status.Connections != nil {
		status.Connections.Export(ch)
	}
	if status.ExtraInfo != nil {
		status.ExtraInfo.Export(ch)
	}
	if status.GlobalLock != nil {
		status.GlobalLock.Export(ch)
	}
	if status.IndexCounter != nil {
		status.IndexCounter.Export(ch)
	}
	if status.Network != nil {
		status.Network.Export(ch)
	}
	if status.Opcounters != nil {
		status.Opcounters.Export(ch)
	}
	if status.OpcountersRepl != nil {
		status.OpcountersRepl.Export(ch)
	}
	if status.Mem != nil {
		status.Mem.Export(ch)
	}
	if status.Locks != nil {
		status.Locks.Export(ch)
	}
	if status.Metrics != nil {
		status.Metrics.Export(ch)
	}
	if status.Cursors != nil {
		status.Cursors.Export(ch)
	}
	if status.InMemory != nil {
		status.InMemory.Export(ch)
	}
	if status.RocksDb != nil {
		status.RocksDb.Export(ch)
	}
	if status.WiredTiger != nil {
		status.WiredTiger.Export(ch)
	}

	// If db.serverStatus().storageEngine does not exist (3.0+ only) and status.BackgroundFlushing does (MMAPv1 only), default to mmapv1
	// https://docs.mongodb.com/v3.0/reference/command/serverStatus/#storageengine
	if status.StorageEngine == nil && status.BackgroundFlushing != nil {
		status.StorageEngine = &StorageEngineStats{
			Name: "mmapv1",
		}
	}
	if status.StorageEngine != nil {
		status.StorageEngine.Export(ch)
	}
}

// Describe describes the server status for prometheus.
func (status *ServerStatus) Describe(ch chan<- *prometheus.Desc) {
	versionInfo.Describe(ch)
	ch <- instanceUptimeSecondsDesc
	ch <- instanceUptimeEstimateSecondsDesc
	ch <- instanceLocalTimeDesc

	if status.Asserts != nil {
		status.Asserts.Describe(ch)
	}
	if status.Dur != nil {
		status.Dur.Describe(ch)
	}
	if status.BackgroundFlushing != nil {
		status.BackgroundFlushing.Describe(ch)
	}
	if status.Connections != nil {
		status.Connections.Describe(ch)
	}
	if status.ExtraInfo != nil {
		status.ExtraInfo.Describe(ch)
	}
	if status.GlobalLock != nil {
		status.GlobalLock.Describe(ch)
	}
	if status.IndexCounter != nil {
		status.IndexCounter.Describe(ch)
	}
	if status.Network != nil {
		status.Network.Describe(ch)
	}
	if status.Opcounters != nil {
		status.Opcounters.Describe(ch)
	}
	if status.OpcountersRepl != nil {
		status.OpcountersRepl.Describe(ch)
	}
	if status.Mem != nil {
		status.Mem.Describe(ch)
	}
	if status.Locks != nil {
		status.Locks.Describe(ch)
	}
	if status.Metrics != nil {
		status.Metrics.Describe(ch)
	}
	if status.Cursors != nil {
		status.Cursors.Describe(ch)
	}
	if status.StorageEngine != nil {
		status.StorageEngine.Describe(ch)
	}
	if status.InMemory != nil {
		status.InMemory.Describe(ch)
	}
	if status.RocksDb != nil {
		status.RocksDb.Describe(ch)
	}
	if status.WiredTiger != nil {
		status.WiredTiger.Describe(ch)
	}
}

// GetServerStatus returns the server status info.
func GetServerStatus(ctx context.Context, client *mongo.Client) *ServerStatus {
	result := &ServerStatus{}
	err := client.Database("admin").RunCommand(ctx, bson.D{{"serverStatus", 1}, {"recordStats", 0}}).Decode(result)
	if err != nil {
		slog.Error(fmt.Sprintf("Failed to get server status: %s", err))
		return nil
	}

	return result
}
