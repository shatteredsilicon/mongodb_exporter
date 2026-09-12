package mongod

import (
	"context"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/log"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
)

// TopStatus represents top metrics
type TopStatus struct {
	TopStats TopStatsMap `bson:"totals,omitempty"`
}

// GetTopStats fetches top stats
func GetTopStats(ctx context.Context, client *mongo.Client) (*TopStatus, error) {
	results := &TopStatus{}
	err := client.Database("admin").RunCommand(ctx, bson.D{{"top", 1}}).Decode(&results)
	return results, err
}

// Export exports metrics to Prometheus
func (status *TopStatus) Export(ch chan<- prometheus.Metric) {
	status.TopStats.Export(ch)
}

// GetTopStatus fetches top stats
func GetTopStatus(ctx context.Context, client *mongo.Client) *TopStatus {
	topStatus, err := GetTopStats(ctx, client)
	if err != nil {
		log.Debugf("Failed to get top status: %s", err)
		return nil
	}

	return topStatus
}
