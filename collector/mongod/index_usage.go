package mongod

import (
	"context"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/log"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
)

var (
	indexUsage = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: Namespace,
		Name:      "index_usage_count",
		Help:      "Contains a usage count of each index",
	}, []string{"collection", "index"})
)

// IndexStatsList represents index usage information
type IndexStatsList struct {
	Items []IndexUsageStats
}

// IndexUsageStats represents stats about an Index
type IndexUsageStats struct {
	Name       string         `bson:"name"`
	Accesses   IndexUsageInfo `bson:"accesses"`
	Collection string
}

// IndexUsageInfo represents a single index stats of an Index
type IndexUsageInfo struct {
	Ops float64 `bson:"ops"`
}

// Export exports database stats to prometheus
func (indexStats *IndexStatsList) Export(ch chan<- prometheus.Metric) {
	indexUsage.Reset()
	for _, indexStat := range indexStats.Items {
		indexUsage.WithLabelValues(indexStat.Collection, indexStat.Name).Add(indexStat.Accesses.Ops)
	}
	indexUsage.Collect(ch)
}

// Describe describes database stats for prometheus
func (indexStats *IndexStatsList) Describe(ch chan<- *prometheus.Desc) {
	indexUsage.Describe(ch)
}

// GetIndexUsageStatList returns stats for a given collection in a database
func GetIndexUsageStatList(ctx context.Context, client *mongo.Client) *IndexStatsList {
	indexUsageStatsList := &IndexStatsList{}
	log.Debug("collecting index stats")
	databaseNames, err := client.ListDatabaseNames(ctx, bson.D{})
	if err != nil {
		log.Error("Failed to get database names")
		return nil
	}
	for _, db := range databaseNames {
		collectionNames, err := client.Database(db).ListCollectionNames(ctx, bson.D{})
		if err != nil {
			log.Error("Failed to get collection names for db=" + db)
			return nil
		}
		for _, collectionName := range collectionNames {

			collIndexUsageStats := IndexStatsList{}
			if cur, err := client.Database(db).Collection(collectionName).Aggregate(ctx, mongo.Pipeline{bson.D{{"$indexStats", bson.M{}}}}); err != nil {
				log.Error("Failed to collect index stats for coll=" + collectionName)
				return nil
			} else if cur.All(ctx, &collIndexUsageStats.Items); err != nil {
				log.Error("Failed to collect index stats for coll=" + collectionName)
				return nil
			}
			// Label index stats with corresponding db.collection
			for _, stat := range collIndexUsageStats.Items {
				stat.Collection = db + "." + collectionName
			}
			indexUsageStatsList.Items = append(indexUsageStatsList.Items, collIndexUsageStats.Items...)
		}
	}

	return indexUsageStatsList
}
