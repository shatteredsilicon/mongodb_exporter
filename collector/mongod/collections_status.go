package mongod

import (
	"context"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/log"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
)

var (
	collectionSize = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: Namespace,
		Subsystem: "db_coll",
		Name:      "size",
		Help:      "The total size in memory of all records in a collection",
	}, []string{"db", "coll"})
	collectionObjectCount = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: Namespace,
		Subsystem: "db_coll",
		Name:      "count",
		Help:      "The number of objects or documents in this collection",
	}, []string{"db", "coll"})
	collectionAvgObjSize = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: Namespace,
		Subsystem: "db_coll",
		Name:      "avgobjsize",
		Help:      "The average size of an object in the collection (plus any padding)",
	}, []string{"db", "coll"})
	collectionStorageSize = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: Namespace,
		Subsystem: "db_coll",
		Name:      "storage_size",
		Help:      "The total amount of storage allocated to this collection for document storage",
	}, []string{"db", "coll"})
	collectionIndexes = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: Namespace,
		Subsystem: "db_coll",
		Name:      "indexes",
		Help:      "The number of indexes on the collection",
	}, []string{"db", "coll"})
	collectionIndexesSize = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: Namespace,
		Subsystem: "db_coll",
		Name:      "indexes_size",
		Help:      "The total size of all indexes",
	}, []string{"db", "coll"})
	collectionIndexSize = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: Namespace,
		Subsystem: "db_coll",
		Name:      "index_size",
		Help:      "The individual index size",
	}, []string{"db", "coll", "index"})
)

// CollectionStatList contains stats from all collections
type CollectionStatList struct {
	Members []CollectionStatus
}

// CollectionStatus represents stats about a collection in database (mongod and raw from mongos)
type CollectionStatus struct {
	Database    string
	Name        string
	Size        int                `bson:"size,omitempty"`
	Count       int                `bson:"count,omitempty"`
	AvgObjSize  int                `bson:"avgObjSize,omitempty"`
	StorageSize int                `bson:"storageSize,omitempty"`
	IndexesSize int                `bson:"totalIndexSize,omitempty"`
	IndexSizes  map[string]float64 `bson:"indexSizes,omitempty"`
}

// Export exports database stats to prometheus
func (collStatList *CollectionStatList) Export(ch chan<- prometheus.Metric) {
	for _, member := range collStatList.Members {
		ls := prometheus.Labels{
			"db":   member.Database,
			"coll": member.Name,
		}
		collectionSize.With(ls).Set(float64(member.Size))
		collectionObjectCount.With(ls).Set(float64(member.Count))
		collectionAvgObjSize.With(ls).Set(float64(member.AvgObjSize))
		collectionStorageSize.With(ls).Set(float64(member.StorageSize))
		collectionIndexes.With(ls).Set(float64(len(member.IndexSizes)))
		collectionIndexesSize.With(ls).Set(float64(member.IndexesSize))
		for indexName, size := range member.IndexSizes {
			ls = prometheus.Labels{
				"db":    member.Database,
				"coll":  member.Name,
				"index": indexName,
			}
			collectionIndexSize.With(ls).Set(size)
		}
	}
	collectionSize.Collect(ch)
	collectionObjectCount.Collect(ch)
	collectionAvgObjSize.Collect(ch)
	collectionStorageSize.Collect(ch)
	collectionIndexes.Collect(ch)
	collectionIndexesSize.Collect(ch)
	collectionIndexSize.Collect(ch)
}

// Describe describes database stats for prometheus
func (collStatList *CollectionStatList) Describe(ch chan<- *prometheus.Desc) {
	collectionSize.Describe(ch)
	collectionObjectCount.Describe(ch)
	collectionAvgObjSize.Describe(ch)
	collectionStorageSize.Describe(ch)
	collectionIndexes.Describe(ch)
	collectionIndexesSize.Describe(ch)
}

// GetDatabaseStatus returns stats for a given database
func GetCollectionStatList(ctx context.Context, client *mongo.Client) *CollectionStatList {
	collectionStatList := &CollectionStatList{}
	database_names, err := client.ListDatabaseNames(ctx, bson.D{})
	if err != nil {
		log.Error("Failed to get database names")
		return nil
	}
	for _, db := range database_names {
		collectionSpecs, err := client.Database(db).ListCollectionSpecifications(ctx, bson.D{})
		if err != nil {
			log.Error("Failed to get collection names for db=" + db)
			return nil
		}
		for _, spec := range collectionSpecs {
			if spec.Type == "view" {
				// You can't get collection status from a view
				continue
			}

			collStatus := CollectionStatus{}
			err := client.Database(db).RunCommand(ctx, bson.D{{"collStats", spec.Name}, {"scale", 1}}).Decode(&collStatus)
			collStatus.Database = db
			collStatus.Name = spec.Name
			if err != nil {
				log.Errorf("Failed to get collection status for collection=%s: %s", spec.Name, err)
				return nil
			}
			collectionStatList.Members = append(collectionStatList.Members, collStatus)
		}
	}

	return collectionStatList
}
