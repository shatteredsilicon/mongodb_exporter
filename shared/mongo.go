package shared

import (
	"context"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
)

// The BuildInfo type encapsulates details about the running MongoDB server.
//
// Note that the VersionArray field was introduced in MongoDB 2.0+, but it is
// internally assembled from the Version information for previous versions.
// In both cases, VersionArray is guaranteed to have at least 4 entries.
type BuildInfo struct {
	Version        string
	VersionArray   []int  `bson:"versionArray"`
	GitVersion     string `bson:"gitVersion"`
	OpenSSLVersion string `bson:"OpenSSLVersion"`
	SysInfo        string `bson:"sysInfo"`
	Bits           int
	Debug          bool
	MaxObjectSize  int `bson:"maxBsonObjectSize"`
}

// getBuildInfo retrieves the version and other details about the
// running MongoDB server.
func getBuildInfo(ctx context.Context, client *mongo.Client) (info BuildInfo, err error) {
	err = client.Database("admin").RunCommand(ctx, bson.D{{"buildInfo", 1}}).Decode(&info)
	return
}
