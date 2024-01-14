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

package shared

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/prometheus/common/log"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

func RedactMongoUri(uri string) string {
	if strings.HasPrefix(uri, "mongodb://") && strings.Contains(uri, "@") {
		opts := options.Client().ApplyURI(uri)
		if err := opts.Validate(); err != nil {
			log.Errorf("Cannot parse mongodb server url: %s", err)
			return "unknown/error"
		}
		if opts.Auth.Username != "" && opts.Auth.Password != "" {
			return "mongodb://****:****@" + strings.Join(opts.Hosts, ",")
		}
	}
	return uri
}

type MongoSessionOpts struct {
	URI                   string
	TLSConnection         bool
	TLSCertificateFile    string
	TLSPrivateKeyFile     string
	TLSCaFile             string
	TLSHostnameValidation bool
	PoolLimit             int
	SocketTimeout         time.Duration
	SyncTimeout           time.Duration
}

// MongoClient connects to MongoDB and returns ready to use MongoDB client.
func MongoClient(ctx context.Context, opts *options.ClientOptions) (*mongo.Client, error) {
	client, err := mongo.Connect(ctx, opts)
	if err != nil {
		return nil, err
	}

	return client, nil
}

func MongoClientServerVersion(ctx context.Context, client *mongo.Client) (string, error) {
	buildInfo, err := getBuildInfo(ctx, client)
	if err != nil {
		log.Errorf("Could not get MongoDB BuildInfo: %s!", err)
		return "unknown", err
	}
	return buildInfo.Version, nil
}

func MongoClientNodeType(ctx context.Context, client *mongo.Client) (string, error) {
	masterDoc := struct {
		SetName interface{} `bson:"setName"`
		Hosts   interface{} `bson:"hosts"`
		Msg     string      `bson:"msg"`
	}{}
	err := client.Database("admin").RunCommand(ctx, bson.D{{"isMaster", 1}}).Decode(&masterDoc)
	if err != nil {
		log.Errorf("Got unknown node type: %s", err)
		return "unknown", err
	}

	if masterDoc.SetName != nil || masterDoc.Hosts != nil {
		return "replset", nil
	} else if masterDoc.Msg == "isdbgrid" {
		// isdbgrid is always the msg value when calling isMaster on a mongos
		// see http://docs.mongodb.org/manual/core/sharded-cluster-query-router/
		return "mongos", nil
	}
	return "mongod", nil
}

// TestConnection connects to MongoDB and returns BuildInfo.
func TestConnection(ctx context.Context, opts *options.ClientOptions) ([]byte, error) {
	client, err := MongoClient(ctx, opts)
	if err != nil || client == nil {
		return nil, fmt.Errorf("cannot connect using uri '%s': %s", opts.GetURI(), err.Error())
	}

	buildInfo, err := getBuildInfo(ctx, client)
	if err != nil {
		return nil, fmt.Errorf("cannot get buildInfo() for MongoDB using uri '%s': %s", opts.GetURI(), err.Error())
	}

	b, err := json.MarshalIndent(buildInfo, "", "  ")
	if err != nil {
		return nil, fmt.Errorf("cannot create json: %s", err)
	}

	return b, nil
}
