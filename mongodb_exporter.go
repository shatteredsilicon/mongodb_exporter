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

package main

import (
	"context"
	"crypto/tls"
	"flag"
	"fmt"
	"net/url"
	"os"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/prometheus/common/log"
	"github.com/prometheus/common/version"
	"github.com/shatteredsilicon/exporter_shared"
	"go.mongodb.org/mongo-driver/mongo/options"
	"gopkg.in/ini.v1"

	"github.com/shatteredsilicon/mongodb_exporter/collector"
	"github.com/shatteredsilicon/mongodb_exporter/shared"
)

const (
	program = "mongodb_exporter"
)

func defaultMongoDBURL() string {
	if u := os.Getenv("MONGODB_URL"); u != "" {
		return u
	}
	return "mongodb://localhost:27017"
}

var (
	versionF       = flag.Bool("version", false, "Print version information and exit.")
	configPathF    = flag.String("config", "/opt/ss/ssm-client/mongodb_exporter.conf", "Path of config file")
	listenAddressF = flag.String("web.listen-address", ":9216", "Address to listen on for web interface and telemetry.")
	metricsPathF   = flag.String("web.metrics-path", "/metrics", "Path under which to expose metrics.")

	collectDatabaseF   = flag.Bool("collect.database", false, "Enable collection of Database metrics")
	collectCollectionF = flag.Bool("collect.collection", false, "Enable collection of Collection metrics")
	collectTopF        = flag.Bool("collect.topmetrics", false, "Enable collection of table top metrics")
	collectIndexUsageF = flag.Bool("collect.indexusage", false, "Enable collection of per index usage stats")

	uriF     = flag.String("mongodb.uri", defaultMongoDBURL(), "MongoDB URI, format: [mongodb://][user:pass@]host1[:port1][,host2[:port2],...][/database][?options]")
	tlsF     = flag.Bool("mongodb.tls", false, "Enable tls connection with mongo server")
	tlsCertF = flag.String("mongodb.tls-cert", "", "Path to PEM file that contains the certificate (and optionally also the decrypted private key in PEM format).\n"+
		"    \tThis should include the whole certificate chain.\n"+
		"    \tIf provided: The connection will be opened via TLS to the MongoDB server.")
	tlsPrivateKeyF = flag.String("mongodb.tls-private-key", "", "Path to PEM file that contains the decrypted private key (if not contained in mongodb.tls-cert file).")
	tlsCAF         = flag.String("mongodb.tls-ca", "", "Path to PEM file that contains the CAs that are trusted for server connections.\n"+
		"    \tIf provided: MongoDB servers connecting to should present a certificate signed by one of this CAs.\n"+
		"    \tIf not provided: System default CAs are used.")
	tlsDisableHostnameValidationF = flag.Bool("mongodb.tls-disable-hostname-validation", false, "Disable hostname validation for server connection.")
	maxConnectionsF               = flag.Int("mongodb.max-connections", 1, "Max number of pooled connections to the database.")
	testF                         = flag.Bool("test", false, "Check MongoDB connection, print buildInfo() information and exit.")

	socketTimeoutF = flag.String("mongodb.socket-timeout", "3s", "Amount of time to wait for a non-responding socket to the database before it is forcefully closed.\n"+
		"    \tValid time units are 'ns', 'us' (or 'µs'), 'ms', 's', 'm', 'h'.")
	syncTimeoutF = flag.String("mongodb.sync-timeout", "1m", "Amount of time an operation with this session will wait before returning an error in case\n"+
		"    \ta connection to a usable server can't be established.\n"+
		"    \tValid time units are 'ns', 'us' (or 'µs'), 'ms', 's', 'm', 'h'.")

	// FIXME currently ignored
	// enabledGroupsFlag = flag.String("groups.enabled", "asserts,durability,background_flushing,connections,extra_info,global_lock,index_counters,network,op_counters,op_counters_repl,memory,locks,metrics", "Comma-separated list of groups to use, for more info see: docs.mongodb.org/manual/reference/command/serverStatus/")
	enabledGroupsFlag = flag.String("groups.enabled", "", "Currently ignored")
)

var cfg = new(config)

func main() {
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "%s %s exports various MongoDB metrics in Prometheus format.\n", os.Args[0], version.Version)
		fmt.Fprintf(os.Stderr, "Usage: %s [flags]\n\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "Flags:\n")
		flag.PrintDefaults()
	}
	flag.Parse()

	if os.Getenv("DEBUG") == "1" {
		log.Base().SetLevel("debug")
	}

	if os.Getenv("ON_CONFIGURE") == "1" {
		err := configure()
		if err != nil {
			os.Exit(1)
		}
		os.Exit(0)
	}

	uri := os.Getenv("MONGODB_URI")
	if uri != "" {
		uriF = &uri
	}

	err := ini.MapTo(cfg, *configPathF)
	if err != nil {
		log.Fatal(fmt.Sprintf("Load config file %s failed: %s", *configPathF, err.Error()))
	}

	// set flags for exporter_shared server
	flag.Set("web.ssl-cert-file", lookupConfig("web.ssl-cert-file", "").(string))
	flag.Set("web.ssl-key-file", lookupConfig("web.ssl-key-file", "").(string))
	flag.Set("web.auth-file", lookupConfig("web.auth-file", "/opt/ss/ssm-client/ssm.yml").(string))

	if uri == "" {
		uri = lookupConfig("mongodb.uri", *uriF).(string)
	}

	tlsEnabled := lookupConfig("mongodb.tls", *tlsF).(bool)
	tlsCert := lookupConfig("mongodb.tls-cert", *tlsCertF).(string)
	tlsPrivateKey := lookupConfig("mongodb.tls-private-key", *tlsPrivateKeyF).(string)
	tlsCA := lookupConfig("mongodb.tls-ca", *tlsCAF).(string)
	tlsDisableHostnameValidation := lookupConfig("mongodb.disable-hostname-validation", *tlsDisableHostnameValidationF).(bool)

	// uri must has scheme
	u, err := url.Parse(uri)
	if err != nil || u == nil || u.Scheme == "" {
		// assume it's invalid because it doesn't have schema,
		// add default schema 'mongodb://' and try it again
		tmpURI := "mongodb://" + uri
		u, err = url.Parse(tmpURI)
		if err == nil && u != nil && u.Scheme != "" {
			uri = tmpURI
		}
	}

	if lookupConfig("test", *testF).(bool) {
		serverAPI := options.ServerAPI(options.ServerAPIVersion1)
		clientOpts := options.Client().ApplyURI(uri)

		if tlsEnabled {
			tlsConfig := tls.Config{
				InsecureSkipVerify: tlsDisableHostnameValidation,
			}
			if len(tlsCA) > 0 {
				ca, err := shared.LoadCaFrom(tlsCA)
				if err != nil {
					log.Fatalf("Couldn't load client CAs from %s. Got: %s", tlsCA, err)
				}
				tlsConfig.RootCAs = ca
			}
			if len(tlsCert) > 0 {
				certificates, err := shared.LoadKeyPairFrom(tlsCert, tlsPrivateKey)
				if err != nil {
					log.Fatalf("Cannot load key pair from '%s' and '%s' to connect to server '%s'. Got: %v", tlsCert, tlsPrivateKey, uri, err)
				}
				tlsConfig.Certificates = []tls.Certificate{certificates}
			}

			clientOpts.SetTLSConfig(&tlsConfig)
		}
		clientOpts.SetServerAPIOptions(serverAPI)

		buildInfo, err := shared.TestConnection(
			context.Background(),
			clientOpts,
		)
		if err != nil {
			log.Errorf("Can't connect to MongoDB: %s", err)
			os.Exit(1)
		}
		fmt.Println(string(buildInfo))
		os.Exit(0)
	}
	if *versionF {
		fmt.Println(version.Print(program))
		os.Exit(0)
	}

	socketTimeout, _ := time.ParseDuration(lookupConfig("mongodb.socket-timeout", *socketTimeoutF).(string))
	syncTimeout, _ := time.ParseDuration(lookupConfig("mongodb.sync-timeout", *syncTimeoutF).(string))
	mongodbCollector := collector.NewMongodbCollector(&collector.MongodbCollectorOpts{
		URI:                      uri,
		TLSConnection:            tlsEnabled,
		TLSCertificateFile:       tlsCert,
		TLSPrivateKeyFile:        tlsPrivateKey,
		TLSCaFile:                tlsCA,
		TLSHostnameValidation:    !tlsDisableHostnameValidation,
		DBPoolLimit:              lookupConfig("mongodb.max-connections", *maxConnectionsF).(int),
		CollectDatabaseMetrics:   lookupConfig("collect.database", *collectDatabaseF).(bool),
		CollectCollectionMetrics: lookupConfig("collect.collection", *collectCollectionF).(bool),
		CollectTopMetrics:        lookupConfig("collect.topmetrics", *collectTopF).(bool),
		CollectIndexUsageStats:   lookupConfig("collect.indexusage", *collectIndexUsageF).(bool),
		SocketTimeout:            socketTimeout,
		SyncTimeout:              syncTimeout,
	})
	defer mongodbCollector.Close()
	prometheus.MustRegister(mongodbCollector)

	exporter_shared.RunServer("MongoDB", lookupConfig("web.listen-address", *listenAddressF).(string), lookupConfig("web.metrics-path", *metricsPathF).(string), promhttp.ContinueOnError)
}

type config struct {
	Test    bool          `ini:"test"`
	Web     webConfig     `ini:"web"`
	Collect collectConfig `ini:"collect"`
	Mongodb mongodbConfig `ini:"mongodb"`
	Groups  groupsConfig  `ini:"groups"`
}

type webConfig struct {
	ListenAddress string  `ini:"listen-address"`
	MetricsPath   string  `ini:"metrics-path"`
	SSLCertFile   string  `ini:"ssl-cert-file"`
	SSLKeyFile    string  `ini:"ssl-key-file"`
	AuthFile      *string `ini:"auth-file"`
}

type collectConfig struct {
	Database   bool `ini:"database"`
	Collection bool `ini:"collection"`
	TopMetrics bool `ini:"topmetrics"`
	IndexUsage bool `ini:"indexusage"`
}

type groupsConfig struct {
	Enabled string `ini:"enabled"`
}

type mongodbConfig struct {
	URL                       string `ini:"uri"`
	TLS                       bool   `ini:"tls"`
	TLSCert                   string `ini:"tls-cert"`
	TLSPrivateKey             string `ini:"tls-private-key"`
	TLSCA                     string `ini:"tls-ca"`
	DisableHostnameValidation bool   `ini:"disable-hostname-validation"`
	MaxConnections            int    `ini:"max-connections"`
	Test                      bool   `ini:"test"`
	SocketTimeout             string `ini:"socket-timeout"`
	SyncTimeout               string `ini:"sync-timeout"`
}

// lookupConfig lookup config from flag
// or config by name, returns nil if none exists.
// name should be in this format -> '[section].[key]'
func lookupConfig(name string, defaultValue interface{}) interface{} {
	flagSet, flagValue := lookupFlag(name)
	if flagSet {
		return flagValue
	}

	section := ""
	key := name
	if i := strings.Index(name, "."); i > 0 {
		section = name[0:i]
		if len(name) > i+1 {
			key = name[i+1:]
		} else {
			key = ""
		}
	}

	t := reflect.TypeOf(*cfg)
	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)
		iniName := field.Tag.Get("ini")
		matched := iniName == section
		if section == "" {
			matched = iniName == key
		}
		if !matched {
			continue
		}

		v := reflect.ValueOf(cfg).Elem().Field(i)
		if section == "" {
			return v.Interface()
		}

		if !v.CanAddr() {
			continue
		}

		st := reflect.TypeOf(v.Interface())
		for j := 0; j < st.NumField(); j++ {
			sectionField := st.Field(j)
			sectionININame := sectionField.Tag.Get("ini")
			if sectionININame != key {
				continue
			}

			if reflect.ValueOf(v.Addr().Elem().Field(j).Interface()).Kind() != reflect.Ptr {
				return v.Addr().Elem().Field(j).Interface()
			}

			if v.Addr().Elem().Field(j).IsNil() {
				return defaultValue
			}

			return v.Addr().Elem().Field(j).Elem().Interface()
		}
	}

	return defaultValue
}

func lookupFlag(name string) (flagSet bool, flagValue interface{}) {
	flag.Visit(func(f *flag.Flag) {
		if f.Name == name {
			flagSet = true
			switch reflect.Indirect(reflect.ValueOf(f.Value)).Kind() {
			case reflect.Bool:
				flagValue = reflect.Indirect(reflect.ValueOf(f.Value)).Bool()
			case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
				flagValue = reflect.Indirect(reflect.ValueOf(f.Value)).Int()
			case reflect.Float32, reflect.Float64:
				flagValue = reflect.Indirect(reflect.ValueOf(f.Value)).Float()
			case reflect.String:
				flagValue = reflect.Indirect(reflect.ValueOf(f.Value)).String()
			case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
				flagValue = reflect.Indirect(reflect.ValueOf(f.Value)).Uint()
			}
		}
	})

	return
}

func configure() error {
	iniCfg, err := ini.Load(*configPathF)
	if err != nil {
		return err
	}

	if err = iniCfg.MapTo(cfg); err != nil {
		return err
	}

	type item struct {
		value   reflect.Value
		section string
	}

	items := []item{
		{
			value:   reflect.ValueOf(cfg).Elem(),
			section: "",
		},
	}
	for i := 0; i < len(items); i++ {
		for j := 0; j < items[i].value.Type().NumField(); j++ {
			fieldValue := items[i].value.Field(j)
			fieldType := items[i].value.Type().Field(j)
			section := items[i].section
			key := fieldType.Tag.Get("ini")

			if fieldValue.Kind() == reflect.Struct {
				if fieldValue.CanAddr() && section == "" {
					items = append(items, item{
						value:   fieldValue.Addr().Elem(),
						section: key,
					})
				}
				continue
			}

			flagSet, flagValue := lookupFlag(fmt.Sprintf("%s.%s", section, key))
			if !flagSet {
				continue
			}

			if fieldValue.IsValid() && fieldValue.CanSet() {
				switch fieldValue.Kind() {
				case reflect.Bool:
					iniCfg.Section(section).Key(key).SetValue(fmt.Sprintf("%t", flagValue.(bool)))
				case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
					iniCfg.Section(section).Key(key).SetValue(fmt.Sprintf("%d", flagValue.(int64)))
				case reflect.Float32, reflect.Float64:
					iniCfg.Section(section).Key(key).SetValue(fmt.Sprintf("%f", flagValue.(float64)))
				case reflect.String:
					iniCfg.Section(section).Key(key).SetValue(strconv.Quote(flagValue.(string)))
				case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
					iniCfg.Section(section).Key(key).SetValue(fmt.Sprintf("%d", flagValue.(uint64)))
				}
			}
		}
	}

	if os.Getenv("MONGODB_URI") != "" {
		iniCfg.Section("mongodb").Key("uri").SetValue(strconv.Quote(os.Getenv("MONGODB_URI")))
	}

	if err = iniCfg.SaveTo(*configPathF); err != nil {
		return err
	}

	return nil
}
