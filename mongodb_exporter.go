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
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/alecthomas/kingpin/v2"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/prometheus/common/promslog"
	"github.com/prometheus/common/promslog/flag"
	"github.com/prometheus/common/version"
	"github.com/prometheus/exporter-toolkit/web"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/x/mongo/driver/connstring"
	"go.yaml.in/yaml/v2"
	"golang.org/x/crypto/bcrypt"
	"gopkg.in/ini.v1"

	"github.com/shatteredsilicon/mongodb_exporter/collector"
	"github.com/shatteredsilicon/mongodb_exporter/shared"
)

const (
	program             = "mongodb_exporter"
	webAuthFileFlagName = "web.auth-file"
)

func defaultMongoDBURL() string {
	if u := os.Getenv("MONGODB_URL"); u != "" {
		return u
	}
	return "mongodb://localhost:27017"
}

var (
	configPathF = kingpin.Flag("config", "Path of config file").Default("/opt/ss/ssm-client/mongodb_exporter.conf").String()

	// Web Flags
	metricsPathF    = kingpin.Flag("web.metrics-path", "Path under which to expose metrics.").Default("/metrics").String()
	webAuthFile     = kingpin.Flag("web.auth-file", "Path to YAML file with server_user, server_password keys for HTTP Basic authentication.").String()
	webConfigFile   = kingpin.Flag("web.config.file", "Path to prometheus web config file (YAML).").Default("/opt/ss/ssm-client/mongodb_exporter.yml").String()
	tlsMinVersion   = kingpin.Flag("web.tls-min-version", "Minimum TLS version that is acceptable.").String()
	tlsMaxVersion   = kingpin.Flag("web.tls-max-version", "Maximum TLS version that is acceptable.").String()
	tlsCipherSuites = kingpin.Flag(
		"web.tls-cipher-suites",
		"A list of enabled TLS 1.0–1.2 cipher suites. Check full list at https://github.com/golang/go/blob/master/src/crypto/tls/cipher_suites.go",
	).Strings()
	sslCertFile = kingpin.Flag(
		"web.ssl-cert-file",
		"Path to SSL certificate file.",
	).String()
	sslKeyFile = kingpin.Flag(
		"web.ssl-key-file",
		"Path to SSL key file.",
	).String()
	listenAddress = kingpin.Flag(
		"web.listen-address",
		"Address on which to expose metrics and web interface.",
	).Strings()
	systemdSocket = kingpin.Flag(
		"web.systemd-socket",
		"Use systemd socket activation listeners instead of port listeners (Linux only).",
	).Bool()

	// Collector Flags
	collectDatabaseF   = kingpin.Flag("collect.database", "Enable collection of Database metrics").Bool()
	collectCollectionF = kingpin.Flag("collect.collection", "Enable collection of Collection metrics").Bool()
	collectTopF        = kingpin.Flag("collect.topmetrics", "Enable collection of table top metrics").Bool()
	collectIndexUsageF = kingpin.Flag("collect.indexusage", "Enable collection of per index usage stats").Bool()

	// MongoDB Connection Flags
	uriF           = kingpin.Flag("mongodb.uri", "MongoDB URI format.").Default(defaultMongoDBURL()).String()
	tlsF           = kingpin.Flag("mongodb.tls", "Enable tls connection with mongo server").Bool()
	tlsCertF       = kingpin.Flag("mongodb.tls-cert", "Path to PEM file that contains the certificate.").String()
	tlsPrivateKeyF = kingpin.Flag("mongodb.tls-private-key", "Path to PEM file that contains the decrypted private key.").String()
	tlsCAF         = kingpin.Flag("mongodb.tls-ca", "Path to PEM file that contains the trusted CAs.").String()

	tlsDisableHostnameValidationF = kingpin.Flag("mongodb.tls-disable-hostname-validation", "Disable hostname validation.").Bool()
	maxConnectionsF               = kingpin.Flag("mongodb.max-connections", "Max number of pooled connections.").Default("1").Int()

	socketTimeoutF = kingpin.Flag("mongodb.socket-timeout", "Socket timeout duration.").Default("3s").String()
	syncTimeoutF   = kingpin.Flag("mongodb.sync-timeout", "Sync timeout duration.").Default("1m").String()

	testF = kingpin.Flag("test", "Check MongoDB connection and exit.").Bool()
	// FIXME currently ignored
	// enabledGroupsFlag = flag.String("groups.enabled", "asserts,durability,background_flushing,connections,extra_info,global_lock,index_counters,network,op_counters,op_counters_repl,memory,locks,metrics", "Comma-separated list of groups to use, for more info see: docs.mongodb.org/manual/reference/command/serverStatus/")
	enabledGroupsFlag = kingpin.Flag("groups.enabled", "Currently ignored").String()

	_ = kingpin.Flag("c", "").Hidden().Short('c').Action(convertFlagAction('c')).Strings()
	_ = kingpin.Flag("w", "").Hidden().Short('w').Action(convertFlagAction('w')).Strings()
	_ = kingpin.Flag("e", "").Hidden().Short('e').Action(convertFlagAction('e')).Strings()
	_ = kingpin.Flag("t", "").Hidden().Short('t').Action(convertFlagAction('t')).Strings()
)

var cfg = new(config)
var setByUserMap = make(map[string]bool)

func init() {
	kingpin.CommandLine.PreAction(setByUserFlagAction())
}

func setByUserFlagAction() func(ctx *kingpin.ParseContext) error {
	executed := false

	return func(pc *kingpin.ParseContext) error {
		if executed {
			return nil
		}

		for _, elem := range pc.Elements {
			if elem.Clause == nil {
				continue
			}

			flagClause, ok := elem.Clause.(*kingpin.FlagClause)
			if !ok || flagClause == nil {
				continue
			}

			setByUserMap[flagClause.Model().Name] = true
		}

		executed = true
		return nil
	}
}

func main() {
	kingpin.Version(version.Print(program))
	kingpin.HelpFlag.Short('h')
	kingpin.Parse()

	promslogConfig := &promslog.Config{}
	flag.AddFlags(kingpin.CommandLine, promslogConfig)
	if os.Getenv("DEBUG") == "1" {
		promslogConfig.Level.Set("debug")
	}
	logger := promslog.New(promslogConfig)
	slog.SetDefault(logger)

	if os.Getenv("ON_CONFIGURE") == "1" {
		err := configure()
		if err != nil {
			os.Exit(1)
		}
		os.Exit(0)
	}

	err := ini.MapTo(cfg, *configPathF)
	if err != nil {
		logger.Error(fmt.Sprintf("Load config file %s failed: %s", *configPathF, err.Error()))
		os.Exit(1)
	}

	// override flag value with config value
	// if it's not set
	overrideFlags()

	uri := os.Getenv("MONGODB_URI")
	if uri != "" {
		uriF = &uri
	} else {
		uri = *uriF
	}

	// tlsEnabled := lookupConfig("mongodb.tls", *tlsF).(bool)
	// tlsCert := lookupConfig("mongodb.tls-cert", *tlsCertF).(string)
	// tlsPrivateKey := lookupConfig("mongodb.tls-private-key", *tlsPrivateKeyF).(string)
	// tlsCA := lookupConfig("mongodb.tls-ca", *tlsCAF).(string)
	// tlsDisableHostnameValidation := lookupConfig("mongodb.disable-hostname-validation", *tlsDisableHostnameValidationF).(bool)

	// uri must has scheme
	if _, err := connstring.ParseAndValidate(uri); err != nil {
		// assume it's invalid because it doesn't have schema,
		// add default schema 'mongodb://' and try it again
		tmpURI := "mongodb://" + *uriF
		_, err = connstring.ParseAndValidate(tmpURI)
		if err == nil {
			uri = tmpURI
		}
	}

	serverAPI := options.ServerAPI(options.ServerAPIVersion1)
	clientOpts := options.Client().ApplyURI(uri).SetServerAPIOptions(serverAPI)
	if clientOpts.Direct == nil {
		// default to directConnection=true if it's not set
		clientOpts.SetDirect(true)
	}

	if *testF {
		if *tlsF {
			tlsConfig := tls.Config{
				InsecureSkipVerify: *tlsDisableHostnameValidationF,
			}
			if len(*tlsCAF) > 0 {
				ca, err := shared.LoadCaFrom(*tlsCAF)
				if err != nil {
					logger.Error(fmt.Sprintf("Couldn't load client CAs from %s. Got: %s", *tlsCAF, err))
					os.Exit(1)
				}
				tlsConfig.RootCAs = ca
			}
			if len(*tlsCertF) > 0 {
				certificates, err := shared.LoadKeyPairFrom(*tlsCertF, *tlsPrivateKeyF)
				if err != nil {
					logger.Error(fmt.Sprintf("Cannot load key pair from '%s' and '%s' to connect to server '%s'. Got: %v", *tlsCertF, *tlsPrivateKeyF, shared.RedactMongoUri(uri), err))
					os.Exit(1)
				}
				tlsConfig.Certificates = []tls.Certificate{certificates}
			}

			clientOpts.SetTLSConfig(&tlsConfig)
		}

		buildInfo, err := shared.TestConnection(
			context.Background(),
			clientOpts,
		)
		if err != nil {
			logger.Error(fmt.Sprintf("Can't connect to MongoDB: %s", err))
			os.Exit(1)
		}
		fmt.Println(string(buildInfo))
		os.Exit(0)
	}

	socketTimeout, _ := time.ParseDuration(*socketTimeoutF)
	syncTimeout, _ := time.ParseDuration(*syncTimeoutF)
	mongodbCollector := collector.NewMongodbCollector(&collector.MongodbCollectorOpts{
		ClientOpts:               clientOpts,
		TLSConnection:            *tlsF,
		TLSCertificateFile:       *tlsCertF,
		TLSPrivateKeyFile:        *tlsPrivateKeyF,
		TLSCaFile:                *tlsCAF,
		TLSHostnameValidation:    !*tlsDisableHostnameValidationF,
		DBPoolLimit:              *maxConnectionsF,
		CollectDatabaseMetrics:   *collectDatabaseF,
		CollectCollectionMetrics: *collectCollectionF,
		CollectTopMetrics:        *collectTopF,
		CollectIndexUsageStats:   *collectIndexUsageF,
		SocketTimeout:            socketTimeout,
		SyncTimeout:              syncTimeout,
	})
	defer mongodbCollector.Close()

	handlerFunc := newHandler(mongodbCollector)
	http.Handle(*metricsPathF, promhttp.InstrumentMetricHandler(prometheus.DefaultRegisterer, handlerFunc))

	var authC authConfig
	if *webAuthFile != "" {
		authConfigBytes, err := os.ReadFile(*webAuthFile)
		if err != nil {
			logger.Error(err.Error())
			os.Exit(1)
		}
		if err := yaml.Unmarshal(authConfigBytes, &authC); err != nil {
			logger.Error(err.Error())
			os.Exit(1)
		}
	}

	tlsMinVer := (web.TLSVersion)(tls.VersionTLS12)
	tlsMaxVer := (web.TLSVersion)(tls.VersionTLS13)
	if tlsMinVersion != nil && *tlsMinVersion != "" {
		if err := yaml.Unmarshal([]byte(*tlsMinVersion), &tlsMinVer); err != nil {
			logger.Error(fmt.Sprintf("Unsupported tls minimum version: %s", *tlsMinVersion))
			os.Exit(1)
		}
	}
	if tlsMaxVersion != nil && *tlsMaxVersion != "" {
		if err := yaml.Unmarshal([]byte(*tlsMaxVersion), &tlsMaxVer); err != nil {
			logger.Error(fmt.Sprintf("Unsupported tls maximum version: %s", *tlsMaxVersion))
			os.Exit(1)
		}
	}

	cipherSuites := []web.Cipher{}
	if tlsCipherSuites != nil && len(*tlsCipherSuites) != 0 {
		allCipherSuites := append(tls.CipherSuites(), tls.InsecureCipherSuites()...)
		for _, tlsCipherSuite := range *tlsCipherSuites {
			var cipherSuite *tls.CipherSuite
			for _, v := range allCipherSuites {
				if v.Name == tlsCipherSuite {
					cipherSuite = v
					break
				}
			}
			if cipherSuite == nil {
				logger.Error(fmt.Sprintf("Unsupported cipher suite: %s", tlsCipherSuite))
				os.Exit(1)
			}
			cipherSuites = append(cipherSuites, web.Cipher(cipherSuite.ID))
		}
	}

	prometheusWebConfig := prometheusWebConfig{
		TLSConfig: prometheusTLSConfig{
			MinVersion:   &tlsMinVer,
			MaxVersion:   &tlsMaxVer,
			CipherSuites: cipherSuites,
		},
	}
	if authC.ServerUser != "" {
		hashedPsw, err := bcrypt.GenerateFromPassword([]byte(authC.ServerPassword), 0)
		if err != nil {
			logger.Error(err.Error())
			os.Exit(1)
		}
		prometheusWebConfig.Users = map[string]string{
			authC.ServerUser: string(hashedPsw),
		}
	}
	if *sslCertFile != "" || *sslKeyFile != "" {
		prometheusWebConfig.TLSConfig.TLSCertPath = *sslCertFile
		prometheusWebConfig.TLSConfig.TLSKeyPath = *sslKeyFile
	}

	if *webConfigFile == "" {
		logger.Error("Use web.config.file flag/config to tell the location of prometheus web file")
		os.Exit(1)
	}
	webConfigBytes, err := yaml.Marshal(prometheusWebConfig)
	if err != nil {
		logger.Error(err.Error())
		os.Exit(1)
	}
	if err = os.WriteFile(*webConfigFile, webConfigBytes, 0600); err != nil {
		logger.Error(err.Error())
		os.Exit(1)
	}

	srv := &http.Server{}
	toolkitFlags := &web.FlagConfig{
		WebSystemdSocket:   systemdSocket,
		WebListenAddresses: listenAddress,
		WebConfigFile:      webConfigFile,
	}
	if err := web.ListenAndServe(srv, toolkitFlags, logger); err != nil {
		logger.Error("Error starting HTTP server", "err", err)
		os.Exit(1)
	}
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

func configVisit(visitFn func(string, string, reflect.Value)) {
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
			key := strings.SplitN(fieldType.Tag.Get("ini"), ",", 2)[0]

			if fieldValue.Kind() == reflect.Struct {
				if fieldValue.CanAddr() {
					if section == "" {
						section = key
					} else if section != key {
						section = fmt.Sprintf("%s.%s", section, key)
					}

					items = append(items, item{
						value:   fieldValue.Addr().Elem(),
						section: section,
					})
				}
				continue
			} else if fieldValue.Kind() == reflect.Ptr && fieldValue.Type().Elem().Kind() == reflect.String && fieldValue.IsNil() {
				continue
			}

			visitFn(section, key, fieldValue)
		}
	}
}

func configure() error {
	iniCfg, err := ini.Load(*configPathF)
	if err != nil {
		return err
	}

	if err = iniCfg.MapTo(cfg); err != nil {
		return err
	}

	configVisit(func(section, key string, fieldValue reflect.Value) {
		flagKey := fmt.Sprintf("%s.%s", section, key)
		if section == "" {
			flagKey = key
		}

		setByUser := setByUserMap[flagKey]
		kingpinF := kingpin.CommandLine.GetFlag(flagKey)
		if !setByUser || kingpinF == nil {
			return
		}

		// Don't override web.auth-file config
		if flagKey == webAuthFileFlagName {
			return
		}

		iniCfg.Section(section).Key(key).SetValue(kingpinF.Model().Value.String())
	})

	if dsn := os.Getenv("DATA_SOURCE_NAME"); dsn != "" {
		iniCfg.Section("exporter").Key("dsn").SetValue(strconv.Quote(dsn))
	}

	if err = iniCfg.SaveTo(*configPathF); err != nil {
		return err
	}

	return nil
}

func overrideFlags() {
	configVisit(func(section, key string, fieldValue reflect.Value) {
		flagKey := fmt.Sprintf("%s.%s", section, key)
		if section == "" {
			flagKey = key
		}

		setByUser := setByUserMap[flagKey]
		kingpinF := kingpin.CommandLine.GetFlag(flagKey)
		if setByUser || kingpinF == nil {
			return
		}

		var values []reflect.Value
		if fieldValue.Kind() == reflect.Slice {
			for i := 0; i < fieldValue.Len(); i++ {
				values = append(values, fieldValue.Index(i))
			}
		} else {
			values = []reflect.Value{fieldValue}
		}

		for i := range values {
			switch values[i].Kind() {
			case reflect.Int, reflect.Int8, reflect.Int16, reflect.Float32, reflect.Int64:
				kingpinF.Model().Value.Set(strconv.FormatInt(values[i].Int(), 10))
			case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
				kingpinF.Model().Value.Set(strconv.FormatUint(values[i].Uint(), 10))
			case reflect.Bool:
				kingpinF.Model().Value.Set(strconv.FormatBool(values[i].Bool()))
			case reflect.Ptr:
				if !values[i].IsNil() {
					if values[i].Elem().Kind() == reflect.Bool {
						kingpinF.Model().Value.Set(strconv.FormatBool(values[i].Elem().Bool()))
					} else {
						kingpinF.Model().Value.Set(values[i].Elem().String())
					}
				}
			default:
				kingpinF.Model().Value.Set(values[i].String())
			}
		}
	})
}

type authConfig struct {
	ServerUser     string `yaml:"server_user,omitempty"`
	ServerPassword string `yaml:"server_password,omitempty"`
}

type prometheusWebConfig struct {
	TLSConfig prometheusTLSConfig `yaml:"tls_server_config"`
	Users     map[string]string   `yaml:"basic_auth_users"`
}

type prometheusTLSConfig struct {
	TLSCertPath  string          `yaml:"cert_file"`
	TLSKeyPath   string          `yaml:"key_file"`
	MinVersion   *web.TLSVersion `yaml:"min_version"`
	MaxVersion   *web.TLSVersion `yaml:"max_version"`
	CipherSuites []web.Cipher    `yaml:"cipher_suites,omitempty"`
}

func newHandler(collector *collector.MongodbCollector) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		registry := prometheus.NewRegistry()
		registry.MustRegister(collector)

		gatherers := prometheus.Gatherers{
			prometheus.DefaultGatherer,
			registry,
		}

		// Delegate http serving to Prometheus client library, which will call collector.Collect.
		h := promhttp.HandlerFor(gatherers, promhttp.HandlerOpts{})
		h.ServeHTTP(w, r)
	}
}

// this function is for translating single-hyphen flags into long flags,
// to make it compatible with earily PMM/SSM version of node_exporter
func convertFlagAction(short rune) func(ctx *kingpin.ParseContext) error {
	convertedMap := make(map[rune]bool)

	return func(pc *kingpin.ParseContext) error {
		if convertedMap[short] {
			return nil
		}

		for _, elem := range pc.Elements {
			if elem.Clause == nil {
				continue
			}

			flagClause, ok := elem.Clause.(*kingpin.FlagClause)
			if !ok || flagClause.Model().Short != short {
				continue
			}

			ctx, err := kingpin.CommandLine.ParseContext([]string{fmt.Sprintf("--%c%s", short, *elem.Value)})
			if err != nil && ctx != nil && len(ctx.Elements) > 0 && ctx.Elements[0].Clause != nil {
				// with standard flag package, single-hyphen bool flag is in format
				// '-<name>=<bool>', this code block here tries to translate it into
				// kingpin long bool flag

				clause, ok := ctx.Elements[0].Clause.(*kingpin.FlagClause)
				if !ok || !clause.Model().IsBoolFlag() {
					return err
				}

				boolStrs := strings.Split(*elem.Value, "=")
				if len(boolStrs) == 1 {
					return err
				}

				var boolValue bool
				boolValue, err = strconv.ParseBool(boolStrs[len(boolStrs)-1])
				if err != nil {
					return err
				}

				if boolValue {
					ctx, err = kingpin.CommandLine.ParseContext([]string{fmt.Sprintf("--%s", clause.Model().Name)})
				} else {
					ctx, err = kingpin.CommandLine.ParseContext([]string{fmt.Sprintf("--no-%s", clause.Model().Name)})
				}
			}
			if err != nil || ctx == nil || len(ctx.Elements) == 0 || ctx.Elements[0].Clause == nil {
				return err
			}

			flag, ok := ctx.Elements[0].Clause.(*kingpin.FlagClause)
			if !ok {
				return fmt.Errorf("unknow flag")
			}

			setByUserMap[flag.Model().Name] = true
			if err = flag.Model().Value.Set(*ctx.Elements[0].Value); err != nil {
				return err
			}
		}

		convertedMap[short] = true
		return nil
	}
}
