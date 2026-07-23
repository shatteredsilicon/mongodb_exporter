// mongodb_exporter
// Copyright (C) 2017 Percona LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
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
	"log"
	"log/slog"
	"net"
	"net/url"
	"os"
	"reflect"
	"regexp"
	"strconv"
	"strings"

	"github.com/alecthomas/kong"
	"github.com/prometheus/common/promslog"
	"github.com/prometheus/exporter-toolkit/web"
	"go.yaml.in/yaml/v2"
	"golang.org/x/crypto/bcrypt"
	"gopkg.in/ini.v1"

	"github.com/shatteredsilicon/mongodb_exporter/exporter"
	"github.com/shatteredsilicon/mongodb_exporter/shared"
)

//nolint:gochecknoglobals
var (
	version   string
	commit    string
	buildDate string
)

// GlobalFlags has command line flags to configure the exporter.
type GlobalFlags struct {
	User                  string   `name:"mongodb.user" help:"monitor user, need clusterMonitor role in admin db and read role in local db" env:"MONGODB_USER" placeholder:"monitorUser"`
	Password              string   `name:"mongodb.password" help:"monitor user password" env:"MONGODB_PASSWORD" placeholder:"monitorPassword"`
	CollStatsNamespaces   string   `name:"mongodb.collstats-colls" help:"List of comma separared databases.collections to get $collStats" placeholder:"db1,db2.col2"`
	IndexStatsCollections string   `name:"mongodb.indexstats-colls" help:"List of comma separared databases.collections to get $indexStats" placeholder:"db1.col1,db2.col2"`
	URI                   []string `name:"mongodb.uri" help:"MongoDB connection URI" env:"MONGODB_URI" placeholder:"mongodb://user:pass@127.0.0.1:27017/admin?ssl=true"`
	GlobalConnPool        bool     `name:"mongodb.global-conn-pool" help:"Use global connection pool instead of creating new pool for each http request." negatable:""`
	DirectConnect         bool     `name:"mongodb.direct-connect" help:"Whether or not a direct connect should be made. Direct connections are not valid if multiple hosts are specified or an SRV URI is used." default:"true" negatable:""`
	WebListenAddress      string   `name:"web.listen-address" help:"Address to listen on for web interface and telemetry" default:":9216"`
	WebTelemetryPath      string   `name:"web.telemetry-path" help:"Metrics expose path" default:"/metrics" aliases:"web.metrics-path"`
	LogLevel              string   `name:"log.level" help:"Only log messages with the given severity or above. Valid levels: [debug, info, warn, error, fatal]" enum:"debug,info,warn,error,fatal" default:"error"`
	ConnectTimeoutMS      int      `name:"mongodb.connect-timeout-ms" help:"Connection timeout in milliseconds" default:"5000"`

	EnableExporterMetrics    bool `name:"collector.exporter-metrics" help:"Enable collecting metrics about the exporter itself (process_*, go_*)" negatable:"" default:"True"`
	EnableDiagnosticData     bool `name:"collector.diagnosticdata" help:"Enable collecting metrics from getDiagnosticData"`
	EnableReplicasetStatus   bool `name:"collector.replicasetstatus" help:"Enable collecting metrics from replSetGetStatus"`
	EnableReplicasetConfig   bool `name:"collector.replicasetconfig" help:"Enable collecting metrics from replSetGetConfig"`
	EnableDBStats            bool `name:"collector.dbstats" help:"Enable collecting metrics from dbStats" aliases:"collect.database"`
	EnableDBStatsFreeStorage bool `name:"collector.dbstatsfreestorage" help:"Enable collecting free space metrics from dbStats"`
	EnableTopMetrics         bool `name:"collector.topmetrics" help:"Enable collecting metrics from top admin command" aliases:"collect.topmetrics"`
	EnableCurrentopMetrics   bool `name:"collector.currentopmetrics" help:"Enable collecting metrics currentop admin command"`
	EnableIndexStats         bool `name:"collector.indexstats" help:"Enable collecting metrics from $indexStats" aliases:"collect.indexusage"`
	EnableCollStats          bool `name:"collector.collstats" help:"Enable collecting metrics from $collStats" aliases:"collect.collection"`
	EnableProfile            bool `name:"collector.profile" help:"Enable collecting metrics from profile"`
	EnableFCV                bool `name:"collector.fcv" help:"Enable Feature Compatibility Version collector"`
	EnableShards             bool `help:"Enable collecting metrics from sharded Mongo clusters about chunks" name:"collector.shards"`
	EnablePBM                bool `help:"Enable collecting metrics from Percona Backup for MongoDB" name:"collector.pbm"`

	EnableOverrideDescendingIndex bool `name:"metrics.overridedescendingindex" help:"Enable descending index name override to replace -1 with _DESC"`

	CollectAll bool `name:"collect-all" help:"Enable all collectors. Same as specifying all --collector.<name>"`

	CollStatsLimit         int  `name:"collector.collstats-limit" help:"Disable collstats, dbstats, topmetrics and indexstats collector if there are more than <n> collections. 0=No limit" default:"0"`
	CollStatsEnableDetails bool `name:"collector.collstats-enable-details" help:"Enable collecting index details and wired tiger metrics from $collStats" default:"false"`

	ProfileTimeTS int `name:"collector.profile-time-ts" help:"Set time for scrape slow queries." default:"30"`

	CurrentOpSlowTime string `name:"collector.currentopmetrics-slow-time" help:"Set minimum time for registration queries." default:"5m"`

	DiscoveringMode bool `name:"discovering-mode" help:"Enable autodiscover collections" negatable:""`
	CompatibleMode  bool `name:"compatible-mode" help:"Enable old mongodb-exporter compatible metrics" negatable:""`
	Version         bool `name:"version" help:"Show version and exit"`
	SplitCluster    bool `name:"split-cluster" help:"Treat each node in cluster as a separate target" negatable:"" default:"false"`

	Config string `name:"config" help:"Path of config file" default:"/opt/ss/ssm-client/mongodb_exporter.conf"`
	Test   bool   `name:"test" help:"Check MongoDB connection and exit."`

	TLS                          bool   `name:"mongodb.tls" help:"Enable tls connection with mongo server"`
	TLSCert                      string `name:"mongodb.tls-cert" help:"Path to PEM file that contains the certificate."`
	TLSPrivateKey                string `name:"mongodb.tls-private-key" help:"Path to PEM file that contains the decrypted private key."`
	TLSCA                        string `name:"mongodb.tls-ca" help:"Path to PEM file that contains the trusted CAs."`
	TLSDisableHostnameValidation bool   `name:"mongodb.tls-disable-hostname-validation" help:"Disable hostname validation."`

	WebConfigFile      string   `name:"web.config" help:"Path to the file having Prometheus TLS config for basic auth" default:"/opt/ss/ssm-client/mongodb_exporter.yml"`
	WebAuthFile        string   `name:"web.auth-file" help:"Path to YAML file with server_user, server_password keys for HTTP Basic authentication."`
	WebTLSMinVersion   string   `name:"web.tls-min-version" help:"Minimum TLS version that is acceptable."`
	WebTLSMaxVersion   string   `name:"web.tls-max-version" help:"Maximum TLS version that is acceptable."`
	WebTLSCipherSuites []string `name:"web.tls-cipher-suites" help:"A list of enabled TLS 1.0–1.2 cipher suites. Check full list at https://github.com/golang/go/blob/master/src/crypto/tls/cipher_suites.go"`
	WebSSLCertFile     string   `name:"web.ssl-cert-file" help:"Path to SSL certificate file."`
	WebSSLKeyFile      string   `name:"web.ssl-key-file" help:"Path to SSL key file."`
}

func main() {
	var opts GlobalFlags
	ctx := kong.Parse(&opts,
		kong.Name("mongodb_exporter"),
		kong.Description("MongoDB Prometheus exporter"),
		kong.UsageOnError(),
		kong.ConfigureHelp(kong.HelpOptions{
			Compact: true,
		}),
		kong.Vars{
			"version": version,
		})

	if opts.Version {
		fmt.Println("mongodb_exporter - MongoDB Prometheus exporter")
		fmt.Printf("Version: %s\n", version)
		fmt.Printf("Commit: %s\n", commit)
		fmt.Printf("Build date: %s\n", buildDate)
		return
	}

	iniCfg, err := ini.Load(opts.Config)
	if err != nil {
		ctx.Fatalf("Failed to load config file %s: %s", opts.Config, err.Error())
	}

	if os.Getenv("ON_CONFIGURE") == "1" {
		err := configure(ctx, iniCfg)
		if err != nil {
			os.Exit(1)
		}
		if err := iniCfg.SaveTo(opts.Config); err != nil {
			os.Exit(1)
		}
		os.Exit(0)
	}

	// override flag value with config value
	// if it's not set
	overrideFlags(ctx, iniCfg)

	logLevel := promslog.NewLevel()
	_ = logLevel.Set(opts.LogLevel)
	logger := promslog.New(&promslog.Config{
		Level: logLevel,
	})

	if opts.Test {
		URIs := parseURIList(opts.URI, logger, opts.SplitCluster)
		buildInfos := make([][]byte, len(URIs))
		for serverIdx := range URIs {
			exporterOpts := buildExporterOpts(opts, URIs[serverIdx], logger)
			client, err := exporter.Connect(context.Background(), exporterOpts)
			if err != nil {
				ctx.Fatalf("Can't connect to MongoDB: %s", err.Error())
			} else {
				buildInfos[serverIdx], err = shared.TestConnection(context.Background(), client)
				if err != nil {
					ctx.Fatalf("Failed to test the connection: %s", err.Error())
				}
			}
			client.Disconnect(context.Background())
		}
		if len(buildInfos) > 0 {
			fmt.Println(string(buildInfos[0]))
		}
		os.Exit(0)
	}

	logger.Debug("Compatible mode", "compatible_mode", opts.CompatibleMode)

	if opts.WebTelemetryPath == "" {
		logger.Warn("Web telemetry path \"\" is invalid, falling back to \"/\" instead")
		opts.WebTelemetryPath = "/"
	}

	if len(opts.URI) == 0 {
		ctx.Fatalf("No MongoDB hosts were specified. You must specify the host(s) with the --mongodb.uri command argument or the MONGODB_URI environment variable")
	}

	if opts.WebConfigFile == "" {
		ctx.Fatalf("Use web.config.file flag/config to tell the location of prometheus web file")
	}

	var authC authConfig
	if opts.WebAuthFile != "" {
		authConfigBytes, err := os.ReadFile(opts.WebAuthFile)
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
	if opts.WebTLSMinVersion != "" {
		if err := yaml.Unmarshal([]byte(opts.WebTLSMinVersion), &tlsMinVer); err != nil {
			logger.Error(fmt.Sprintf("Unsupported tls minimum version: %s", opts.WebTLSMinVersion))
			os.Exit(1)
		}
	}
	if opts.WebTLSMaxVersion != "" {
		if err := yaml.Unmarshal([]byte(opts.WebTLSMaxVersion), &tlsMaxVer); err != nil {
			logger.Error(fmt.Sprintf("Unsupported tls maximum version: %s", opts.WebTLSMaxVersion))
			os.Exit(1)
		}
	}

	cipherSuites := []web.Cipher{}
	if len(opts.WebTLSCipherSuites) != 0 {
		allCipherSuites := append(tls.CipherSuites(), tls.InsecureCipherSuites()...)
		for _, tlsCipherSuite := range opts.WebTLSCipherSuites {
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
		TLSConfig: tlsConfig{
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
	if opts.WebSSLCertFile != "" || opts.WebSSLKeyFile != "" {
		prometheusWebConfig.TLSConfig.TLSCertPath = opts.WebSSLCertFile
		prometheusWebConfig.TLSConfig.TLSKeyPath = opts.WebSSLKeyFile
	}

	webConfigBytes, err := yaml.Marshal(prometheusWebConfig)
	if err != nil {
		logger.Error(err.Error())
		os.Exit(1)
	}
	if err = os.WriteFile(opts.WebConfigFile, webConfigBytes, 0600); err != nil {
		logger.Error(err.Error())
		os.Exit(1)
	}

	serverOpts := &exporter.ServerOpts{
		Path:              opts.WebTelemetryPath,
		MultiTargetPath:   "/scrape",
		OverallTargetPath: "/scrapeall",
		WebListenAddress:  opts.WebListenAddress,
		TLSConfigPath:     opts.WebConfigFile,
	}
	exporter.RunWebServer(serverOpts, buildServers(opts, logger), logger)
}

func buildExporter(opts GlobalFlags, uri string, log *slog.Logger) *exporter.Exporter {
	exporterOpts := buildExporterOpts(opts, uri, log)
	return exporter.New(exporterOpts)
}

func buildServers(opts GlobalFlags, logger *slog.Logger) []*exporter.Exporter {
	URIs := parseURIList(opts.URI, logger, opts.SplitCluster)
	servers := make([]*exporter.Exporter, len(URIs))
	for serverIdx := range URIs {
		servers[serverIdx] = buildExporter(opts, URIs[serverIdx], logger)
	}

	return servers
}

func parseURIList(uriList []string, logger *slog.Logger, splitCluster bool) []string { //nolint:gocognit,cyclop
	var URIs []string

	// If server URI is prefixed with mongodb scheme string, then every next URI in
	// line not prefixed with mongodb scheme string is a part of cluster. Otherwise,
	// treat it as a standalone server
	realURI := ""
	matchRegexp := regexp.MustCompile(`^mongodb(\+srv)?://`)
	for _, URI := range uriList {
		matches := matchRegexp.FindStringSubmatch(URI)
		if matches != nil {
			if realURI != "" {
				// Add the previous host buffer to the url list as we met the scheme part
				URIs = append(URIs, realURI)
				realURI = ""
			}
			if matches[1] == "" {
				realURI = URI
			} else {
				// There can be only one host in SRV connection string
				if splitCluster {
					// In splitCluster mode we get srv connection string from SRV recors
					URI = exporter.GetSeedListFromSRV(URI, logger)
				}
				URIs = append(URIs, URI)
			}
		} else {
			if realURI == "" {
				URIs = append(URIs, "mongodb://"+URI)
			} else {
				realURI += "," + URI
			}
		}
	}
	if realURI != "" {
		URIs = append(URIs, realURI)
	}

	if splitCluster {
		// In this mode we split cluster strings into separate targets
		separateURIs := []string{}
		for _, hosturl := range URIs {
			urlParsed, err := url.Parse(hosturl)
			if err != nil {
				log.Fatalf("Failed to parse URI %s: %v", hosturl, err)
			}
			for _, host := range strings.Split(urlParsed.Host, ",") {
				targetURI := "mongodb://"
				if urlParsed.User != nil {
					targetURI += urlParsed.User.String() + "@"
				}
				targetURI += host
				if urlParsed.Path != "" {
					targetURI += urlParsed.Path
				}
				if urlParsed.RawQuery != "" {
					targetURI += "?" + urlParsed.RawQuery
				}
				separateURIs = append(separateURIs, targetURI)
			}
		}
		return separateURIs
	}
	return URIs
}

// buildURIManually builds the URI manually by checking if the user and password are supplied
func buildURIManually(uri string, user string, password string) string {
	uriArray := strings.SplitN(uri, "://", 2) //nolint:mnd
	prefix := uriArray[0] + "://"
	uri = uriArray[1]

	// IF user@pass not contained in uri AND custom user and pass supplied in arguments
	// DO concat a new uri with user and pass arguments value
	if !strings.Contains(uri, "@") && user != "" && password != "" {
		// add user and pass to the uri
		uri = fmt.Sprintf("%s:%s@%s", user, password, uri)
	}

	// add back prefix after adding the user and pass
	uri = prefix + uri

	return uri
}

func buildURI(uri string, user string, password string) string {
	defaultPrefix := "mongodb://" // default prefix

	if !strings.HasPrefix(uri, defaultPrefix) && !strings.HasPrefix(uri, "mongodb+srv://") {
		uri = defaultPrefix + uri
	}
	parsedURI, err := url.Parse(uri)
	if err != nil {
		// PMM generates URI with escaped path to socket file, so url.Parse fails
		// in this case we build URI manually
		return buildURIManually(uri, user, password)
	}

	if parsedURI.User == nil && user != "" && password != "" {
		parsedURI.User = url.UserPassword(user, password)
	}

	return parsedURI.String()
}

func buildExporterOpts(opts GlobalFlags, uri string, log *slog.Logger) *exporter.Opts {
	uri = buildURI(uri, opts.User, opts.Password)

	uriParsed, _ := url.Parse(uri)
	var nodeName string
	switch {
	case uriParsed == nil:
		nodeName = ""
	case uriParsed.Port() != "":
		nodeName = net.JoinHostPort(uriParsed.Hostname(), uriParsed.Port())
	default:
		nodeName = uriParsed.Host
	}

	collStatsNamespaces := []string{}
	if opts.CollStatsNamespaces != "" {
		collStatsNamespaces = strings.Split(opts.CollStatsNamespaces, ",")
	}
	indexStatsCollections := []string{}
	if opts.IndexStatsCollections != "" {
		indexStatsCollections = strings.Split(opts.IndexStatsCollections, ",")
	}

	var tlsConfig *tls.Config
	if opts.TLS {
		tlsConfig = &tls.Config{
			InsecureSkipVerify: opts.TLSDisableHostnameValidation,
		}
		if len(opts.TLSCA) > 0 {
			ca, err := shared.LoadCaFrom(opts.TLSCA)
			if err != nil {
				log.Error(fmt.Sprintf("Couldn't load client CAs from %s. Got: %s", opts.TLSCA, err))
				os.Exit(1)
			}
			tlsConfig.RootCAs = ca
		}
		if len(opts.TLSCert) > 0 {
			certificates, err := shared.LoadKeyPairFrom(opts.TLSCert, opts.TLSPrivateKey)
			if err != nil {
				log.Error(fmt.Sprintf("Cannot load key pair from '%s' and '%s' to connect to server '%s'. Got: %v", opts.TLSCert, opts.TLSPrivateKey, shared.RedactMongoUri(uri), err))
				os.Exit(1)
			}
			tlsConfig.Certificates = []tls.Certificate{certificates}
		}
	}

	return &exporter.Opts{
		CollStatsNamespaces:   collStatsNamespaces,
		CompatibleMode:        opts.CompatibleMode,
		DiscoveringMode:       opts.DiscoveringMode,
		IndexStatsCollections: indexStatsCollections,
		Logger:                log,
		URI:                   uri,
		NodeName:              nodeName,
		GlobalConnPool:        opts.GlobalConnPool,
		DirectConnect:         opts.DirectConnect,
		ConnectTimeoutMS:      opts.ConnectTimeoutMS,
		TLSConfig:             tlsConfig,

		DisableDefaultRegistry:   !opts.EnableExporterMetrics,
		EnableDiagnosticData:     opts.EnableDiagnosticData,
		EnableReplicasetStatus:   opts.EnableReplicasetStatus,
		EnableReplicasetConfig:   opts.EnableReplicasetConfig,
		EnableCurrentopMetrics:   opts.EnableCurrentopMetrics,
		EnableTopMetrics:         opts.EnableTopMetrics,
		EnableDBStats:            opts.EnableDBStats,
		EnableDBStatsFreeStorage: opts.EnableDBStatsFreeStorage,
		EnableIndexStats:         opts.EnableIndexStats,
		EnableCollStats:          opts.EnableCollStats,
		EnableProfile:            opts.EnableProfile,
		EnableShards:             opts.EnableShards,
		EnableFCV:                opts.EnableFCV,
		EnablePBMMetrics:         opts.EnablePBM,

		EnableOverrideDescendingIndex: opts.EnableOverrideDescendingIndex,

		CollStatsLimit:         opts.CollStatsLimit,
		CollStatsEnableDetails: opts.CollStatsEnableDetails,
		CollectAll:             opts.CollectAll,
		ProfileTimeTS:          opts.ProfileTimeTS,
		CurrentOpSlowTime:      opts.CurrentOpSlowTime,
	}
}

func flagWalk(ctx *kong.Context, walkFunc func(string, string, *kong.Flag, bool)) {
	setByUserMap := make(map[string]bool)
	for _, trace := range ctx.Path {
		if trace.Flag == nil {
			continue
		}

		setByUserMap[trace.Flag.Name] = true
	}

	for _, flag := range ctx.Flags() {
		sectionKeys := append([]string{flag.Name}, flag.Aliases...)
		for _, sectionKey := range sectionKeys {
			var section, key string
			if parts := strings.SplitN(sectionKey, ".", 2); len(parts) > 1 {
				section = parts[0]
				key = parts[1]
			} else {
				key = parts[0]
			}
			walkFunc(section, key, flag, setByUserMap[flag.Name])
		}
	}
}

func configure(ctx *kong.Context, cfg *ini.File) error {
	flagWalk(ctx, func(section, key string, flag *kong.Flag, set bool) {
		if !set {
			return
		}

		cfg.Section(section).Key(key).SetValue(fmt.Sprint(flag.Value.Target.Interface()))
	})

	if dsn := os.Getenv("DATA_SOURCE_NAME"); dsn != "" {
		cfg.Section("exporter").Key("dsn").SetValue(strconv.Quote(dsn))
	}

	return nil
}

func overrideFlags(ctx *kong.Context, cfg *ini.File) {
	flagWalk(ctx, func(section, key string, flag *kong.Flag, set bool) {
		if set {
			return
		}

		if !cfg.HasSection(section) || !cfg.Section(section).HasKey(key) {
			return
		}

		switch flag.Value.Target.Kind() {
		case reflect.Slice:
			flag.Apply(reflect.ValueOf(cfg.Section(section).Key(key).ValueWithShadows()))
		case reflect.Bool:
			flag.Apply(reflect.ValueOf(cfg.Section(section).Key(key).MustBool(flag.Value.Target.Bool())))
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Float32, reflect.Int64:
			flag.Apply(reflect.ValueOf(cfg.Section(section).Key(key).MustInt64(flag.Value.Target.Int())).Convert(flag.Value.Target.Type()))
		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			flag.Apply(reflect.ValueOf(cfg.Section(section).Key(key).MustUint64(flag.Value.Target.Uint())).Convert(flag.Value.Target.Type()))
		default:
			flag.Apply(reflect.ValueOf(cfg.Section(section).Key(key).Value()).Convert(flag.Value.Target.Type()))
		}
	})
}

type authConfig struct {
	ServerUser     string `yaml:"server_user,omitempty"`
	ServerPassword string `yaml:"server_password,omitempty"`
}

type prometheusWebConfig struct {
	TLSConfig tlsConfig         `yaml:"tls_server_config"`
	Users     map[string]string `yaml:"basic_auth_users"`
}

type tlsConfig struct {
	TLSCertPath  string          `yaml:"cert_file"`
	TLSKeyPath   string          `yaml:"key_file"`
	MinVersion   *web.TLSVersion `yaml:"min_version"`
	MaxVersion   *web.TLSVersion `yaml:"max_version"`
	CipherSuites []web.Cipher    `yaml:"cipher_suites,omitempty"`
}
