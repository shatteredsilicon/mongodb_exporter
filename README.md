# MongoDB exporter
[![Go Report Card](https://goreportcard.com/badge/github.com/shatteredsilicon/mongodb_exporter)](https://goreportcard.com/report/github.com/shatteredsilicon/mongodb_exporter)


This is the new MongoDB exporter implementation that handles ALL metrics exposed by MongoDB monitoring commands.
This new implementation loops over all the fields exposed in diagnostic commands and tries to get data from them.

Currently, these metric sources are implemented:
- $collStats
- $indexStats
- getDiagnosticData
- replSetGetStatus
- replSetGetConfig
- serverStatus

## Supported MongoDB versions

The exporter works with Percona Server for MongoDB and MongoDB Community or Enterprise Edition versions 6.0 and newer. Older versions might also work but are not tested anymore.

### Permissions
Connecting user should have sufficient rights to query needed stats:

```
      {
         "role":"clusterMonitor",
         "db":"admin"
      },
      {
         "role":"read",
         "db":"local"
      }
```
When using the PBM collector to get metrics from Percona Backup for MongoDB, the user should also have sufficient privileges
to query the PBM internal collections (in the `admin` database). One option is to grant `find` privileges on the `admin` collection:
```
privileges: [
        { resource: { db: "admin", collection: "" }, actions: [ "find" ] },
],
```

Alternatively, you can grant the `find` privilege for each PBM internal collection (see: https://docs.percona.com/percona-backup-mongodb/details/control-collections.html):
```
privileges: [
       { resource: { db: "admin", collection: "pbmBackups" }, actions: [ "find" ] },
       { resource: { db: "admin", collection: "pbmAgents" }, actions: [ "find" ] },
       { resource: { db: "admin", collection: "pbmConfig" }, actions: [ "find" ] },
       ...
],
```
However, this is not recommended as the list of internal collections may change in future releases.

More info about roles in MongoDB [documentation](https://docs.mongodb.com/manual/reference/built-in-roles/#mongodb-authrole-clusterMonitor).

#### Example
```sh
mongodb_exporter_linux_amd64/mongodb_exporter --mongodb.uri=mongodb://127.0.0.1:17001
```

#### MongoDB Authentication
You can supply the mongodb user/password direct in the `--mongodb.uri=` like `--mongodb.uri=mongodb://user:pass@127.0.0.1:17001`, you can also supply the mongodb user/password with `--mongodb.user=`, `--mongodb.password=`
but the user and password info will be leaked via `ps` or `top` command, for security issue, you can use `MONGODB_USER` and `MONGODB_PASSWORD` env variable to set user/password for given uri
```sh
MONGODB_USER=XXX MONGODB_PASSWORD=YYY mongodb_exporter_linux_amd64/mongodb_exporter --mongodb.uri=mongodb://127.0.0.1:17001 --mongodb.collstats-colls=db1.c1,db2.c2
# or
export MONGODB_USER=XXX
export MONGODB_PASSWORD=YYY
mongodb_exporter_linux_amd64/mongodb_exporter --mongodb.uri=mongodb://127.0.0.1:17001 --mongodb.collstats-colls=db1.c1,db2.c2
```

#### Multi-target support
You can run the exporter specifying multiple URIs, devided by a comma in --mongodb.uri option or MONGODB_URI environment variable in order to monitor multiple mongodb instances with the a single mongodb_exporter instance.
```sh
--mongodb.uri=mongodb://user:pass@127.0.0.1:27017/admin,mongodb://user2:pass2@127.0.0.1:27018/admin
```
In this case you can use the **/scrape** endpoint with the **target** parameter to retreive the specified tartget's metrics.  When querying the data you can use just mongodb://host:port in the target parameter without other parameters and, of course without host credentials
```sh
GET /scrape?target=mongodb://127.0.0.1:27018
```
If your URI is prefixed by mongodb:// or mongodb+srv:// schema, any host not prefixed by it after comma is being treated as part of a cluster rather then as a standalone host. Thus clusters and standalone hosts can be combined like this:
```
--mongodb.uri=mongodb+srv://user:pass@host1:27017,host2:27017,host3:27017/admin,mongodb://user2:pass2@host4:27018/admin
```

You can use the --split-cluster option to split all cluster nodes into separate targets. This mode is useful when cluster nodes are defined as SRV records and the mongodb_exporter is running with mongodb+srv domain specified. In this case SRV records will be queried upon mongodb_exporter start and each cluster node can be queried using the **target** parameter of multitarget endpoint. 

#### Overall targets request endpoint

There is an overall targets endpoint **/scrapeall** that queries all the targets in one request. It can be used to store multiple node metrics without separate target requests. In this case, each node metric will have a **instance** label containing the node name as a host:port pair (or just host if no port was not specified). For example, for mongodb_exporter running with the options:
```
--mongodb.uri="mongodb://host1:27015,host2:27016" --split-cluster=true
``` 
we get metrics like this:
```
mongodb_up{instance="host1:27015"} 1
mongodb_up{instance="host2:27016"} 1
```

#### Enabling collstats metrics gathering
`--mongodb.collstats-colls` receives a list of databases and collections to monitor using collstats.
Usage example: `--mongodb.collstats-colls=database1.collection1,database2.collection2`
```sh
mongodb_exporter_linux_amd64/mongodb_exporter --mongodb.uri=mongodb://127.0.0.1:17001 --mongodb.collstats-colls=db1.c1,db2.c2
```
#### Enabling compatibility mode.
When compatibility mode is enabled by the `--compatible-mode`, the exporter will expose all new metrics with the new naming and labeling schema and at the same time will expose metrics in the version 1 compatible way.
For example, if compatibility mode is enabled, the metric `mongodb_ss_wt_log_log_bytes_written` (new format)
```
# HELP mongodb_ss_wt_log_log_bytes_written serverStatus.wiredTiger.log.
# TYPE mongodb_ss_wt_log_log_bytes_written untyped
mongodb_ss_wt_log_log_bytes_written 2.6208e+06
```
will be also exposed as `mongodb_mongod_wiredtiger_log_bytes_total`  with the `unwritten` label.
```
HELP mongodb_mongod_wiredtiger_log_bytes_total mongodb_mongod_wiredtiger_log_bytes_total
# TYPE mongodb_mongod_wiredtiger_log_bytes_total untyped
mongodb_mongod_wiredtiger_log_bytes_total{type="unwritten"} 2.6208e+06
```
#### Enabling profile metrics gathering
`--collector.profile` 
To collect metrics, you need to enable the profiler in [MongoDB](https://www.mongodb.com/docs/manual/tutorial/manage-the-database-profiler/):
Usage example: `db.setProfilingLevel(2)`

|Level|Description|
|-----|-----------|
|0| The profiler is off and does not collect any data. This is the default profiler level.|
|1| The profiler collects data for operations that take longer than the value of `slowms` or that match a filter.<br> When a filter is set: <ul><li> The `slowms` and `sampleRate` options are not used for profiling.</li><li>The profiler only captures operations that match the filter.</li></ul>
|2|The profiler collects data for all operations.|

#### Enabling shards metrics gathering
When shard metrics collection is enabled by `--collector.shards`, the exporter will expose metrics related to sharded Mongo. 
Example, if shards collector is enabled:
```
# HELP mongodb_shards_collection_chunks_count sharded collection chunks.
# TYPE mongodb_shards_collection_chunks_count counter
mongodb_shards_collection_chunks_count{collection="system.sessions",database="config",shard="rs1"} 250
mongodb_shards_collection_chunks_count{collection="system.sessions",database="config",shard="rs2"} 250
```
You can see shard name, it's collection, database and count.

#### Cluster role labels
The exporter sets some topology labels in all metrics.
The labels are:

- cl_role: Cluster role according to this table:

|Server type|Label|
|-----|-----|
|mongos|mongos|
|regular instance (primary or secondary)|shardsvr|
|arbiter|shardsvr|
|standalone|(empty string)|

- cl_id: Cluster ID
- rs_nm: Replicaset name
- rs_state: Replicaset state is an integer from `getDiagnosticData()` -> `replSetGetStatus.myState`. 
Check [the official documentation](https://docs.mongodb.com/manual/reference/replica-states/) for details on replicaset status values.

## Usage Reference

See the [Reference Guide](REFERENCE.md) for details on using the exporter.
## Bug Reports / Feature PR
