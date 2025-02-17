# Tributary User Guide of Config Details

Tributary Configuration consists of four parts, including server, source, channel, and sink.

## Server

```properties
server.port=8765
server.host=127.0.0.1
```

The Tributary service provides metrics indicators in the form of http restful api.

The parameter "server.host" is a regular expression that is used to filter the
required hosts when there are multiple network cards in the machine, and assign the value to the
host dimension in the metrics.

Get tributary metrics as follows, ensure that the port matches the server.port configuration:

```shell
$ curl -s http://localhost:8765/metrics|grep -v '#'
sink_print_counter{host="127.0.0.1",id="c1_group_1_0",} 1.0
channel_block_cache_query_hit_count{topic="c1",host="127.0.0.1",} 1.0
channel_buffer_usage{topic="c1",host="127.0.0.1",} 0.0
channel_write_bytes{topic="c1",host="127.0.0.1",} 95.0
channel_active_segment{topic="c1",host="127.0.0.1",} 1.0
channel_read_bytes{topic="c1",host="127.0.0.1",} 95.0
channel_block_cache_query_total_count{topic="c1",host="127.0.0.1",} 1.0
sink_lag{host="127.0.0.1",id="c1_group_1",} 0.0
```

| Key                 | default  | valid value | describe                                                                                                      |
|---------------------|----------|-------------|---------------------------------------------------------------------------------------------------------------|
| server.port         |          | int(number) | the port to bind, range 1000-65535                                                                            |
| server.host         | .*       | string      | the pattern to filter expected host as the metrics value of host dimension, select first if found multi hosts |
| server.metrics.path | /metrics | string      | the metrics http path                                                                                         |

## Source

Tributary supports defining multiple sources in the application.properties

Each source must be associated with a channel and its implementation as follows

```properties
source.s1.channel=c1
source.s1.implement=netty
source.s2.channel=c2
source.s2.implement=netty
``` 

Above are defined two sources named s1 and s2, in which s1 is bound to channel c1 and s2 is bound to
channel c2, and their implementations are both Netty.

The source must config the implement to receive data, convert
as [Records](../tributary-common/src/main/java/org/zicat/tributary/common/records/Records.java) and
append to channel for sinks to consume.

| key       | default | type   | describe                                                                                                      |
|-----------|---------|--------|---------------------------------------------------------------------------------------------------------------|
| channel   |         | string | the channel to append records                                                                                 |   
| implement | netty   | string | the [SourceFactory](../tributary-source/src/main/java/org/zicat/tributary/source/SourceFactory.java) identity |

Tributary provide the
[SourceFactory](../tributary-source/src/main/java/org/zicat/tributary/source/SourceFactory.java)
interface that supports the development of specific sources scenarios.

Tributary also provides the default implementation of
[Netty](../tributary-source/src/main/java/org/zicat/tributary/source/netty/DefaultNettySourceFactory.java),
which supports receiving data from the network. The configuration parameters for Netty are as
follows :

```properties
source.s1.netty.host=10\\.103\\.1\\..*,localhost
source.s1.netty.port=8200
source.s1.netty.threads=10
source.s1.netty.idle=60sec
source.s1.netty.decoder=lineDecoder
```

| key           | default       | type        | describe                                                                                                                                                                                                                         |
|---------------|---------------|-------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| netty.host    | null          | string      | the host to bind, default null means bind \*, one port can bind multi host split by`,`, localhost means bind loop back address, 10\\.103\\.1\\..* means bind the first InetAddress on the machine matching start with 10.103.1.* |   
| netty.port    |               | int(number) | the port to bind, range 1000-65535                                                                                                                                                                                               |
| netty.threads | 10            | int(number) | the count of netty event loop threads                                                                                                                                                                                            |
| netty.idle    | 120sec        | duration    | the idle time to close the socket                                                                                                                                                                                                |
| netty.decoder | lengthDecoder | enum        | the parser of network streaming                                                                                                                                                                                                  |

### Netty Decoder

The `netty.decoder` configuration is used to parse the streaming records received by the Netty,
Supported decoders are as follows:

#### lineDecoder

The lineDecoder parses streaming records line by line, making it more suitable for scenarios
where telnet is used for demonstrations.

#### lengthDecoder

The lengthDecoder parses streaming records by length-value like below, making it more suitable
for most scenarios.
![image](picture/line_decoder.png)

Tributary provide the lengthDecoder java
client [LengthDecoderClient](../sample-code/src/main/java/org/zicat/tributary/demo/client/LengthDecoderClient.java)
for reference.

#### [kafkaDecoder](#kafkaDecoder)

The kafkaDecoder parses streaming records
by [kafka-producer-protocol](https://kafka.apache.org/protocol#The_Messages_Produce). It supports
more configuration as follows:

| key                                                | default     | type        | describe                                                                                                                                                                                                        |
|----------------------------------------------------|-------------|-------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| netty.decoder.kafka.cluster.id                     | ${sourceId} | string      | the kafka cluster to response when the client send [MetadataRequest](https://kafka.apache.org/protocol#The_Messages_Metadata)                                                                                   |
| netty.decoder.kafka.zookeeper.connect              |             | string      | the zookeeper host path to register the tributary instance for instance discovery, like: localhost:2181,localhost:2182/tributary_source_kafka                                                                   |
| netty.decoder.kafka.zookeeper.connection.timeout   | 15sec       | duration    | the zookeeper client connection timeout                                                                                                                                                                         |
| netty.decoder.kafka.zookeeper.session.timeout      | 60sec       | duration    | the zookeeper client session timeout                                                                                                                                                                            |
| netty.decoder.kafka.zookeeper.retry.times          | 3           | int(number) | the zookeeper client retry times when call zookeeper server exception                                                                                                                                           |
| netty.decoder.kafka.zookeeper.fail.base.sleep.time | 1sec        | duration    | the zookeeper client sleep time before next retry                                                                                                                                                               |
| netty.decoder.kafka.meta.ttl                       | 10sec       | duration    | the zookeeper nodes cache ttl                                                                                                                                                                                   |
| netty.decoder.kafka.topic.partitions               | 60          | int(number) | the topic partitions to response when the client send [MetadataRequest](https://kafka.apache.org/protocol#The_Messages_Metadata), recommend to set as multiples of tributary instances count for load balancing |
| netty.decoder.kafka.sasl.plain                     | false       | bool        | the security switch whether open sasl plain                                                                                                                                                                     |
| netty.decoder.kafka.sasl.plain.usernames           | null        | string      | the plain users configuration, config multi user-password pairs splitting by `,`, user-password is split by `_`, like user1_16Ew658jjzvmxDqk,user2_bbbb,user3_cccc                                              |

#### [httpDecoder](#httpDecoder)

The httpDecoder parses streaming records by http protocol. It supports more configuration as
follows:

| key                                   | default | type   | describe                                                                                                                   |
|---------------------------------------|---------|--------|----------------------------------------------------------------------------------------------------------------------------|
| netty.decoder.http.path               | null    | string | the http path to match, if null means match any path. If the http path not matched, will return http 400 code(bad request) |
| netty.decoder.http.content.length.max | 16mb    | int    | the limited content length, the http body over this will refused                                                           |

The http request demo as follows, before send request please start tributary
with [http source config](../sample-code/src/main/resources/application-http-source-print-sink.properties)
first:

```shell
$ curl -X POST http://localhost:8200?topic=my_topic&partition=10 \
     -H "Content-Type: application/json; charset=UTF-8" \
     -H "my_records_header: hv1" \
     -d '[{"key":"key1","value":"value1","headers":{"header1":"value1","header2":"value2"}}]' -i
```

## Channel

A channel is like a data stream that can append records and allow sinks to consume data
independently.

Some types of channels such as file channel support data persistence.

Tributary supports defining multiple channels in the application.properties

```properties
channel.c1.type=file
channel.c1.groups=group_1,group_2
channel.c1.partitions=/tmp/tributary/p1,/tmp/tributary/p3
channel.c1.compression=snappy
channel.c1.block.size=32kb
channel.c1.segment.size=4gb
channel.c1.flush.period=1sec
channel.c1.block.cache.per.partition.size=1024
channel.c1.groups.persist.period=40s
channel.c2.type=memory
channel.c2.groups=group_2
channel.c2.partitions=2
channel.c2.compression=snappy
channel.c2.block.size=32kb
channel.c2.segment.size=4gb
channel.c2.flush.period=1sec
channel.c2.block.cache.per.partition.size=1024
```

### Common Config

| key    | default | type              | describe                                                       |
|--------|---------|-------------------|----------------------------------------------------------------|
| type   | file    | enum[file,memory] | the implement type of the channel, only support file or memory | 
| groups |         | string            | the group id list that consume this channel, split by`,`       | 

### File Config

| key                            | default      | type                   | describe                                                                                                                                                                                       |
|--------------------------------|--------------|------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| block.size                     | 32kb         | bytes                  | the block size to store records in memory                                                                                                                                                      |
| compression                    | none         | enum[none,zstd,snappy] | the type of compression to compress the block before writing block to page cache                                                                                                               |
| segment.size                   | 4gb          | bytes                  | the size of a segment, in file and memory channel segment is the smallest unit of resource recycling                                                                                           |
| partitions                     |              | string                 | the directory list to store records, each directory represent one partition, the directory is allowed reading and writing, split by `,`                                                        |
| flush.period                   | 500ms        | duration               | the period time to async flush page cache to disk                                                                                                                                              |
| groups.persist.period          | 30s          | duration               | the period time to async persist the committed group offset to disk                                                                                                                            |     
| block.cache.per.partition.size | 1024         | long(number)           | the block count in cache per partition, the newest blocks are cached in memory before compression for sinks read channel data directly without decompression if channel compression is turn on | 
| append.sync.await              | false        | bool                   | the switch whether wait the append function put data to page cache successfully, default false means return directly only the append function  put data to block successfully                  |
| append.sync.await.timeout      | flush.period | duration               | the timeout if append sync await is open, the default equal flush.period                                                                                                                       |

### Memory Config

| key                            | default | type                   | describe                                                                                                                                                                                       |
|--------------------------------|---------|------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| block.size                     | 32kb    | bytes                  | the block size to store records in memory                                                                                                                                                      |
| compression                    | none    | enum[none,zstd,snappy] | the type of compression to compress the block before writing block to page cache                                                                                                               |
| segment.size                   | 4gb     | bytes                  | the size of a segment, in file and memory channel segment is the smallest unit of resource recycling                                                                                           |
| partitions                     | 1       | int(number)            | the number of partitions                                                                                                                                                                       |
| block.cache.per.partition.size | 1024    | long(number)           | the block count in cache per partition, the newest blocks are cached in memory before compression for sinks read channel data directly without decompression if channel compression is turn on | 

Note:

1. Please configure a reasonable block.size like 32kb in the file channel. A value that is too small
   may increase the iops of the disk.
2. Please configure a reasonable segment.size like 4gb in the file channel. A value that is too
   small
   will cause frequent creation and deletion of files, while a value that is too large will affect
   the duration of expired data retention.
3. When defining multiple file channels, please make sure to set different partition directories.
   Setting the same directory may cause unknown exceptions.
4. Strongly recommend using file channel in production environment, memory channel more for
   debugging.

## Sink

Tributary supports defining multiple sinks in the application.properties, each sink config is
started by `sink.{group_id}.*`. The {group_id} must be used in `channel.{channel_id}.groups=` .

```properties
sink.group_1.function.id=hdfs
sink.group_1.partition.retain.max.bytes=100gb
sink.group_2.function.id=kafka
sink.group_2.partition.retain.max.bytes=100gb
sink.group_2.partition.concurrent=3
sink.group_3.function.id=hbase
sink.group_3.partition.retain.max.bytes=100gb
sink.group_4.function.id=elasticsearch
sink.group_4.partition.retain.max.bytes=100gb
``` 

### Common Config

| key                           | default | type                                       | describe                                                                                                                                                                                                                               |
|-------------------------------|---------|--------------------------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| function.id                   |         | enum[print,kafka,hdfs,hbase,elasticsearch] | the function identity that configure how to consume records, user can customize functions by  implements [FunctionFactory](../tributary-sink/tributary-sink-base/src/main/java/org/zicat/tributary/sink/function/FunctionFactory.java) |
| partition.retain.max.bytes    |         | bytes                                      | the max retain bytes of each partition. When the sink lag is over, the oldest segment will be deleted, the param may cause data lost, be careful                                                                                       |
| partition.group.commit.period | 30sec   | duration                                   | the period to commit consume group id                                                                                                                                                                                                  |
| partition.concurrent          | 1       | int(number)                                | the threads to consume one partition data from channel, multi threads will cause data disorder in one partition, be careful                                                                                                            |  

### Sink HDFS

```properties
sink.group_1.sink.path=/tmp/test/cache
sink.group_1.roll.size=10mb
sink.group_1.bucket.date.format=yyyyMMdd_HH
sink.group_1.bucket.date.timezone=GMT+8
sink.group_1.max.retries=3
sink.group_1.retry.interval=200ms
sink.group_1.keytab=
sink.group_1.principle=
sink.group_1.writer.identity=parquet
sink.group_1.writer.parquet.compression.codec=snappy
sink.group_1.idle.trigger=60sec
```

| key                              | default     | type        | describe                                                                                                                                                                                |
|----------------------------------|-------------|-------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| sink.path                        |             | string      | the root path to sink                                                                                                                                                                   |
| roll.size                        | 256mb       | bytes       | the max size of the file                                                                                                                                                                |
| bucket.date.format               | yyyyMMdd_HH | string      | the part of the bucket, the bucket is composed of ${sink.path}/${bucketDateFormat}/                                                                                                     |   
| bucket.date.timezone             | UTC         | string      | the timezone of bucket date format                                                                                                                                                      | 
| max.retries                      | 3           | int(number) | the max retry times when operate hdfs fail                                                                                                                                              |
| retry.interval                   | 200ms       | duration    | the interval between 2 retries                                                                                                                                                          |
| keytab                           |             | string      | the keytab if hdfs use kerberos authenticator                                                                                                                                           |
| principle                        |             | string      | the principle if hdfs use kerberos authenticator                                                                                                                                        |
| writer.identity                  | parquet     | string      | the spi implement id of the interface [HDFSRecordsWriterFactory](../tributary-sink/tributary-sink-hdfs/src/main/java/org/zicat/tributary/sink/hdfs/HDFSRecordsWriterFactory.java)       |
| writer.parquet.compression.codec | snappy      | string      | the compression type in org.apache.parquet.hadoop.metadata.CompressionCodecName, default snappy                                                                                         | 
| idle.trigger                     | 30sec       | duration    | the idle time to trigger the idleTrigger() function if function implement [Trigger](../tributary-sink/tributary-sink-base/src/main/java/org/zicat/tributary/sink/function/Trigger.java) |

[more details](../tributary-sink/tributary-sink-hdfs/README.md)

### Sink Kafka

```properties
sink.group_2.topic=test_topic
sink.group_2.kafka.bootstrap.servers=127.0.0.1:9092
sink.group_2.kafka.buffer.memory=134217728
sink.group_2.kafka.linger.ms=1000
sink.group_2.kafka.batch.size=524288
sink.group_2.kafka.compression.type=snappy
```

| key   | default | type   | describe                 |
|-------|---------|--------|--------------------------|
| topic |         | string | the n ame of kafka topic |

Tributary support all kafka producer params,
[apache kafka producer config](https://kafka.apache.org/documentation/#producerconfigs) .

Note:

1. The value of keys `key.serializer` and `value.serializer` is always
   `org.apache.kafka.common.serialization.ByteArraySerializer`. Setting value to others is ignored.

2. The value of param `topic` support var `${topic}` like dispatcher_proxy_sink_${topic} to sink data to different topic
   depend on the record's topic.

### Sink HBase

```properties
sink.group_3.hbase-site-xml.path=/tmp/hbase-site.xml
sink.group_3.table.name=table_test
sink.group_3.table.family.name=info
sink.group_3.table.column.value.name=value
sink.group_3.table.column.topic.name=topic
sink.group_3.table.column.head.name.prefix=head_
```

| key                           | default | type   | describe                                                                                                                                    |
|-------------------------------|---------|--------|---------------------------------------------------------------------------------------------------------------------------------------------|
| hbase-site-xml.path           | null    | string | the hbase-site.xml path, supports file path or classpath, null means load files named `hbase-default.xml` and `hbase-site.xml` in classpath |
| table.name                    |         | string | the hbase table to write                                                                                                                    |
| table.family.name             | info    | string | the hbase table column family                                                                                                               |
| table.column.value.name       | value   | string | the hbase table column name for storing value                                                                                               |
| table.column.topic.name       | topic   | string | the hbase table column name for storing topic                                                                                               |
| table.column.head.name.prefix | head_   | string | the hbase table column name prefix for storing headers, the real column name is {table.column.head.name.prefix}+{head.key}                  |

Note:

1. HbaseSink
   require [Record](../tributary-common/src/main/java/org/zicat/tributary/common/records/Record.java) has key.

   The record without key will be filtered. User can watch how many records are
   filtered by metrics key `sink_hbase_discard_counter`.

2. The key-supported sources
   include [kafkaDecoder](#kafkadecoder) and [httpDecoder](#httpDecoder).

### Sink ElasticSearch

```properties
sink.group_4.hosts=http://localhost:9200
sink.group_4.path-prefix=
sink.group_4.index=my_index
sink.group_4.compression=true
sink.group_4.username=
sink.group_4.password=
sink.group_4.request.timeout=30s
sink.group_4.connection.timeout=10s
sink.group_4.socket.timeout=20s
sink.group_4.request.indexer.identity=default
sink.group_4.async.bulk.queue.size=1024
sink.group_4.async.bulk.queue.await.timeout=30s
sink.group_4.idle.trigger=30s
```

| key                            | default | type     | describe                                                                                                                                                      |
|--------------------------------|---------|----------|---------------------------------------------------------------------------------------------------------------------------------------------------------------|
| hosts                          |         | string   | the elasticsearch hosts, split by `;`                                                                                                                         |
| path-prefix                    | null    | string   | the elasticsearch restful api path prefix, default null                                                                                                       |
| index                          |         | string   | the elasticsearch index to sink                                                                                                                               |
| compression                    | true    | bool     | whether compress the index request, default true                                                                                                              |
| username                       | null    | string   | the username of elasticsearch, default null                                                                                                                   |
| password                       | null    | string   | the password of elasticsearch, default null                                                                                                                   |
| request.timeout                | 30s     | duration | the timeout for requesting a connection from the connection manager, default 30s                                                                              |
| connection.timeout             | 10s     | duration | the timeout for establishing a connection, default 10s                                                                                                        |
| socket.timeout                 | 20s     | duration | the socket timeout (SO_TIMEOUT) for waiting for data, a maximum period inactivity between two consecutive data packets, default 20s                           |
| async.bulk.queue.size          | 1024    | int      | the size of the queue which contains async bulk insert listener callback instances                                                                            |
| async.bulk.queue.await.timeout | 30s     | duration | throw exception if wait timeout to put instance to queue over this param                                                                                      |
| idle.trigger                   | 30s     | duration | set idle trigger to call trigger callback function, for elasticsearch, sink instance will deal with top finished requests callback instances in the queue     |
| request.indexer                | default | string   | the spi identity of [RequestIndexer](../tributary-sink/tributary-sink-elasticsearch/src/main/java/org/zicat/tributary/sink/elasticsearch/RequestIndexer.java) |

Note:

1. For the default of request.indexer, the value of records must be a string object json. The records will be discarded
   if not, user can watch how many records are filtered by metrics key `sink_elasticsearch_discard_counter`.

2. For the default of request.indexer, the headers of the record will also be stored in the elasticsearch. The header
   will be discarded if the key of header is contained in value json object keys.

3. For the default of request.indexer, the topic value of the record will be stored in the elasticsearch by key
   `_topic`.

4. For the default of request.indexer, the key value of the record will be store in the elasticsearch by id if the key
   exists.

## The complete demo config

```
server.port=8765
server.host=.*

source.s1.channel=c1
source.s1.implement=netty
source.s1.netty.port=8200
source.s1.netty.threads=10
source.s1.netty.idle=60sec

source.s2.channel=c1
source.s2.implement=netty
source.s2.netty.port=8300
source.s2.netty.threads=5
source.s2.netty.idle=120sec
source.s2.netty.decoder=lineDecoder

source.s3.channel=c2
source.s3.implement=netty
source.s3.netty.host=localhost
source.s3.netty.port=9093
source.s3.netty.idle=60sec
source.s3.netty.threads=10
source.s3.netty.decoder=kafkaDecoder
source.s3.netty.decoder.kafka.meta.ttl=10sec
source.s3.netty.decoder.kafka.topic.partitions=60
source.s3.netty.decoder.kafka.zookeeper.connect=localhost:2181/tributary_source_kafka
source.s3.netty.decoder.kafka.zookeeper.retry.times=3
source.s3.netty.decoder.kafka.zookeeper.fail.base.sleep.time=1sec
source.s3.netty.decoder.kafka.zookeeper.connection.timeout=15sec
source.s3.netty.decoder.kafka.zookeeper.session.timeout=60sec
source.s3.netty.decoder.kafka.sasl.plain=true
source.s3.netty.decoder.kafka.sasl.plain.usernames=user1_16Ew658jjzvmxDqk,user2_bbbb,user3_cccc

channel.c1.type=file
channel.c1.partitions=/tmp/tributary/p1,/tmp/tributary/p3
channel.c1.groups=group_1,group_2,group_3,group_4
channel.c1.compression=snappy
channel.c1.block.size=32kb
channel.c1.segment.size=4gb
channel.c1.flush.period=1sec

channel.c2.type=memory
channel.c2.partitions=2
channel.c2.groups=group_2
channel.c2.compression=snappy
channel.c2.block.size=32kb
channel.c2.segment.size=4gb
channel.c2.flush.period=1sec

sink.group_1.partition.retain.max.bytes=100gb
sink.group_1.partition.group.commit.period=30sec
sink.group_1.function.id=hdfs
sink.group_1.sink.path=/tmp/test/cache
sink.group_1.roll.size=10mb
sink.group_1.bucket.date.format=yyyyMMdd_HH
sink.group_1.max.retries=3
sink.group_1.retry.interval=200ms
sink.group_1.keytab=
sink.group_1.principle=
sink.group_1.writer.identity=parquet
sink.group_1.writer.parquet.compression.codec=snappy
sink.group_1.idle.trigger=60sec

sink.group_2.partition.retain.max.bytes=100gb
sink.group_2.partition.group.commit.period=30sec
sink.group_2.partition.concurrent=3
sink.group_2.function.id=kafka
sink.group_2.kafka.bootstrap.servers=127.0.0.1:9092
sink.group_2.topic=proxy_sink_${topic}
sink.group_2.kafka.buffer.memory=134217728
sink.group_2.kafka.linger.ms=1000
sink.group_2.kafka.batch.size=524288
sink.group_2.kafka.compression.type=snappy

sink.group_3.partition.retain.max.bytes=100gb
sink.group_3.partition.group.commit.period=30sec
sink.group_3.function.id=hbase
sink.group_3.hbase-site-xml.path=/tmp/hbase-site.xml
sink.group_3.table.name=table_test
sink.group_3.table.family.name=info
sink.group_3.table.column.value.name=value
sink.group_3.table.column.topic.name=topic
sink.group_3.table.column.head.name.prefix=head_

sink.group_4.partition.retain.max.bytes=100gb
sink.group_4.partition.group.commit.period=30sec
sink.group_4.function.id=elasticsearch
sink.group_4.hosts=http://localhost:9200
sink.group_4.path-prefix=
sink.group_4.index=my_index
sink.group_4.compression=true
sink.group_4.username=
sink.group_4.password=
sink.group_4.request.timeout=30s
sink.group_4.connection.timeout=10s
sink.group_4.socket.timeout=20s
sink.group_4.request.indexer.identity=default
sink.group_4.async.bulk.queue.size=1024
sink.group_4.async.bulk.queue.await.timeout=30s
```       
