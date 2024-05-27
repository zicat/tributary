# Tributary User Guide of Config Details

Tributary Configuration consists of four parts, including server, source, channel, and sink.

## Server

```properties
server.port=8765
server.metrics.ip.pattern=.*
```

The Tributary service provides metrics indicators in the form of http restful api through
SpringBoot, and more server.* details can be obtained through the SpringBoot document.

The parameter "server.metrics.ip.pattern" is a regular expression that is used to filter the
required hosts when there are multiple network cards in the machine, and assign the value to the
host dimension in the metrics.

Get tributary metrics as follows, ensure that the port matches the server.port configuration:

![image](picture/metrics_url.png)

| Key                       | default | valid value | describe                                                                   |
|---------------------------|---------|-------------|----------------------------------------------------------------------------|
| server.port               |         | int(number) | the port to bind, range 1000-65535                                         |
| server.metrics.ip.pattern | .*      | string      | the pattern to filter expected host as the metrics value of host dimension |

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

The source must config the implement to get and parse data.

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
source.s1.netty.idle.second=60
source.s1.netty.decoder=lineDecoder
```

| key               | default       | type                                         | describe                                                                                                                                                                                                                         |
|-------------------|---------------|----------------------------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| netty.host        | null          | string                                       | the host to bind, default null means bind \*, one port can bind multi host split by`,`, localhost means bind loop back address, 10\\.103\\.1\\..* means bind the first InetAddress on the machine matching start with 10.103.1.* |   
| netty.port        |               | int(number)                                  | the port to bind, range 1000-65535                                                                                                                                                                                               |
| netty.threads     | 10            | int(number)                                  | the count of netty event loop threads                                                                                                                                                                                            |
| netty.idle.second | 120           | int(second)                                  | the idle second of the channel to close                                                                                                                                                                                          |
| netty.decoder     | lengthDecoder | enum[lengthDecoder,lineDecoder,kafkaDecoder] | the parser of network streaming                                                                                                                                                                                                  |

Note：

1. Different netty sources must use different value of netty.port.

2. The lineDecoder parses streaming records line by line, making it more suitable for scenarios
   where telnet is used for demonstrations.

3. The lengthDecoder parses streaming records by length-value like below, making it more suitable
   for most scenarios.

   ![image](picture/line_decoder.png)

   Tributary provide the lengthDecoder java
   client [LengthDecoderClient](../tributary-source/src/main/java/org/zicat/tributary/source/netty/client/LengthDecoderClient.java)
   for reference.

4. The kafkaDecoder parses streaming records
   by [kafka-producer-protocol](https://kafka.apache.org/protocol#The_Messages_Produce). The
   kafkaDecoder support more configuration as follows:

| key                                                   | default     | type        | describe                                                                                                                                                                                                        |
|-------------------------------------------------------|-------------|-------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| netty.decoder.kafka.cluster.id                        | ${sourceId} | string      | the kafka cluster to response when the client send [MetadataRequest](https://kafka.apache.org/protocol#The_Messages_Metadata)                                                                                   |
| netty.decoder.kafka.zookeeper.connect                 |             | string      | the zookeeper host path to register the tributary instance for instance discovery, like: localhost:2181,localhost:2182/tributary_source_kafka                                                                   |
| netty.decoder.kafka.zookeeper.connection.timeout.ms   | 15000       | int(ms)     | the zookeeper client connection timeout                                                                                                                                                                         |
| netty.decoder.kafka.zookeeper.session.timeout.ms      | 60000       | int(ms)     | the zookeeper client session timeout                                                                                                                                                                            |
| netty.decoder.kafka.zookeeper.retry.times             | 3           | int(number) | the zookeeper client retry times when call zookeeper server exception                                                                                                                                           |
| netty.decoder.kafka.zookeeper.fail.base.sleep.time.ms | 1000        | int(ms)     | the zookeeper client sleep time before next retry                                                                                                                                                               |
| netty.decoder.kafka.meta.ttl.ms                       | 10000       | long(ms)    | the zookeeper nodes cache ttl                                                                                                                                                                                   |
| netty.decoder.kafka.topic.partitions                  | 60          | int(number) | the topic partitions to response when the client send [MetadataRequest](https://kafka.apache.org/protocol#The_Messages_Metadata), recommend to set as multiples of tributary instances count for load balancing |
| netty.decoder.kafka.sasl.plain                        | false       | bool        | the security switch whether open sasl plain                                                                                                                                                                     |
| netty.decoder.kafka.sasl.plain.usernames              | null        | string      | the plain users configuration, config multi user-password pairs splitting by `,`, user-password is split by `_`, like user1_16Ew658jjzvmxDqk,user2_bbbb,user3_cccc                                              |

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
channel.c1.block.size=262144
channel.c1.segment.size=4294967296
channel.c1.flush.period.mills=1000
channel.c1.block.cache.per.partition.size=1024
channel.c1.groups.persist.period.second=40
channel.c2.type=memory
channel.c2.groups=group_2
channel.c2.partitions=2
channel.c2.compression=snappy
channel.c2.block.size=262144
channel.c2.segment.size=4294967296
channel.c2.flush.period.mills=1000
channel.c2.block.cache.per.partition.size=1024
channel.c3.type=kafka
channel.c3.groups=group_3
channel.c3.partitions=4
channel.c3.topicDir=/tmp/tributary/topic_dir
channel.c3.kafka.bootstrap.servers=127.0.0.1:9092
```

### Common Config

| key    | default | type                    | describe                                                                |
|--------|---------|-------------------------|-------------------------------------------------------------------------|
| type   | file    | enum[file,memory,kafka] | the implement type of the channel, only support file or memory or kafka | 
| groups |         | string                  | the group list that consume this channel, split by`,`                   | 

### File Config

| key                            | default        | type                   | describe                                                                                                                                                                                       |
|--------------------------------|----------------|------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| block.size                     | 32768(32K)     | long(byte)             | the block size to store records in memory                                                                                                                                                      |
| compression                    | none           | enum[none,zstd,snappy] | the type of compression to compress the block before writing block to page cache                                                                                                               |
| segment.size                   | 4294967296(4G) | long(byte)             | the size of a segment, in file and memory channel segment is the smallest unit of resource recycling                                                                                           |
| partitions                     |                | string                 | the directory list to store records, each directory represent one partition, the directory is allowed reading and writing, split by `,`                                                        |
| flush.period.mills             | 500            | long(ms)               | the period time to async flush page cache to disk                                                                                                                                              |
| groups.persist.period.second   | 30             | long(second)           | the period time to async persist the committed group offset to disk                                                                                                                            |     
| block.cache.per.partition.size | 1024           | long(number)           | the block count in cache per partition, the newest blocks are cached in memory before compression for sinks read channel data directly without decompression if channel compression is turn on | 

### Memory Config

| key                            | default        | type                   | describe                                                                                                                                                                                       |
|--------------------------------|----------------|------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| block.size                     | 32768(32K)     | long(byte)             | the block size to store records in memory                                                                                                                                                      |
| compression                    | none           | enum[none,zstd,snappy] | the type of compression to compress the block before writing block to page cache                                                                                                               |
| segment.size                   | 4294967296(4G) | long(byte)             | the size of a segment, in file and memory channel segment is the smallest unit of resource recycling                                                                                           |
| partitions                     | 1              | int(number)            | the number of partitions                                                                                                                                                                       |
| block.cache.per.partition.size | 1024           | long(number)           | the block count in cache per partition, the newest blocks are cached in memory before compression for sinks read channel data directly without decompression if channel compression is turn on | 

### Kafka Config

| key        | default | type        | describe                                                                                                                                                                                                                             |
|------------|---------|-------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| partitions | 1       | int(number) | the number of partitions                                                                                                                                                                                                             |
| topicDir   |         | string      | the directory to restore kafka topic, each tributary instance create diff kafka topic name using channel topic as prefix and store it in the file ${topicDir}/${channel_topic}, read it again after restart and reconsume this topic | 
| kafka.*    |         | string      | the config of all [kafka properties](https://kafka.apache.org/26/documentation.html#configuration)                                                                                                                                   |        

Note:

1. Please configure a reasonable block.size in the file channel. A value that is too small may
   increase the IOPS of the disk.
2. Please configure a reasonable segment.size in the file channel. A value that is too small will
   cause frequent creation and deletion of files, while a value that is too large will affect the
   duration of expired data retention.
3. When defining multiple file channels, please make sure to set different partition directories.
   Setting the same directory may cause unknown exceptions.
4. Setting some kafka properties is not work including key.deserializer, value.deserializer,
   key.serializer, value.serializer, group.id, enable.auto.commit.
5. Strongly recommend using file channel in production environment, memory channel, and kafka
   channel more for debugging.

## Sink

Tributary supports defining multiple sinks in the application.properties

```properties
sink.group_1.partition.retain.max.bytes=9663676414
sink.group_1.partition.handler.id=direct
sink.group_1.function.id=hdfs
sink.group_2.partition.retain.max.bytes=9663676414
sink.group_2.partition.handler.id=multi_thread
sink.group_2.threads=3
sink.group_2.function.id=kafka
``` 

### Common Config

| key                        | default | type                      | describe                                                                                                                                            |
|----------------------------|---------|---------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------|
| partition.retain.max.bytes |         | long(byte)                | the max retain bytes of each partition. When the sink lag is over, the oldest segment will be deleted, the param may cause data lost, be careful    |
| partition.handler.id       | direct  | enum[direct,multi_thread] | the sink model, direct model combine one channel's partition with one thread, multi_thread model combine one channel's partition with multi threads |
| threads                    | 2       | int(number)               | the thread count, only valid when the value of partition.handler.id is multi_thread                                                                 |  
| function.id                |         | enum[print,kafka,hdfs]    | the function identity that configure how to consume records                                                                                         |

Note:

1. User can customize functions by
   implements [FunctionFactory](../tributary-sink/tributary-sink-base/src/main/java/org/zicat/tributary/sink/function/FunctionFactory.java)

### Sink HDFS Detail

```properties
sink.group_1.sink.path=/tmp/test/cache
sink.group_1.roll.size=10240000
sink.group_1.bucket.date.format=yyyyMMdd_HH
sink.group_1.bucket.date.timezone=GMT+8
sink.group_1.max.retries=3
sink.group_1.keytab=
sink.group_1.principle=
sink.group_1.output.compression.codec=
sink.group_1.idle.trigger.millis=60000
```

| key                      | default         | type         | describe                                                                                                                                                                                |
|--------------------------|-----------------|--------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| sink.path                |                 | string       | the root path to sink                                                                                                                                                                   |
| roll.size                | 268435456(256M) | long(byte)   | the max size of the file                                                                                                                                                                |
| bucket.date.format       | yyyyMMdd_HH     | string       | the part of the bucket, the bucket is composed of ${sink.path}/${bucketDateFormat}/                                                                                                     |   
| bucket.date.timezone     | UTC             | string       | the timezone of bucket date format                                                                                                                                                      | 
| max.retries              | 3               | int(number)  | the max retry times when operate hdfs fail                                                                                                                                              |
| keytab                   |                 | string       | the keytab if hdfs use kerberos authenticator                                                                                                                                           |
| principle                |                 | string       | the principle if hdfs use kerberos authenticator                                                                                                                                        |
| output.compression.codec | null            | string       | the compression class that implement org.apache.hadoop.io.compress.CompressionCodec, e.g. org.apache.hadoop.io.compress.SnappyCodec                                                     | 
| idle.trigger.millis      | 60000           | long(millis) | the idle time to trigger the idleTrigger() function if function implement [Trigger](../tributary-sink/tributary-sink-base/src/main/java/org/zicat/tributary/sink/function/Trigger.java) |

[GOTO HDFS Sink for more details](../tributary-sink/tributary-sink-hdfs/README.md)

### Sink Kafka Detail

```properties
sink.group_2.decoder.identity=default
sink.group_2.topic=test_topic
sink.group_2.kafka.bootstrap.servers=127.0.0.1:9092
sink.group_2.kafka.buffer.memory=134217728
sink.group_2.kafka.linger.ms=1000
sink.group_2.kafka.batch.size=524288
sink.group_2.kafka.compression.type=snappy
sink.group_2.kafka.flushMill=60000
```

| key              | default | type                             | describe                                                                                                                                                                                              |
|------------------|---------|----------------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| decoder.identity | default | enum[default,kafkaRecordDecoder] | the decoder id to parse data from the channel, the spi interface is [Byte2RecordFactory](../tributary-sink/tributary-sink-kafka/src/main/java/org/zicat/tributary/sink/kafka/Byte2RecordFactory.java) |
| topic            |         | string                           | the name of kafka topic                                                                                                                                                                               |

Tributary support all kafka producer params,
[apache kafka producer config](https://kafka.apache.org/documentation/#producerconfigs) .

Note:

1. The value of keys `key.serializer` and `value.serializer` is always
   `org.apache.kafka.common.serialization.ByteArraySerializer`. Setting value to other is ignored.

2. The decoder identity of kafkaRecordDecoder only support to consume the channel data
   writing by the source netty decoder of kafkaDecoder.

3. The topic support param of ${topic} like dispatcher_proxy_sink_${topic} to sink data to specify
   topic use setting from kafka client when The decoder identity is kafkaRecordDecoder.

## The complete demo config

```
server.port=8765
server.metrics.ip.pattern=.*

source.s1.channel=c1
source.s1.implement=netty
source.s1.netty.port=8200
source.s1.netty.threads=10
source.s1.netty.idle.second=60

source.s2.channel=c2
source.s2.implement=netty
source.s2.netty.port=8300
source.s2.netty.threads=5
source.s2.netty.idle.second=120
source.s2.netty.decoder=lineDecoder

source.s3.channel=c3
source.s3.implement=netty
source.s3.netty.host=localhost
source.s3.netty.port=9093
source.s3.netty.idle.second=60
source.s3.netty.threads=10
source.s3.netty.decoder=kafkaDecoder
source.s3.netty.decoder.kafka.meta.ttl.ms=10000
source.s3.netty.decoder.kafka.topic.partitions=60
source.s3.netty.decoder.kafka.zookeeper.connect=localhost:2181/tributary_source_kafka
source.s3.netty.decoder.kafka.zookeeper.retry.times=3
source.s3.netty.decoder.kafka.zookeeper.fail.base.sleep.time.ms=1000
source.s3.netty.decoder.kafka.zookeeper.connection.timeout.ms=15000
source.s3.netty.decoder.kafka.zookeeper.session.timeout.ms=60000
source.s3.netty.decoder.kafka.sasl.plain=true
source.s3.netty.decoder.kafka.sasl.plain.usernames=user1_16Ew658jjzvmxDqk,user2_bbbb,user3_cccc

channel.c1.type=file
channel.c1.partitions=/tmp/tributary/p1,/tmp/tributary/p3
channel.c1.groups=group_1,group_2
channel.c1.compression=snappy
channel.c1.block.size=262144
channel.c1.segment.size=4294967296
channel.c1.flush.period.mills=1000

channel.c2.type=memory
channel.c2.partitions=2
channel.c2.groups=group_2
channel.c2.compression=snappy
channel.c2.block.size=262144
channel.c2.segment.size=4294967296
channel.c2.flush.period.mills=1000

channel.c3.type=memory
channel.c3.groups=group_3
channel.c3.compression=snappy
channel.c3.block.size=262144
channel.c3.segment.size=4294967296
channel.c3.flush.period.mills=1000

sink.group_1.partition.retain.max.bytes=9663676414
sink.group_1.partition.handler.id=direct
sink.group_1.function.id=hdfs
sink.group_1.sink.path=/tmp/test/cache
sink.group_1.roll.size=10240000
sink.group_1.bucket.date.format=yyyyMMdd_HH
sink.group_1.max.retries=3
sink.group_1.keytab=
sink.group_1.principle=
sink.group_1.idle.trigger.millis=60000

sink.group_2.partition.retain.max.bytes=9663676414
sink.group_2.partition.handler.id=multi_thread
sink.group_2.threads=3
sink.group_2.function.id=kafka
sink.group_2.decoder.identity=default
sink.group_2.kafka.bootstrap.servers=127.0.0.1:9092
sink.group_2.topic=test_topic
sink.group_2.kafka.buffer.memory=134217728
sink.group_2.kafka.linger.ms=1000
sink.group_2.kafka.batch.size=524288
sink.group_2.kafka.compression.type=snappy
sink.group_2.kafka.flushMill=60000

sink.group_3.partition.retain.max.bytes=9663676414
sink.group_3.partition.handler.id=direct
sink.group_3.function.id=kafka
sink.group_3.decoder.identity=kafkaRecordDecoder
sink.group_3.topic=dispatcher_proxy_sink_${topic}
sink.group_3.kafka.bootstrap.servers=127.0.0.1:9092
sink.group_3.kafka.buffer.memory=134217728
sink.group_3.kafka.linger.ms=1000
sink.group_3.kafka.batch.size=524288
sink.group_3.kafka.compression.type=snappy
sink.group_3.kafka.flushMill=60000
```       