# Tributary User Guide of Config Details

Tributary Configuration consists of four parts, including server, source, channel, and sink.

## Server Detail

```properties
server.port=8765
server.metrics.ip.pattern=.*
```

The Tributary service provides metrics indicators in the form of http restful api through SpringBoot, and more server.* details can be obtained through the SpringBoot document.

The parameter "server.metrics.ip.pattern" is a regular expression that is used to filter the required hosts when there are multiple network cards in the machine, and assign the value to the host dimension in the metrics.

Get tributary metrics as follows, ensure that the port matches the server.port configuration:

![image](picture/metrics_url.png)

|  Key                          |  default       | valid value                  | describe                                             |
|  ----                         | ----           | ---                          | ---                                                  |
| server.port                   |                | int(unit: number)                  | the port to bind, range 1000-65535                                         |
| server.metrics.ip.pattern     | .*             | string                | the pattern to filter expected host as the metrics value of host dimension      |

## Source Detail

Tributary supports defining multiple sources in the application.properties

Each source must be associated with a channel and its implementation as follows

```properties
source.s1.channel=c1
source.s1.implement=netty
source.s2.channel=c2
source.s2.implement=netty
``` 

Above are defined two sources named s1 and s2, in which s1 is bound to channel c1 and s2 is bound to channel c2, and their implementation is both using netty.

The source must config the implement to get and parse data.

Tributary provide the
[SourceFactory](../tributary-source/src/main/java/org/zicat/tributary/source/SourceFactory.java)
interface that supports the development of specific sources scenarios.

Tributary also provides the default implementation
[netty](../tributary-source/src/main/java/org/zicat/tributary/source/netty/DefaultNettySourceFactory.java), which supports receiving data from the network. The configuration parameters for Netty are as follows :

```properties
source.s1.netty.port=8200
source.s1.netty.threads=10
source.s1.netty.idle.second=60
source.s1.netty.decoder=lineDecoder
```

|  key              |  default       | type                  | describe                                             |
|  ----             | ----           | ---                          | ---                                                  |
| netty.port        |                | int(unit: number)                   | the port to bind, range 1000-65535                                        |
| netty.threads     | 10             | int(unit: number)                   | the count of netty event loop threads                             |
| netty.idle.second | 120            | int(unit: second)                   | the idle second of the channel to close |
| netty.decoder     | lengthDecoder  | enum[lengthDecoder,lineDecoder]  |  the parser of streaming                                 |

Note：

1. Different netty sources have different configurations for netty.port.
   
2. The lineDecoder parses streaming records line by line, making it more suitable for scenarios where telnet is used for demonstrations.

3. The lengthDecoder parses streaming records by length-value like below, making it more suitable for most scenarios.

   ![image](picture/line_decoder.png)

Tributary provide the lengthDecoder java client
[LengthDecoderClient](../tributary-source/src/main/java/org/zicat/tributary/source/netty/client/LengthDecoderClient.java)
for reference.

## Channel Detail

A channel is like a data stream that can append records and allow sinks to consume data independently. Some types of channels, such as file channel, support data persistence.

Tributary supports defining multiple channels in the application.properties

```properties
channel.c1.type=file
channel.c1.groups=group_1,group_2
channel.c1.partitions=/tmp/tributary/p1,/tmp/tributary/p3
channel.c1.compression=snappy
channel.c1.blockSize=262144
channel.c1.segmentSize=4294967296
channel.c1.flushPeriodMills=1000
channel.c1.groupPersistPeriodSecond=40
channel.c2.type=memory
channel.c2.groups=group_2
channel.c2.partitions=2
channel.c2.compression=snappy
channel.c2.blockSize=262144
channel.c2.segmentSize=4294967296
channel.c2.flushPeriodMills=1000
channel.c3.type=kafka
channel.c3.groups=group_3
channel.c3.partitions=4
channel.c3.topicDir=/tmp/tributary/topic_dir
channel.c3.kafka.bootstrap.servers=127.0.0.1:9092
```

### Common Config

|  key              |  default       | type                  | describe                                             |
|  ----             | ----           | ---                          | ---                                                  |
| type              | file           | enum[file,memory,kafka]          | the implement type of the channel, only support file or memory or kafka | 
| groups            |                | string                 | the group list that consume this channel, split by ',' |

### File Config

|  key              |  default       | type                  | describe                                             |
|  ----             | ----           | ---                          | ---                                                  |
| blockSize         | 32768(32K)     | long(unit: byte)       | the block size to store records in memory|
| compression       | none           | enum[none,zstd,snappy]           | the type of compression to compress the block before writing block to page cache |
| segmentSize       | 4294967296(4G) | long(unit: byte)       | the size of a segment, in file and memory channel segment is the smallest unit of resource recycling  |
| partitions        |                | string          | the directory list to store records, each directory represent one partition, the directory is allowed reading and writing, split by ','  |
| flushPeriodMills  | 500            | long(unit: ms)         | the period time to async flush page cache to disk|
| groupPersistPeriodSecond| 30       | long(unit: second)| the period time to async persist the committed group offset to disk|     


### Memory Config

|  key              |  default       | type                  | describe                                             |
|  ----             | ----           | ---                          | ---                                                  |
| blockSize         | 32768(32K)     | long(unit: byte)       | the block size to store records in memory|
| compression       | none           | enum[none,zstd,snappy]           | the type of compression to compress the block before writing block to page cache |
| segmentSize       | 4294967296(4G) | long(unit: byte)       | the size of a segment, in file and memory channel segment is the smallest unit of resource recycling  |
| partitions        | 1              | int(unit: number)      | the number of partitions|

### Kafka Config
|  key              |  default       | type                  | describe                                             |
|  ----             | ----           | ---                          | ---                                                  |
| partitions        | 1              | int(unit: number)                    | the number of partitions|
| topicDir          |                | string  | the directory to restore kafka topic, each tributary instance create diff kafka topic name using channel topic as prefix and store it in the file ${topicDir}/${channel_topic}, read it again after restart and reconsume this topic| 
| kafka.*           | | string| the config of all [kafka properties](https://kafka.apache.org/26/documentation.html#configuration) |        

Note:

1. Please configure a reasonable blockSize in the file channel. A value that is too small may increase the IOPS of the disk.
2. Please configure a reasonable segmentSize in the file channel. A value that is too small will cause frequent creation and deletion of files, while a value that is too large will affect the duration of expired data retention.
3. When defining multiple file channels, please make sure to set different partition directories. Setting the same directory may cause unknown exceptions.
4. Setting some kafka properties is not work including key.deserializer, value.deserializer, key.serializer, value.serializer, group.id, enable.auto.commit.  
5. Strongly recommend using file channel in production environment, memory channel, and kafka channel more for debugging.

## Sink Detail

Tributary supports defining multiple sinks in the application.properties

```properties
sink.group_1.maxRetainPerPartitionBytes=9663676414
sink.group_1.partitionHandlerIdentity=direct
sink.group_1.functionIdentity=hdfs
sink.group_2.maxRetainPerPartitionBytes=9663676414
sink.group_2.partitionHandlerIdentity=multi_thread
sink.group_2.threads=3
sink.group_2.functionIdentity=kafka
``` 

### Common Config

key                               |  default       | type                 | describe                                                                  |
|  ----                             | ----           | ---                          | ---                                                                       |
| maxRetainPerPartitionBytes        |                | long(unit: bytes)  | the max retain bytes of each partition. When the sink lag is over, the oldest segment will be deleted, the param may cause data lost, be careful     |
| partitionHandlerIdentity          | direct         | enum[direct,multi_thread]  | the sink model, direct model combine one channel's partition with one thread, multi_thread model combine one channel's partition with multi threads|
| threads                           | 2              | int(unit: number) | the thread count, only valid when the value of partitionHandlerIdentity is multi_thread |  
| functionIdentity                  |                | enum[print,kafka,hdfs]     | the function identity that configure how to consume records  |

Note:

1. User can customize functions by
   implements [FunctionFactory](../tributary-sink/tributary-sink-base/src/main/java/org/zicat/tributary/sink/function/FunctionFactory.java)

### Sink HDFS Detail

```properties
sink.group_1.sinkPath=/tmp/test/cache
sink.group_1.roll.size=10240000
sink.group_1.bucketDateFormat=yyyyMMdd_HH
sink.group_1.maxRetries=3
sink.group_1.keytab=
sink.group_1.principle=
sink.group_1.idleTriggerMillis=60000
```

key                               |  default       | type                 | describe    |
|  ----                             | ----           | ---                          | ---                                                                       
| sinkPath                          |                | string                   | the root path to sink |
| roll.size                         |268435456(256M) | long(unit: byte)       | the max size of the file|
| bucketDateFormat                  |yyyyMMdd_HH     | string  | the part of the bucket, the bucket is composed of ${sinkPath}/${bucketDateFormat}/ |   
| maxRetries                        |3               | int(unit: number)| the max retry times when operate hdfs fail|
| keytab                            |                | string|            the keytab if hdfs use kerberos authenticator|
| principle                         |                | string|            the principle if hdfs use kerberos authenticator|
| idleTriggerMillis                 |60000           | long(unit: millis)| the idle time to trigger the idleTrigger() function if function implement [Trigger](../tributary-sink/tributary-sink-base/src/main/java/org/zicat/tributary/sink/function/Trigger.java)|

[GOTO HDFS Sink for more details](../tributary-sink/tributary-sink-hdfs)

### Sink Kafka Detail

```properties
sink.group_2.kafka.topic=test_topic
sink.group_2.kafka.bootstrap.servers=127.0.0.1:9092
sink.group_2.kafka.key.serializer=org.apache.kafka.common.serialization.ByteArraySerializer
sink.group_2.kafka.value.serializer=org.apache.kafka.common.serialization.ByteArraySerializer
sink.group_2.kafka.buffer.memory=134217728
sink.group_2.kafka.linger.ms=1000
sink.group_2.kafka.batch.size=524288
sink.group_2.kafka.compression.type=snappy
sink.group_2.kafka.flushMill=60000
```

|key                               |  default       | type                 | describe                                                                  |
|  ----                             | ----           | ---                          | ---                                                                       |
| kafka.topic                       |                | string                 | the name of kafka topic|

Tributary support all kafka producer params,
[apache kafka producer config](https://kafka.apache.org/documentation/#producerconfigs) .

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

channel.c1.type=file
channel.c1.partitions=/tmp/tributary/p1,/tmp/tributary/p3
channel.c1.groups=group_1,group_2
channel.c1.compression=snappy
channel.c1.blockSize=262144
channel.c1.segmentSize=4294967296
channel.c1.flushPeriodMills=1000

channel.c2.type=memory
channel.c2.partitions=2
channel.c2.groups=group_2
channel.c2.compression=snappy
channel.c2.blockSize=262144
channel.c2.segmentSize=4294967296
channel.c2.flushPeriodMills=1000

sink.group_1.maxRetainPerPartitionBytes=9663676414
sink.group_1.partitionHandlerIdentity=direct
sink.group_1.functionIdentity=hdfs
sink.group_1.sinkPath=/tmp/test/cache
sink.group_1.roll.size=10240000
sink.group_1.bucketDateFormat=yyyyMMdd_HH
sink.group_1.maxRetries=3
sink.group_1.keytab=
sink.group_1.principle=
sink.group_1.idleTriggerMillis=60000

sink.group_2.maxRetainPerPartitionBytes=9663676414
sink.group_2.partitionHandlerIdentity=multi_thread
sink.group_2.threads=3
sink.group_2.functionIdentity=kafka
sink.group_2.kafka.bootstrap.servers=127.0.0.1:9092
sink.group_2.kafka.topic=test_topic
sink.group_2.kafka.key.serializer=org.apache.kafka.common.serialization.ByteArraySerializer
sink.group_2.kafka.value.serializer=org.apache.kafka.common.serialization.ByteArraySerializer
sink.group_2.kafka.buffer.memory=134217728
sink.group_2.kafka.linger.ms=1000
sink.group_2.kafka.batch.size=524288
sink.group_2.kafka.compression.type=snappy
sink.group_2.kafka.flushMill=60000

```       