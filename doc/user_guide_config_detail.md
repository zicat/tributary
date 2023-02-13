# Tributary User Guide of Config Details

Tributary Config is consists of 4 parts include server, source, channel and sink.

## Server Detail

```properties
server.port=8765
server.metrics.ip.pattern=.*
```

Tributary service provide service metrics by http restful api based on SpringBoot, you can get more details abort
server.* from SpringBoot Document.

The param server.metrics.ip.pattern is a pattern, filter hosts(the server may has multi network adapter cards) and get
the expected matched host as metrics dimension value.

Tributary provide the http restful api to exposure metrics like sink_lag, sink_counter, most metrics need the dimension
of host.

Got the metrics of the tributary service as follows, attend to the port if server.port changed.

![image](picture/metrics_url.png)

|  Key                          |  default       | valid value                  | describe                                             |
|  ----                         | ----           | ---                          | ---                                                  |
| server.port                   |                | int(unit: number)                  | the port to bind, range 1000-65535                                         |
| server.metrics.ip.pattern     | .*             | string                | the pattern to filter expected host as the metrics value of host dimension      |

## Source Detail

Tributary support to define multi sources in the application.properties.

The source must bind a channel and implement as follows

```properties
source.s1.channel=c1
source.s1.implement=netty
source.s2.channel=c2
source.s2.implement=netty
``` 

We define two sources named s1 bind the channel named c1 and s2 bind the channel named c2 (How to define the channel
will be introduced as below).

The source must config the implement to tell the source how to receive records from the network.

Tributary provide the
[SourceFactory](../tributary-source/src/main/java/org/zicat/tributary/source/SourceFactory.java)
interface to develop special sources for suitable scenarios.

Tributary also provide the default implement
[netty](../tributary-source/src/main/java/org/zicat/tributary/source/netty/DefaultNettySourceFactory.java), shows all
params netty required as follows.

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

1. Different source use different netty.port

2. The lineDecoder parse the streaming to records by text line. It is suitable for demo scenarios using telnet.

3. The lengthDecoder parse the streaming by length-value decode like below, lengthDecoder ack the length of the received
   record, ack -1 if append the record to the channel fail, it's suitable for most environment.

   ![image](picture/line_decoder.png)

Tributary provide the lengthDecoder java client
[LengthDecoderClient](../tributary-source/src/main/java/org/zicat/tributary/source/netty/client/LengthDecoderClient.java)
for reference.

## Channel Detail

A channel like data streaming can be appended records and consumed records by sinks repeatedly, some types of channels like file has the
ability of persistence.

Tributary service support to define multi channels in the application.properties.

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
| blockSize         | 32768(32K)     | long(unit: byte)       | the size of memory to store records|
| compression       | none           | enum[none,zstd,snappy]           | the type of compression to compress the block before writing block to page cache |
| segmentSize       | 4294967296(4G) | long(unit: byte)       | the size of a segment, in file and memory channel segment is the smallest unit of resource recycling  |
| partitions        |                | string          | the directory list to store records, each directory represent one partition, the directory is allowed reading and writing, split by ','  |
| flushPeriodMills  | 500            | long(unit: ms)         | the period time to async flush page cache to disk|
| groupPersistPeriodSecond| 30       | long(unit: second)| the period time to async persist the committed group offset to disk|     


### Memory Config

|  key              |  default       | type                  | describe                                             |
|  ----             | ----           | ---                          | ---                                                  |
| blockSize         | 32768(32K)     | long(unit: byte)       | the size of memory to store records|
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

1. In file channel, using suitable blockSize, lower value may cause disk iops high.
2. In file channel, using suitable segmentSize like 4294967296, lower value cause frequent file creation/deletion,
   higher value cause deleting expired files not timely.
3. If we define multi file channels, please set different paths of the partitions, set same paths may cause unknown
   exceptions.
4. Setting some kafka properties is useless including key.deserializer, value.deserializer, key.serializer, value.serializer, group.id, enable.auto.commit.  

## Sink Detail

Tributary support to define multi sinks.

```properties
sink.group_1.maxRetainPerPartitionBytes=9663676414
sink.group_1.partitionHandlerIdentity=direct
sink.group_1.functionIdentity=hdfs
sink.group_2.maxRetainPerPartitionBytes=9663676414
sink.group_2.partitionHandlerIdentity=multi_thread
sink.group_2.threads=3
sink.group_2.functionIdentity=kafka
``` 

key                               |  default       | type                 | describe                                                                  |
|  ----                             | ----           | ---                          | ---                                                                       |
| maxRetainPerPartitionBytes        |                | long(unit: bytes)  | the max retain bytes of each partition. When the sink lag is over, the oldest segment will be deleted, the param may cause data lost, be careful     |
| partitionHandlerIdentity          | direct         | enum[direct,multi_thread]  | the sink model, direct model combine one channel's partition with one thread, multi_thread model combine one channel's partition with multi threads|
| threads                           | 2              | int(unit: number) | the thread count, only valid when the value of partitionHandlerIdentity is multi_thread |  
| functionIdentity                  |                | enum[print,kafka,hdfs]     | the function identity that configure how to consume records  |

Note:

1. User can customize functions by
   implements [FunctionFactory](../tributary-sink/tributary-sink-base/src/main/java/org/zicat/tributary/sink/function/FunctionFactory.java)
2. Print function receive records from channels and log it, kafka function send records to kafka server, hdfs function
   send records to hdfs.

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