# Sink HDFS Document

## Common Completed Configuration

```properties
server.port=8765
source.s1.channel=c1
source.s1.implement=netty
source.s1.netty.port=8200
source.s1.netty.decoder=lineDecoder
channel.c1.type=memory
channel.c1.groups=group_1
sink.group_1.function.id=hdfs
sink.group_1.sink.path=/tmp/test/cache
sink.group_1.roll.size=10240000
sink.group_1.bucket.date.format=yyyyMMdd_HH_mm
sink.group_1.bucket.date.timezone=UTC
sink.group_1.max.retries=3
sink.group_1.keytab=
sink.group_1.principle=
sink.group_1.output.compression.codec=snappy
sink.group_1.idle.trigger.millis=30000
```

[Let's start the tributary service](../../doc/user_guide.md) using the above configuration and
construct some data.

Try to use the telnet to send some records, please attend to the port if source.s1.netty.port
changed.

```shell
$ telnet localhost 8200
Trying ::1...
Connected to localhost.
Escape character is '^]'.
213123
dsafasdf
sadfasfasdfasdf
```

Due to the configuration setting of sink.group_1.bucket.date.format to create a new bucket every
minute, a data file will be generated in the subdirectory where sink.group_1.sink.path is located
after a one-minute wait.

At the same time, due to the configuration of sink.group_1.idle.trigger.millis as 30 seconds, if the
input data channel is idle for 30 seconds, a data file will also be generated in the subdirectory
where sink.group_1.sink.path is located.

```shell
$ ll /tmp/test/cache/20240624_05_57/s1/
1.7K  6 24 13:58 7420ae3d_8f57_4105_9875_3ca0a495b003_c1_group_1_0.1.snappy.parquet
```

The above demo uses the local HDFS file system. How can I switch to another file system? Simply add
the related HDFS configuration files such as hdfs-site.xml and core-site.xml to CLASSPATH.

## Questions

1. How to read those parquet files?

   The parquet schema is defined
   in [ParquetHDFSWriter.SCHEMA](src/main/java/org/zicat/tributary/sink/hdfs/ParquetHDFSWriter.java).

   [Read Demo](../../sample-code/src/main/java/org/zicat/tributary/demo/sink/HDFSSinkParquetReader.java)

   ```text
   topic:s1, partition:0, headers:[_rec_ts:1719208673, _sent_ts:1719208675], key:, value:213123
   topic:s1, partition:0, headers:[_rec_ts:1719208674, _sent_ts:1719208675], key:, value:dsafasdf
   topic:s1, partition:0, headers:[_rec_ts:1719208675, _sent_ts:1719208675], key:, value:sadfasfasdfasdf
   ```
   
  
   
