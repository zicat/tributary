# Tributary User Guide

## Overview

Tributary is a reliable, stateless, fault-tolerance service for efficiently collecting and moving huge amounts of
records. It has a simple and flexible architecture based on streaming records flows. It is robust and fault tolerant.

The duty of tributary is to provide records uploading network interface, ensure records not lost and sink records to
multi external systems, ensure the crash of some external systems not affect other sinks and not affect sources.

## Architecture

A tributary is a jvm-based service process hosts the components through which records flow from a source to a channel to
multi sinks.
![image](picture/tributary.png)

### Reliability

When records sent to the tributary service, the source append into the channel and ack client, sinks fetch records from
the channel with RecordsOffset, and commit the RecordsOffset after external systems store these records. This is a how
records in Tributary provide end-to-end reliability of the flow.

### Recoverability

If the channel crash e.g., disk full, Source report client exception. Because tributary service is stateless, client can
switch to other tributary services until this service is recovered.

If the external sink system crash, the sink will roll up to the previous committed RecordsOffset and reconsume records(
at least once). The crash of some sink systems not affect the others that sink records to theirs healthy external
systems.

## Setting up a tributary service

This section documents how to configure the tributary’s application.properties. See the following section for creating a
tributary application using Spring Boot.

### Build package

Before start the tributary service, we should compile and package it from source code depending on java and maven,
please install JDK8 and Maven3 first on your macOS or linux pc.

Download source code using Git or other tools.

```shell
$ cd tributary
$ bash sbin/package.sh
$ cd release/tributary  
``` 

If expected, the current dir is the release dir named tributary which contains dirs include bin, config, libs.

### A simple example

Here, we give an example in config/application.properties as follows:

```properties
server.port=8765
server.metrics.ip.pattern=.*
source.s1.channel=c1
source.s1.netty.port=8200
source.s1.netty.decoder=lineDecoder
source.s1.implement=netty
channel.c1.dirs=/tmp/tributary/p1
channel.c1.groups=group_1
channel.c1.compression=snappy
channel.c1.flushForce=true
sink.group_1.maxRetainPerPartitionBytes=9663676414
sink.group_1.partitionHandlerIdentity=direct
sink.group_1.functionIdentity=print
```

Note: server.port and source.s1.netty.port is not be used and same, channel.c1.dirs must exist and allow reading and
writing.

Given the application.properties, we can start tributary service as follows:

```shell
$ bash bin/tributary.sh start
```

Check whether the tributary service start successfully by the log file in log dir.

```shell
$ tail -f log/tributary.log
```

![image](picture/start_success_log.png)

Try to use telnet tool sending some records, attend to the port if source.s1.netty.port changed.

![image](picture/telnet_client.png)

Check whether the tributary service receive and sink successfully by the log file in log dir.

![image](picture/receive_success_log.png)

Check the metrics of the tributary service by http restful api, attend to the port if server.port changed.

```shell
$ curl -s http://localhost:8765/metrics|grep -v '#'
```

![image](picture/metrics_url.png)

Congratulations - you’ve successfully configured and deployed a tributary service!

[Tributary User Guide Of Config Details](user_guide_config_detail.md) cover the application.properties in much more
detail, let's continue.
