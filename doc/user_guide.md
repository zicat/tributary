# Tributary User Guide

## Overview

Tributary is a reliable, stateless，fault-tolerance service for efficiently collecting and moving huge amounts of records.
It has a simple and flexible architecture based on streaming records flows. It is robust and fault tolerant.

The duty of tributary is providing records uploading network interface, ensuring records not lost 
and sinking records to multi external systems, ensuring external systems failures not affect each other and not affect sources.

## Architecture

A tributary is a JVM service process hosts the components through which records(byte arrays) flow from a source to a channel to multi sinks.
![image](picture/tributary.png)

### Reliability
When a record upload to the tributary service, the source receive it ack client and append it into the channel, the channel storage and delete util all sinks consumed it. 
This is a how the record in Tributary provide end-to-end reliability of the flow.

Tributary sink uses a RecordOffset to fetch records from the channel, and commit the RecordOffset after external system store the record.

### Recoverability
If the channel crash e.g., disk full, tributary will be offline automatically. Because tributary service is stateless, client can switch to other tributary services.
If the external sink system crashed, the sink will roll up to the previous committed RecordOffset and reconsume records(at least once). 
Some sink systems crashed not affect the others sink records to theirs healthy external systems.

## Setting up a tributary service
This section documents how to configure and wire tributary components using tributary’s long-standing configuration techniques using properties files. 
See the following section for creating a tributary application using Spring Boot.

### prepare
Before start the tributary, We should compile and package it from source code.
Tributary depend on java and maven, so please install JDK8 AND Maven3 first on your machine on MACOS OR LINUX.

### package

Download source code using git clone or other tools first.
```shell
$ cd tributary
$ bash sbin/package.sh
$ cd release/tributary  
``` 
If everything is right, Current dir is the release dir that contains bin, config, libs sub dirs.

### A simple example
Here, we give an example in config/application.properties like below

Note: server.port and source.s1.netty.port is not be used, channel.c1.dirs is exist and allow reading and writing.
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

Given this application.properties, we can start tributary as follows:
```shell
$ bash bin/tributary.sh start
```

Check whether the tributary service start successfully by the log file in log dir
```shell
$ tail -f log/tributary.log
```

![image](picture/start_success_log.png)

Using telnet tool try to send some records

![image](picture/telnet_client.png)

Check whether the tributary service receive and sink successfully by the log file in log dir

![image](picture/receive_success_log.png)

Congratulations - you’ve successfully configured and deployed a tributary service! 

Subsequent sections cover the application.properties in much more detail.