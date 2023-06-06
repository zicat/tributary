# Tributary User Guide

## Overview

The responsibilities of Tributary include providing a network interface for recording reporting, ensuring data reliability, distributing data to multiple external systems while ensuring that partial external system failures do not affect the data distribution of other external systems.

## Architecture

A tributary is a jvm-based service process hosts the components through which records flow from a source to a channel to
multi sinks.
![image](picture/tributary.png)

### Reliability

When the record is sent to the Tributary service, the source is responsible for writing the data to the channel and acking the client. The sinks pull the records from the channel based on their respective GroupOffsets. When the external system successfully stores these records, it submits the GroupOffset, which is the end-to-end reliability flow design of Tributary.

### Recoverability

If a channel experiences a malfunction, such as disk damage or reaching full capacity, the source will inform the client of the relevant problem. As Tributary service is stateless, the client is responsible for maintaining the health of all Tributary instances and making necessary adjustments in case of malfunctions.


If there is an external system failure for the sink, it will roll back to the previously committed GroupOffset and re-consume to ensure at least one successful consumption. Partial sink failures will not affect other sinks.

## Setting up a tributary service

This section documents how to configure the tributary’s application.properties. See the following section for creating a
tributary application using Spring Boot.

### Download package

Before start the tributary service, please install java8 first.

```shell
$ wget https://github.com/zicat/tributary/releases/download/1.0/tributary.zip
$ unzip tributary.zip
$ cd tributary  
``` 

If everything goes well, the current directory will be the release directory named "tributary", which includes three subdirectories: "bin", "config", and "libs".

### A simple example

Here, a simple config of application.properties in config dir is shown as follows:

```properties
server.port=8765
source.s1.channel=c1
source.s1.implement=netty
source.s1.netty.port=8200
source.s1.netty.decoder=lineDecoder
channel.c1.type=memory
channel.c1.groups=group_1
sink.group_1.functionIdentity=print
```

Please note that the port configuration for server.port and source.s1.netty.port should not be duplicated and must not be occupied by other processes.

Start the Tributary service by using this application.properties as follows

```shell
$ bash bin/tributary.sh start
```

Check whether Tributary has started successfully by checking the log files in the log directory.

```shell
$ tail -f log/tributary.log
```

![image](picture/start_success_log.png)

Try to use the telnet to send some records, please attend to the port if source.s1.netty.port changed.

![image](picture/telnet_client.png)

Check whether the tributary service receive and sink records successfully by the log file in log dir.

![image](picture/receive_success_log.png)

Attempt to send some records through telnet, and please note that the port in the command needs to match the configuration of source.s1.netty.port.

```shell
$ curl -s http://localhost:8765/metrics|grep -v '#'
```

![image](picture/metrics_url.png)

Congratulations - you’ve successfully configured and deployed a tributary service!

[Tributary User Guide Of Config Details](user_guide_config_detail.md) cover more details about the application.properties, let's continue.
                                                                           
