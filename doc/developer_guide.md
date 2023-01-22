# Tributary Developer Guide

If you first know abort Tributary, please read the [Tributary User Guide](user_guide.md) first.

## Building Tributary

Checkout the code using Git, compile the source code and testing unit case

```shell script
$ git fetch && git checkout master && git pull
$ cd tributary
$ mvn clean install
```

## Preparing

Before developing custom components, loading the project to IDE, we recommend developing tributary by Intellij.

Goto [Intellij Plugin Install and Config](intellij_plugin.md) for more details.

To show the demo code conveniently, we create the module [tributary-demo](../tributary-demo).

## Developing custom source

In the document [Tributary User Guide of Config Details](user_guide_config_detail.md), we have introduced how to use the
default implement of netty.

Now we try to develop a simple http source and use it.

### Coding

Http protocol is a complicated protocol, we use netty framework helping to finish http decoder and encoder.

Tributary provide
[AbstractNettyTributaryServer](../tributary-service/src/main/java/org/zicat/tributary/service/source/netty/AbstractNettyTributaryServer.java)
to simplify the code, let's have a look at how to extend
[AbstractNettyTributaryServer](../tributary-service/src/main/java/org/zicat/tributary/service/source/netty/AbstractNettyTributaryServer.java)
implement [HttpTributaryServer](../tributary-demo/src/main/java/org/zicat/tributary/demo/source/HttpTributaryServer.java)
.

Note:

1. Add necessary http decoder and encoder, the duty
   of [SimpleHttpHandler](../tributary-demo/src/main/java/org/zicat/tributary/demo/source/SimpleHttpHandler.java)
   defined in the
   [HttpTributaryServer](../tributary-demo/src/main/java/org/zicat/tributary/demo/source/HttpTributaryServer.java)
   is to get data from http body and append to the channel.
2. The channel may have multi
   partitions, [SimpleHttpHandler](../tributary-demo/src/main/java/org/zicat/tributary/demo/source/SimpleHttpHandler.java)
   select one partition to append by the policy of random.
3. Response the length of the received record.

[HttpTributaryServerFactory](../tributary-demo/src/main/java/org/zicat/tributary/demo/source/HttpTributaryServerFactory.java)
is a factory to create
[HttpTributaryServer](../tributary-demo/src/main/java/org/zicat/tributary/demo/source/HttpTributaryServer.java)
when the tributary service is starting, the identity will be used in application.properties.

Create a file if not exist
named [org.zicat.tributary.service.source.TributaryServerFactory](../tributary-demo/src/main/resources/META-INF/services/org.zicat.tributary.service.source.TributaryServerFactory)
in [resources/META-INF/services](../tributary-demo/src/main/resources/META-INF/services), append the full name
of [HttpTributaryServerFactory](../tributary-demo/src/main/java/org/zicat/tributary/demo/source/HttpTributaryServerFactory.java)
.

### Testing

1. Create a
   properties [application-source-demo.properties](../tributary-demo/src/main/resources/application-source-demo.properties)
   set source.s1.implement=http

2. Create a main
   class [HttpSourceDemoApplication](../tributary-demo/src/main/java/org/zicat/tributary/demo/HttpSourceDemoApplication.java)
   for testing

3. Using intellij Run tool start this main class

4. Using terminal to send test data

    ```shell script
     $ curl -s -X POST http://localhost:8200 -d 'Hello Tributary Http Server'
    ```

5. Check whether print the log from intellij console

   ![image](picture/source_http_demo_receive_data.png)