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

## Developing custom sources

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

## Developing custom sinks

In the document [Tributary User Guide of Config Details](user_guide_config_detail.md), we have introduced 3 kinds of
sink include kafka, hdfs, print. In this section, we introduce more details of kafka.

### Sink kafka

The kafka sink need to configure a kafka topic, actually in some scenarios we need to dispatch different records to
different topic, let's implement this feature(All of these codes can be found in
the [tributary-demo](../tributary-demo)).

```java
public class DispatchKafkaFunction extends DefaultKafkaFunction {

    @Override
    protected boolean sendKafka(byte[] value) {
        final String stringValue = new String(value, StandardCharsets.UTF_8);
        final int index = stringValue.indexOf(",");
        final String topic = stringValue.substring(0, index);
        final String realValue = stringValue.substring(index + 1);
        final ProducerRecord<byte[], byte[]> record =
                new ProducerRecord<>(topic, null, realValue.getBytes(StandardCharsets.UTF_8));
        sendKafka(null, record);
        return true;
    }
}

public class DispatcherKafkaFunctionFactory implements FunctionFactory {
    @Override
    public Function createFunction() {
        return new DispatchKafkaFunction();
    }

    @Override
    public String identity() {
        return "kafka_demo_dispatcher";
    }
}
```

[DispatchKafkaFunction](../tributary-demo/src/main/java/org/zicat/tributary/demo/sink/DispatchKafkaFunction.java)
extends
[DefaultKafkaFunction](../tributary-service/src/main/java/org/zicat/tributary/service/sink/DefaultKafkaFunction.java)
override the sendKafka function. In this case, the value is converted to string that be combined with topic and value
by ','.

[DispatchKafkaFunctionFactory](
../tributary-demo/src/main/java/org/zicat/tributary/demo/sink/DispatchKafkaFunctionFactory.java) implements
[FunctionFactory](../tributary-sink/tributary-sink-base/src/main/java/org/zicat/tributary/sink/function/FunctionFactory.java)
to override function createFunction to create a DispatchKafkaFunction instance and define an identity of '
kafka_demo_dispatcher'.

Create the file if not exists named
[org.zicat.tributary.sink.function.FunctionFactory](../tributary-demo/src/main/resources/META-INF/services/org.zicat.tributary.sink.function.FunctionFactory)
in classpath [META-INF/services](../tributary-demo/src/main/resources/META-INF/services), append the full class name of
DispatcherKafkaFunctionFactory to it.

All is ready, let's configure it in the application.properties

```properties
sink.group_kafka.functionIdentity=kafka_demo_dispatcher
sink.group_kafka.partitionHandlerIdentity=direct
#sink.group_kafka.kafka.topic=test_topic
# support all kafka producer configuration, please see kafka official website for more detail
sink.group_kafka.kafka.bootstrap.servers=127.0.0.1:9092
sink.group_kafka.kafka.key.serializer=org.apache.kafka.common.serialization.ByteArraySerializer
sink.group_kafka.kafka.value.serializer=org.apache.kafka.common.serialization.ByteArraySerializer
sink.group_kafka.kafka.buffer.memory=134217728
sink.group_kafka.kafka.linger.ms=1000
sink.group_kafka.kafka.batch.size=524288
sink.group_kafka.kafka.compression.type=snappy
sink.group_kafka.kafka.flushMill=60000
```

Note:

1. The param sink.group_kafka.kafka.topic is useless, because we use records to compute topic in kafka_demo_dispatcher.
2. Because DispatchKafkaFunction extends DefaultKafkaFunction, other params are still supported.
