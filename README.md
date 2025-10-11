# Welcome to Tributary

Tributary is a reliable, stateless, fault-tolerance service for efficiently collecting and moving
huge amounts of
records. It has a simple and flexible architecture based on streaming records flows.

![image](doc/picture/tributary.png)

# Why Choose Tributary

1. Support multiple sinks consuming one channel based on read amplification.

    Tributary is designed to solve the reliability and isolation with multiple sinks consuming one channel.
    For example, sink the data to HDFS and Kafka from the same source for batching and streaming computing requirement.

    Persistently receiving and forwarding data to external systems while achieving mutual fault isolation is a highly challenging task.

    The current mainstream solution, such as [Apache Flume](https://flume.apache.org/), adopts a write amplification strategy to address isolation issues.

    Specifically, the received data is stored in multiple channels, and each sink consumes a separate channel.

    Tributary adopts a read amplification to address isolation issues and supports multiple sinks consuming the same channel based on Offset.

    The benefits of reading amplification not only ensure fault isolation, but also greatly reduce the overhead, especially when the data volume and number of sinks increase.

2. Support multiple source protocols.

    Tributary supports multiple source protocols, such as `Kafka Producer`, `Socket`, `Http`, `LogStash Beats`, `LogStash Http`.

3. Support multiple sinks.

    Tributary supports multiple sink such as `Kafka`, `Hbase`, `Elasticsearch`, `HDFS`.

4. Good performance.
    
    The channel writes data in block and compress block before writing to disk to reduce IO costs.

    The channel cache the block before compressing to reduce the cpu cost of reading in sink sides.
    
5. High reliability.

    Tributary supports at-least-once delivery semantics based on channel storage.

## Documentation

[Tributary User Guide](doc/user_guide.md)

[Tributary Design Guide](doc/tributary_design_guide.md)