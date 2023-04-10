# Welcome to Tributary

Tributary is a reliable, stateless, fault-tolerance service for efficiently collecting and moving huge amounts of
records. It has a simple and flexible architecture based on streaming records flows. It is robust and fault tolerant.

![image](doc/picture/tributary.png)

# Why Choose Tributary
    
Tributary is designed to solve the reliability and isolation with multiple sinks consuming the channel.
For example, sink data to HDFS and Kafka at the same time for batching and streaming computing requirement.

Persistently receiving and forwarding data to external systems such as HDFS and Kafka while achieving mutual fault isolation is a highly challenging task.

The current mainstream solution, such as [Apache Flume](https://flume.apache.org/), adopts a write amplification strategy to address isolation issues. Specifically, the received data is stored in multiple channels, and each sink consumes a separate channel.
                                                                                    
Tributary adopts a read amplification to address isolation issues and supports multiple sinks consuming the same channel based on GroupOffset.

The benefits of reading amplification not only ensure fault isolation, but also greatly reduce the overhead, 
especially when the data volume and number of sinks increase.

## Documentation

[Tributary User Guide](doc/user_guide.md)

[Tributary Developer Guide](doc/developer_guide.md)

[Tributary Design Guide](doc/tributary_design_guide.md)