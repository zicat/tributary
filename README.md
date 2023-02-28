# Welcome to Tributary

Tributary is a reliable, stateless, fault-tolerance service for efficiently collecting and moving huge amounts of
records. It has a simple and flexible architecture based on streaming records flows. It is robust and fault tolerant.

![image](doc/picture/tributary.png)

# Why Choose Tributary
    
Tributary is designed to solve the reliability and isolation with multiple sinks consuming the channel.
For example, sink data to HDFS and Kafka at the same time for batching and streaming computing requirement.

It's challenging to fulfill the requirement because how to save received data when the failure occurs on hdfs, kafka or other external system must be resolved.

The current mainstream solution like [Apache Flume](https://flume.apache.org/) is saving received data to multiple channels, each sink consume one channel (write-amplification). 

Tributary support multiple sinks to consume one channel with the design of GroupOffset (read-amplification).

The benefits of read-amplification not only guarantee isolation but also reduce a lot of overhead 
especially the increase in the amount of data and sinks. This is the greatest advantage of tributary.

## Documentation

[Tributary User Guide](doc/user_guide.md)

[Tributary Developer Guide](doc/developer_guide.md)

[Tributary Design Guide](doc/tributary_design_guide.md)