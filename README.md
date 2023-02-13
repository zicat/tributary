# Welcome to Tributary

Tributary is a reliable, stateless, fault-tolerance service for efficiently collecting and moving huge amounts of
records. It has a simple and flexible architecture based on streaming records flows. It is robust and fault tolerant.

![image](doc/picture/tributary.png)

# Why Choose Tributary
    
Tributary is specially designed to solve the reliability and isolation of multiple sinks consuming the same data,
a common need is that we want to send the data to hdfs system and kafka together for batch computing and streaming computing.

In fact, it is very challenging because we have to think about how to save received data when the failure occurs on hdfs, kafka or other external system.

The current mainstream solution like [Apache Flume](https://flume.apache.org/) is saving received data to multiple channels, each sink consume one channel (write-amplification). 

Tributary support multiple sinks to consume one channel together with the design of groupId and offset (read-amplification).

The benefits of read-amplification not only guarantee isolation but also reduce a lot of overhead 
especially the increase in the amount of data and sinks. This is the greatest advantage of tributary.

## Documentation

[Tributary User Guide](doc/user_guide.md)

[Tributary Developer Guide](doc/developer_guide.md)

[Tributary Design Guide](doc/tributary_design_guide.md)