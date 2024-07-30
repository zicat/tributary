# Tributary Design Guide

In this section, introduction to the design of several important roles.

## Channel

In tributary, the channel is designed as a data stream that supports appending data and multiple
sinks for isolated record reading.

At the same time, it also supports transaction operations to assist sinks in consuming data and
delivering it successfully to external systems at least once.

In [Tributary Channel Module](../tributary-channel), the interface of
[Channel](../tributary-channel/tributary-channel-base/src/main/java/org/zicat/tributary/channel/Channel.java)
is
designed.

## FileChannel Design

The file channel uses disks for data persistence to ensure data reliability. Compared to memory, it
greatly improves fault tolerance.

In fault scenarios, it is necessary to temporarily store a large amount of unconsumed data.
Implementing this based on memory may result in serious GC issues, which could affect sinks and even
cause program crashes.

To support multi-sinks scenarios, each sink needs to maintain its own GroupOffset. In case of
failures, each sink can be designed with different fault-tolerance strategies without affecting
other sinks, sources, or channels.

## FileChannel Implement

In open source products, the implementation of the file channel based
on [Apache Kafka](https://kafka.apache.org/) is relatively simple and basically meets the
requirements for channel isolation and reliability.
However,[Apache Kafka](https://kafka.apache.org/) also has some features that do not match the file
channel, including:

1. The design of logical offset and index

   Kafka uses index files to map the logical offset to the physical offset. To achieve this
   functionality, updating the index file is required when writing data, which is a waste of
   performance.

   In the tributary, the offsets stored in sinks are physical offsets, which can support disk access
   through this offset.

2. Kafka Service

   Kafka is a logging service and data must be transmitted over the network.

   In tributary, the file channel directly writes data to the local disk, saving unnecessary network
   expenses.

3. Data lifecycle

   Under normal circumstances, when a file has been successfully consumed by all sinks, the file
   channel can directly delete the file to ensure the quick release of system resources.

   Kafka not support directly deleting consumed segments. Although it is possible to set an
   expiration time, it is not appropriate in this scenario.

For the above reasons, the [Tributary-Channel](../tributary-channel)
implements the
[FileChannel](../tributary-channel/tributary-channel-file/src/main/java/org/zicat/tributary/channel/file/FileChannel.java)
based on disk.

The design of file channel allows tributary deployment to be independent of Kafka components, but
requires sufficient disk capacity on the machine.

### Tributary FileChannel Implement

#### About Segment

Under normal circumstances, the data lifecycle of a distribution system is transient, and in theory,
a record can be deleted immediately after it has been successfully consumed by all sinks.

However, in order to cater to the characteristics of the disk,
[FileChannel](../tributary-channel/tributary-channel-file/src/main/java/org/zicat/tributary/channel/file/FileChannel.java)
manages data through segments.

Segments cannot be too small, as frequent creation and deletion will bring additional performance
overhead. At the same time, segments cannot be too large, as large segments will cause expired data
to be deleted inefficiently, resulting in waste of system resources.

The appropriate size for a segment file is between 2G and 4G.

![image](picture/segment_struct.png)

Within the segment, data is managed using the block. The write operation is only responsible for
writing the data into the block. Once the block is filled, the data will be written into the page
cache of the operating system.

The advantage of using blocks as the minimum writing unit is that it shields the impact of small
data on writing and ensures the stability of the writing process.

The recommended block size under normal circumstances is 32k. If a single record itself is
relatively large, such as exceeding 1K, the block size can be appropriately increased.

Segment supports block-level compression, so if compression policies such as snappy or zstd are
configured, the block will be compressed before being written to the page cache, resulting in a
difference between the actual size written to disk and the size in memory.

Go to the source code of
[Segment](../tributary-channel/tributary-channel-base/src/main/java/org/zicat/tributary/channel/Segment.java)
for more
details.

#### About FileChannel

The main responsibilities of the file channel include segment lifecycle management, flush strategy,
and management of GroupOffset.

1. segment lifecycle management

   Each partition of the channel only has one segment running for writing, so the writing thread is
   blocked until the segment is successfully created during the segment creation phase.

   When a reading thread tries to read data that has not yet been produced, FileChannel control
   blocks it until a timeout occurs or new data arrives.

   When a segment is filled, the file channel sets that segment to read-only mode and creates a new
   segment.

   When a segment has been consumed by all sinks, the file channel is responsible for deleting that
   segment at an appropriate time.

2. disk flushing mechanism

   In general, synchronous writes to disks are time-consuming operations.
   File channel support asynchronous flush operations and monitor the size of the page cache.
   By configuring a trigger for synchronous writes, dirty page cache can be written to disks within
   a reasonable range.

3. consumption group management and persistence

   Responsible for making the GroupOffset submitted by sinks, and calculating the minimum segment ID
   to allow the clean-up thread to delete expired segments.

Go to the source code of
[FileChannel](../tributary-channel/tributary-channel-file/src/main/java/org/zicat/tributary/channel/file/FileChannel.java)
for more details.

## Sink

[Tributary-Sink](../tributary-sink) two consumption models, abstracting the real consumption
requirements and simplifying integration with [channels](../tributary-channel).

### Sink Models

Channel supports configuring multiple partitions, and the partitions are independent of each other.
Based on this background, the sink model includes two types: single-thread and multi-threads.

User can choose models by config `partition.concurrent` in application.properties.
If `partition.concurrent` is 1, the sink will use single-thread model, otherwise, the sink will
use multi-threads model.

#### Single Thread Model

In direct mode, partition and threads are bound one-to-one, making it suitable for scenarios where
the consumption rate of a single thread is greater than the writing rate.

![image](picture/direct_sink_model.png)

Each function in the figure above corresponds to a thread, go to the source code of
[DirectPartitionHandler](../tributary-sink/tributary-sink-base/src/main/java/org/zicat/tributary/sink/handler/DirectPartitionHandler.java)
for details.

#### Multi Threads Model

In multi-threads mode, a partition can be bound to multiple consumer threads. This is suitable for
scenarios where the consumption rate of a single thread is slower than the writing rate.

Compared to the direct mode, multi-threads mode require more CPU and memory resources.

The downside of the multi-threads mode is that it disrupts the order of records within a single
partition, making it unsuitable for scenarios that are sensitive to order. The solution is to
increase the number of partitions and use the direct mode, such that the write rate in each
partition is slower than the consumption rate.

![image](picture/multi_thread_sink_model.png)

In the figure above, two functions corresponds to one thread, go to the source code of
[MultiThreadPartitionHandler](../tributary-sink/tributary-sink-base/src/main/java/org/zicat/tributary/sink/handler/MultiThreadPartitionHandler.java)
for details.

### Function

Both direct mode and multi-threads support configuring
[AbstractFunction](../tributary-sink/tributary-sink-base/src/main/java/org/zicat/tributary/sink/function/AbstractFunction.java)
to develop sinks that meet actual needs.

[FunctionFactory](../tributary-sink/tributary-sink-base/src/main/java/org/zicat/tributary/sink/function/FunctionFactory.java)
uses Java SPI to inject
the [Function](../tributary-sink/tributary-sink-base/src/main/java/org/zicat/tributary/sink/function/AbstractFunction.java)
instance, users can implement it according to actual needs, such
as [PrintFunction](../tributary-sink/tributary-sink-base/src/main/java/org/zicat/tributary/sink/function/PrintFunctionFactory.java)
,
[HDFSFunction](../tributary-sink/tributary-sink-hdfs/src/main/java/org/zicat/tributary/sink/hdfs/HDFSFunctionFactory.java)
,
[KafkaFunction](../tributary-sink/tributary-sink-kafka/src/main/java/org/zicat/tributary/sink/kafka/KafkaFunctionFactory.java)
,
[HBaseFunction](../tributary-sink/tributary-sink-hbase/src/main/java/org/zicat/tributary/sink/hbase/HBaseFunctionFactory.java)
.

#### Trigger

User can define an implement of
the [AbstractFunction](../tributary-sink/tributary-sink-base/src/main/java/org/zicat/tributary/sink/function/AbstractFunction.java)
with the interface
of [Trigger](../tributary-sink/tributary-sink-base/src/main/java/org/zicat/tributary/sink/function/Trigger.java).
Set
idleTimeMillis and trigger idleTrigger() function if no records is received for idleTimeMillis.

Tributary service use the trigger feature
in [HDFSFunction](../tributary-sink/tributary-sink-hdfs/src/main/java/org/zicat/tributary/sink/hdfs/HDFSFunction.java)
to flush the records to hdfs and close the file handler when the idle exceeds the configured time.

