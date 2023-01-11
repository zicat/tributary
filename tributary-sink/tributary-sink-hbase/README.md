# Tributary-Sink-HBASE
Tributary-Sink-Hbase基于[Tributary-Sink-Base](../README.md)提供Sink HBASE能力。

## AbstractHBaseFunction

[AbstractHBaseFunction](src/main/java/org/zicat/tributary/sink/hbase/AbstractHBaseFunction.java)是此项目的核心类。

主要功能包括：

- 维护HBASE client链接。

    为了保证写入性能，每一个分区的每一个hbase table维护独立的hbase connection。所以createHBaseConf()方法返回的Configuration也是针对单分区单个hbase table，需要关注其中的性能参数配置。

- hbase flush与文件队列commit管理，保证数据可靠性。