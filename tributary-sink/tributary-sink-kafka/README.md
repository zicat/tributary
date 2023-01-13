# Tributary-Sink-Kafka
Tributary-Sink-Kafka基于[Tributary-Sink-Base](../README.md)提供Sink Kafka能力。

## AbstractKafkaFunction

[AbstractKafkaFunction](src/main/java/org/zicat/tributary/sink/kafka/AbstractKafkaFunction.java)是此项目的核心类。

主要功能包括：多kafka集群Producer链接管理。

## 使用方式

由于[AbstractKafkaFunction](src/main/java/org/zicat/tributary/sink/kafka/AbstractKafkaFunction.java)为抽象类，需要结合用户实际业务需求继承该类将数据处理加工后调用sendKafka方法进行数据下发。