# 项目介绍

Tributary项目负责提供可靠的数据接收服务，并灵活地将数据分发至多个外部系统，同时保证相互之间的故障隔离。


# 什么是分发系统

[分发系统](doc/what_is_dispatcher.md)

# 子项目介绍

## Tributary-Channel

分发系统中关键角色Channel的设计与实现，多路Sink隔离性可靠性的重要保证，[详细介绍](tributary-channel)。

## Tributary-Sink

基于Tributary-Channel实现多路分发框架，针对不同分发需求，提供多样的分发算法，包括：单线程分发，多线程分发等，[详细介绍](tributary-sink)。

# 项目开发

## 编译与导入
- $ checkout project first
- $ cd tributary && mvn clean install -DskipTests
- 将项目导入至Intellij中进行开发工作，配置checkstyle & format

    ![image](doc/picture/checkstyle.png)


