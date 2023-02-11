#!/bin/sh

# shellcheck disable=SC2046
# shellcheck disable=SC2164
# shellcheck disable=SC2034
base_path=$(cd $(dirname "$0");cd ../; pwd)
cd "${base_path}"

release_dir="${base_path}"/release/tributary
release_dir_libs="${release_dir}""/libs"
release_dir_config="${release_dir}""/config"
release_dir_bin="${release_dir}""/bin"

# compile project
compile() {
  mvn clean install -DskipTests
}

# output dependency jars
# shellcheck disable=SC2034
# shellcheck disable=SC2012
prepare_lib() {
  mvn -f spring-boot-starter-tributary dependency:copy-dependencies -DoutputDirectory="${release_dir_libs}"
  ls spring-boot-starter-tributary/target/spring-boot-starter-tributary-*.jar | awk '{print $1}' | grep -v "tests.jar" | xargs -I {} cp {} "${release_dir}"
}

prepare_config() {
cat << EOF > "${release_dir_config}""/application.properties"
server.port=8765
server.metrics.ip.pattern=.*
source.s1.channel=c1
source.s1.netty.port=8200
source.s1.netty.decoder=lineDecoder
source.s1.implement=netty
channel.c1.type=file
channel.c1.partitions=/tmp/tributary/p1
channel.c1.groups=group_1
channel.c1.compression=snappy
sink.group_1.maxRetainPerPartitionBytes=9663676414
sink.group_1.partitionHandlerIdentity=direct
sink.group_1.functionIdentity=print
EOF
cat << EOF > "${release_dir_config}""/logback.xml"
<?xml version="1.0" encoding="UTF-8" ?>
<configuration scan="false">
    <appender name="FILE"
              class="ch.qos.logback.core.rolling.RollingFileAppender">
        <File>\${TRIBUTARY_LOG}/tributary.log</File>
        <rollingPolicy
                class="ch.qos.logback.core.rolling.TimeBasedRollingPolicy">
            <FileNamePattern>\${TRIBUTARY_LOG}/tributary.log.%d{yyyy-MM-dd}.log.zip</FileNamePattern>
            <maxHistory>7</maxHistory>
        </rollingPolicy>
        <layout class="ch.qos.logback.classic.PatternLayout">
            <Pattern>%d{yyyy-MM-dd HH:mm:ss.SSS} [%thread] %-5level %logger{32} - %msg%n</Pattern>
        </layout>
    </appender>
    <root level="INFO">
        <appender-ref ref="FILE"/>
    </root>
</configuration>
EOF
}

prepare_bin() {
  cp bin/*.sh "${release_dir_bin}"
}

create_release_dir() {
  # create release/tributary/bin,release/tributary/libs,release/tributary/config dir in root path
  rm -rf "${release_dir}" && mkdir -p "${release_dir}" && mkdir "${release_dir_config}" && mkdir "${release_dir_libs}" && mkdir "${release_dir_bin}"
}

# shellcheck disable=SC2034
# shellcheck disable=SC2012
create_release_dir
compile
prepare_lib
prepare_config
prepare_bin
