/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.zicat.tributary.service.configuration;

import lombok.Data;
import org.apache.hadoop.fs.FileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;
import org.zicat.tributary.channel.Channel;
import org.zicat.tributary.channel.CompressionType;
import org.zicat.tributary.channel.file.PartitionFileChannelBuilder;
import org.zicat.tributary.channel.utils.IOUtils;
import org.zicat.tributary.service.metrics.SinkGroupManagerCollector;
import org.zicat.tributary.sink.SinkGroupConfig;
import org.zicat.tributary.sink.SinkGroupConfigBuilder;
import org.zicat.tributary.sink.SinkGroupManager;
import org.zicat.tributary.sink.function.AbstractFunction;
import org.zicat.tributary.sink.handler.AbstractPartitionHandler;
import org.zicat.tributary.sink.handler.factory.DirectPartitionHandlerFactory;
import org.zicat.tributary.sink.utils.HostUtils;

import javax.annotation.PostConstruct;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

/** DynamicChannel. */
@ConfigurationProperties
@Configuration
@Data
public class DynamicChannel implements Closeable {

    private static final Logger LOG = LoggerFactory.getLogger(DynamicChannel.class);
    private static final String SPLIT_STR = ",";

    private static final String KEY_FILE_DIRS = "dirs";

    private static final String KEY_FILE_BLOCK_SIZE = "blockSize";
    private static final String DEFAULT_FILE_BLOCK_SIZE = String.valueOf(32 * 1024);

    private static final String KEY_FILE_SEGMENT_SIZE = "segmentSize";
    private static final String DEFAULT_FILE_SEGMENT_SIZE =
            String.valueOf(4L * 1024L * 1024L * 1024L);

    private static final String KEY_FILE_CLEAN_UP_PERIOD_SECOND = "cleanUpPeriodSecond";
    private static final String DEFAULT_FILE_CLEAN_UP_PERIOD_SECOND = String.valueOf(60);

    private static final String KEY_FILE_FLUSH_PERIOD_MILLS = "flushPeriodMills";
    private static final String DEFAULT_FILE_FLUSH_PERIOD_MILLS = String.valueOf(500);

    private static final String KEY_FILE_FLUSH_FORCE = "flushForce";
    private static final String DEFAULT_FILE_FLUSH_FORCE = "false";

    private static final String KEY_FILE_COMPRESSION = "compression";
    private static final String DEFAULT_FILE_COMPRESSION = "none";

    private static final String KEY_FILE_FLUSH_PAGE_CACHE_SIZE = "flushPageCacheSize";
    private static final String DEFAULT_FILE_FLUSH_PAGE_CACHE_SIZE =
            String.valueOf(1024L * 1024L * 32L);

    private static final String KEY_SINK_HANDLER_IDENTIFY = "partitionHandlerIdentify";
    private static final String DEFAULT_SINK_HANDLER_IDENTIFY =
            DirectPartitionHandlerFactory.IDENTIFY;

    private static final String KEY_SINK_CHANNEL = "channel";
    private static final String KEY_SINK_FUNCTION_IDENTIFY = "functionIdentify";

    private final AtomicBoolean closed = new AtomicBoolean(false);

    final List<String> channelNames = new ArrayList<>();
    final List<String> groupIds = new ArrayList<>();
    final Map<String, Channel> channels = new HashMap<>();
    final Map<String, Channel> groupFileChannelMapping = new HashMap<>();
    final Map<String, SinkGroupManager> sinkGroupManagerMap = new HashMap<>();

    Map<String, String> channel;
    Map<String, String> sink;

    @Value("${server.metrics.ip.pattern:.*}")
    String metricsIpPattern;

    @PostConstruct
    public void init() {
        if (channel == null || channel.isEmpty()) {
            return;
        }
        try {
            channelNames.addAll(getAllChannels());
            groupIds.addAll(getAllGroups());
            initFileChannels();
            initSinkGroupManagers();
        } catch (Throwable e) {
            IOUtils.closeQuietly(this);
            throw e;
        }
    }

    /**
     * parser channels.
     *
     * @return topic list
     */
    private Set<String> getAllChannels() {
        final Set<String> channels = new HashSet<>();
        for (Map.Entry<String, String> entry : channel.entrySet()) {
            final String key = entry.getKey();
            final String[] keySplit = key.split("\\.");
            channels.add(keySplit[0]);
        }
        return channels;
    }

    /**
     * parser groups.
     *
     * @return group list
     */
    private Set<String> getAllGroups() {
        final Set<String> topics = new HashSet<>();
        for (Map.Entry<String, String> entry : sink.entrySet()) {
            final String key = entry.getKey();
            final String[] keySplit = key.split("\\.");
            topics.add(keySplit[0]);
        }
        return topics;
    }

    /**
     * get channel by topic.
     *
     * @param channel channel
     * @return Channel
     */
    public Channel getChannel(String channel) {
        return channels.get(channel);
    }

    @Override
    public void close() throws IOException {
        if (closed.compareAndSet(false, true)) {
            for (Map.Entry<String, Channel> entry : channels.entrySet()) {
                try {
                    entry.getValue().flush();
                } catch (Exception e) {
                    LOG.warn("flush error", e);
                }
            }
            try {
                sinkGroupManagerMap.forEach((k, v) -> IOUtils.closeQuietly(v));
            } finally {
                try {
                    channels.forEach((k, v) -> IOUtils.closeQuietly(v));
                } finally {
                    FileSystem.closeAll();
                }
            }
        }
    }

    /**
     * get dynamic field value.
     *
     * @param topic topic
     * @param key key
     * @param defaultValue defaultValue
     * @return string value
     */
    private String dynamicFileValue(String topic, String key, String defaultValue) {
        final String realKey = String.join(".", topic, key);
        final String value = channel.get(realKey);
        if (value == null && defaultValue == null) {
            throw new IllegalStateException("file key not exist in configuration " + realKey);
        }
        return value == null ? defaultValue : value;
    }

    /**
     * get dynamic sink value.
     *
     * @param groupId groupId
     * @param key key
     * @param defaultValue defaultValue
     * @return string value
     */
    private String dynamicSinkValue(String groupId, String key, String defaultValue) {
        final String value = dynamicNullableSinkValue(groupId, key, defaultValue);
        if (value == null) {
            throw new IllegalStateException(
                    "sink key not exist in configuration " + groupId + "." + key);
        }
        return value;
    }

    /**
     * get dynamic nullable sink value.
     *
     * @param groupId groupId
     * @param key key
     * @param defaultValue defaultValue
     * @return string value
     */
    private String dynamicNullableSinkValue(String groupId, String key, String defaultValue) {
        final String realKey = String.join(".", groupId, key);
        final String value = sink.get(realKey);
        return value == null ? defaultValue : value;
    }

    /** init topic sink group ids. */
    private Map<String, Set<String>> buildTopicSinkGroupIds() {
        final Map<String, Set<String>> topicSinkGroupIds = new HashMap<>();
        for (String groupId : groupIds) {
            final String topic = dynamicSinkValue(groupId, KEY_SINK_CHANNEL, null);
            if (!channelNames.contains(topic)) {
                throw new IllegalStateException(
                        "sink topic not configuration in channel.file.topics, topic = " + topic);
            }
            topicSinkGroupIds.computeIfAbsent(topic, key -> new HashSet<>()).add(groupId);
        }
        return topicSinkGroupIds;
    }

    /** init sink group configs. */
    private Map<String, SinkGroupConfigBuilder> buildSinkGroupConfigs() {
        Map<String, SinkGroupConfigBuilder> sinkGroupConfigs = new HashMap<>();
        for (String groupId : groupIds) {
            sinkGroupConfigs.put(groupId, createSinkGroupConfigBuilderByGroupId(groupId));
        }
        return sinkGroupConfigs;
    }

    /** init file channels. */
    private void initFileChannels() {
        final Map<String, Set<String>> topicSinkGroupIds = buildTopicSinkGroupIds();
        for (String topic : channelNames) {
            final Channel channel = createChannelBuilderByTopic(topic, topicSinkGroupIds).build();
            channels.put(topic, channel);
        }
        for (String group : groupIds) {
            String channel = dynamicSinkValue(group, KEY_SINK_CHANNEL, null);
            groupFileChannelMapping.put(group, channels.get(channel));
        }
    }

    /**
     * create sink group config builder by groupId.
     *
     * @param groupId groupId
     * @return SinkGroupConfigBuilder
     */
    private SinkGroupConfigBuilder createSinkGroupConfigBuilderByGroupId(String groupId) {

        final String sinkHandlerIdentify =
                dynamicSinkValue(groupId, KEY_SINK_HANDLER_IDENTIFY, DEFAULT_SINK_HANDLER_IDENTIFY);
        final String functionIdentify = dynamicSinkValue(groupId, KEY_SINK_FUNCTION_IDENTIFY, null);
        final SinkGroupConfigBuilder configBuilder =
                SinkGroupConfigBuilder.newBuilder()
                        .functionIdentify(functionIdentify)
                        .handlerIdentify(sinkHandlerIdentify);

        // add custom property.
        final String keyPrefix = groupId + ".";
        for (Map.Entry<String, String> entry : sink.entrySet()) {
            final String key = entry.getKey();
            final int index = key.indexOf(keyPrefix);
            if (index == 0) {
                configBuilder.addCustomProperty(
                        key.substring(keyPrefix.length()), entry.getValue());
            }
        }
        return configBuilder;
    }

    /** init all sink group manager and start. */
    private void initSinkGroupManagers() {

        final Map<String, SinkGroupConfigBuilder> sinkGroupConfigs = buildSinkGroupConfigs();
        final String metricsHost = HostUtils.getLocalHostString(metricsIpPattern);
        for (Map.Entry<String, SinkGroupConfigBuilder> entry : sinkGroupConfigs.entrySet()) {

            final String groupId = entry.getKey();
            final Channel channel = groupFileChannelMapping.get(groupId);

            final SinkGroupConfigBuilder sinkGroupConfigBuilder = entry.getValue();
            final String maxRetainPerPartition =
                    dynamicSinkValue(
                            groupId,
                            AbstractPartitionHandler.KEY_MAX_RETAIN_SIZE,
                            AbstractPartitionHandler.DEFAULT_MAX_RETAIN_SIZE);
            sinkGroupConfigBuilder.addCustomProperty(
                    AbstractPartitionHandler.KEY_MAX_RETAIN_SIZE,
                    maxRetainPerPartition.isEmpty() ? null : Long.parseLong(maxRetainPerPartition));
            sinkGroupConfigBuilder.addCustomProperty(
                    AbstractFunction.KEY_METRICS_HOST, metricsHost);

            final SinkGroupConfig sinkGroupConfig = sinkGroupConfigBuilder.build();
            final SinkGroupManager sinkGroupManager =
                    new SinkGroupManager(groupId, channel, sinkGroupConfig);
            sinkGroupManagerMap.put(groupId, sinkGroupManager);
        }
        sinkGroupManagerMap.forEach((k, v) -> v.createPartitionHandlesAndStart());
        new SinkGroupManagerCollector(sinkGroupManagerMap, metricsHost).register();
    }

    /**
     * create partition file channel builder by topic.
     *
     * @param topic topic
     * @return PartitionFileChannelBuilder
     */
    private PartitionFileChannelBuilder createChannelBuilderByTopic(
            String topic, Map<String, Set<String>> topicSinkGroupIds) {
        final Set<String> groupIds = topicSinkGroupIds.get(topic);
        if (groupIds == null || groupIds.isEmpty()) {
            throw new IllegalStateException("topic has no sink group " + topic);
        }
        final List<String> dirs =
                Arrays.asList(dynamicFileValue(topic, KEY_FILE_DIRS, null).split(SPLIT_STR));
        final int blockSize =
                Integer.parseInt(
                        dynamicFileValue(topic, KEY_FILE_BLOCK_SIZE, DEFAULT_FILE_BLOCK_SIZE));
        final long segmentSize =
                Long.parseLong(
                        dynamicFileValue(topic, KEY_FILE_SEGMENT_SIZE, DEFAULT_FILE_SEGMENT_SIZE));
        final int cleanUpPeriodSecond =
                Integer.parseInt(
                        dynamicFileValue(
                                topic,
                                KEY_FILE_CLEAN_UP_PERIOD_SECOND,
                                DEFAULT_FILE_CLEAN_UP_PERIOD_SECOND));
        final int flushPeriodMills =
                Integer.parseInt(
                        dynamicFileValue(
                                topic,
                                KEY_FILE_FLUSH_PERIOD_MILLS,
                                DEFAULT_FILE_FLUSH_PERIOD_MILLS));
        final long flushPageCacheSize =
                Long.parseLong(
                        dynamicFileValue(
                                topic,
                                KEY_FILE_FLUSH_PAGE_CACHE_SIZE,
                                DEFAULT_FILE_FLUSH_PAGE_CACHE_SIZE));
        final String compression =
                dynamicFileValue(topic, KEY_FILE_COMPRESSION, DEFAULT_FILE_COMPRESSION);

        final boolean flushForce =
                Boolean.parseBoolean(
                        dynamicFileValue(topic, KEY_FILE_FLUSH_FORCE, DEFAULT_FILE_FLUSH_FORCE));

        final PartitionFileChannelBuilder builder =
                PartitionFileChannelBuilder.newBuilder()
                        .dirs(dirs.stream().map(File::new).collect(Collectors.toList()));
        builder.blockSize(blockSize)
                .segmentSize(segmentSize)
                .compressionType(CompressionType.getByName(compression))
                .cleanUpPeriod(cleanUpPeriodSecond, TimeUnit.SECONDS)
                .flushPeriod(flushPeriodMills, TimeUnit.MILLISECONDS)
                .flushPageCacheSize(flushPageCacheSize)
                .flushForce(flushForce)
                .topic(topic)
                .consumerGroups(new ArrayList<>(groupIds));
        return builder;
    }
}
