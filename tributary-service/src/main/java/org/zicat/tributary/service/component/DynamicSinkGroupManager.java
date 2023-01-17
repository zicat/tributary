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

package org.zicat.tributary.service.component;

import lombok.Getter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.zicat.tributary.channel.Channel;
import org.zicat.tributary.channel.utils.IOUtils;
import org.zicat.tributary.service.configuration.SinkGroupManagerConfiguration;
import org.zicat.tributary.sink.SinkGroupConfig;
import org.zicat.tributary.sink.SinkGroupConfigBuilder;
import org.zicat.tributary.sink.SinkGroupManager;
import org.zicat.tributary.sink.function.AbstractFunction;
import org.zicat.tributary.sink.handler.AbstractPartitionHandler;
import org.zicat.tributary.sink.handler.factory.DirectPartitionHandlerFactory;
import org.zicat.tributary.sink.utils.HostUtils;

import javax.annotation.PostConstruct;
import java.io.Closeable;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

/** DynamicChannelSinkGroupManager. */
@Component
@Getter
public class DynamicSinkGroupManager implements Closeable {

    private static final String KEY_SINK_HANDLER_IDENTITY = "partitionHandlerIdentity";
    private static final String DEFAULT_SINK_HANDLER_IDENTITY =
            DirectPartitionHandlerFactory.IDENTITY;

    private static final String KEY_SINK_FUNCTION_IDENTITY = "functionIdentity";

    private final AtomicBoolean closed = new AtomicBoolean(false);

    /* key:group id, value: sink group manager consumed by the key */
    final Map<String, List<SinkGroupManager>> sinkGroupManagerMap = new HashMap<>();

    @Autowired SinkGroupManagerConfiguration sinkGroupManagerConfiguration;
    @Autowired DynamicChannel dynamicChannel;

    @Value("${server.metrics.ip.pattern:.*}")
    String metricsIpPattern;

    @PostConstruct
    public void init() {
        initSinkGroupManagers();
    }

    /** init all sink group manager and start. */
    private void initSinkGroupManagers() {

        final Map<String, SinkGroupConfigBuilder> sinkGroupConfigs = buildSinkGroupConfigs();
        final String metricsHost = HostUtils.getLocalHostString(metricsIpPattern);
        for (Map.Entry<String, SinkGroupConfigBuilder> entry : sinkGroupConfigs.entrySet()) {

            final String groupId = entry.getKey();
            final List<Channel> channels = dynamicChannel.findChannels(groupId);
            if (channels.isEmpty()) {
                throw new RuntimeException("group id not found channel, groupId = " + groupId);
            }
            for (Channel channel : channels) {
                final SinkGroupConfigBuilder sinkGroupConfigBuilder = entry.getValue();
                final String maxRetainPerPartition =
                        dynamicSinkValue(
                                groupId,
                                AbstractPartitionHandler.KEY_MAX_RETAIN_SIZE,
                                AbstractPartitionHandler.DEFAULT_MAX_RETAIN_SIZE);
                sinkGroupConfigBuilder.addCustomProperty(
                        AbstractPartitionHandler.KEY_MAX_RETAIN_SIZE,
                        maxRetainPerPartition.isEmpty()
                                ? null
                                : Long.parseLong(maxRetainPerPartition));
                sinkGroupConfigBuilder.addCustomProperty(
                        AbstractFunction.KEY_METRICS_HOST, metricsHost);

                final SinkGroupConfig sinkGroupConfig = sinkGroupConfigBuilder.build();
                final SinkGroupManager sinkGroupManager =
                        new SinkGroupManager(groupId, channel, sinkGroupConfig);
                sinkGroupManagerMap
                        .computeIfAbsent(groupId, k -> new ArrayList<>())
                        .add(sinkGroupManager);
            }
        }
        sinkGroupManagerMap.forEach(
                (k, vs) -> vs.forEach(SinkGroupManager::createPartitionHandlesAndStart));
    }

    /**
     * create sink group config builder by groupId.
     *
     * @param groupId groupId
     * @return SinkGroupConfigBuilder
     */
    private SinkGroupConfigBuilder createSinkGroupConfigBuilderByGroupId(String groupId) {

        final String sinkHandlerIdentity =
                dynamicSinkValue(groupId, KEY_SINK_HANDLER_IDENTITY, DEFAULT_SINK_HANDLER_IDENTITY);
        final String functionIdentity = dynamicSinkValue(groupId, KEY_SINK_FUNCTION_IDENTITY, null);
        final SinkGroupConfigBuilder configBuilder =
                SinkGroupConfigBuilder.newBuilder()
                        .functionIdentity(functionIdentity)
                        .handlerIdentity(sinkHandlerIdentity);

        // add custom property.
        final String keyPrefix = groupId + ".";
        for (Map.Entry<String, String> entry : sinkGroupManagerConfiguration.getSink().entrySet()) {
            final String key = entry.getKey();
            final int index = key.indexOf(keyPrefix);
            if (index == 0) {
                configBuilder.addCustomProperty(
                        key.substring(keyPrefix.length()), entry.getValue());
            }
        }
        return configBuilder;
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
        final String value = sinkGroupManagerConfiguration.getSink().get(realKey);
        return value == null ? defaultValue : value;
    }

    /** init sink group configs. */
    private Map<String, SinkGroupConfigBuilder> buildSinkGroupConfigs() {
        Map<String, SinkGroupConfigBuilder> sinkGroupConfigs = new HashMap<>();
        for (String groupId : getAllGroups()) {
            sinkGroupConfigs.put(groupId, createSinkGroupConfigBuilderByGroupId(groupId));
        }
        return sinkGroupConfigs;
    }

    /**
     * parser groups.
     *
     * @return group list
     */
    private Set<String> getAllGroups() {
        final Set<String> topics = new HashSet<>();
        for (Map.Entry<String, String> entry : sinkGroupManagerConfiguration.getSink().entrySet()) {
            final String key = entry.getKey();
            final String[] keySplit = key.split("\\.");
            topics.add(keySplit[0]);
        }
        return topics;
    }

    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            sinkGroupManagerMap.forEach((k, vs) -> vs.forEach(IOUtils::closeQuietly));
        }
    }
}
