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

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.zicat.tributary.channel.Channel;
import org.zicat.tributary.common.*;
import org.zicat.tributary.service.configuration.SinkGroupManagerConfiguration;
import org.zicat.tributary.sink.SinkGroupConfig;
import org.zicat.tributary.sink.SinkGroupConfigBuilder;
import org.zicat.tributary.sink.SinkGroupManager;
import org.zicat.tributary.sink.handler.DirectPartitionHandlerFactory;
import org.zicat.tributary.sink.utils.HostUtils;

import javax.annotation.PostConstruct;
import java.io.Closeable;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.zicat.tributary.sink.function.AbstractFunction.OPTION_METRICS_HOST;
import static org.zicat.tributary.sink.handler.AbstractPartitionHandler.OPTION_MAX_RETAIN_SIZE;

/** DynamicSinkGroupManager. */
@Component
public class DynamicSinkGroupManager implements Closeable {

    public static final ConfigOption<String> OPTION_SINK_HANDLER_ID =
            ConfigOptions.key("partitionHandlerIdentity")
                    .stringType()
                    .description(
                            "the id of partition handler, support [direct,multi_thread], default direct")
                    .defaultValue(DirectPartitionHandlerFactory.IDENTITY);

    public static final ConfigOption<String> OPTION_FUNCTION_ID =
            ConfigOptions.key("functionIdentity")
                    .stringType()
                    .description("the id of function")
                    .noDefaultValue();

    private final AtomicBoolean closed = new AtomicBoolean(false);

    /* key:group id, value: sink group manager consumed by the key */
    final Map<String, List<SinkGroupManager>> sinkGroupManagerMap = new HashMap<>();

    @Autowired SinkGroupManagerConfiguration sinkGroupManagerConfiguration;
    @Autowired DynamicChannel dynamicChannel;

    @Value("${server.metrics.ip.pattern:.*}")
    String metricsIpPattern;

    DefaultReadableConfig readableConfig;

    @PostConstruct
    public void init() {
        readableConfig = new DefaultReadableConfig();
        readableConfig.putAll(sinkGroupManagerConfiguration.getSink());
        initSinkGroupManagers();
    }

    /** init all sink group manager and start. */
    private void initSinkGroupManagers() {

        final Map<String, SinkGroupConfigBuilder> sinkGroupConfigs = buildSinkGroupConfigs();
        final String metricsHost = getMetricsIp();
        for (Map.Entry<String, SinkGroupConfigBuilder> entry : sinkGroupConfigs.entrySet()) {

            final String groupId = entry.getKey();
            final List<Channel> channels = dynamicChannel.findChannels(groupId);
            if (channels.isEmpty()) {
                throw new TributaryRuntimeException(
                        "group id not found channel, groupId = " + groupId);
            }
            for (Channel channel : channels) {
                final SinkGroupConfigBuilder sinkGroupConfigBuilder = entry.getValue();
                sinkGroupConfigBuilder.addCustomProperty(
                        OPTION_MAX_RETAIN_SIZE.key(),
                        dynamicSinkValue(groupId, OPTION_MAX_RETAIN_SIZE));
                sinkGroupConfigBuilder.addCustomProperty(OPTION_METRICS_HOST.key(), metricsHost);

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

        final SinkGroupConfigBuilder configBuilder =
                SinkGroupConfigBuilder.newBuilder()
                        .functionIdentity(dynamicSinkValue(groupId, OPTION_FUNCTION_ID))
                        .handlerIdentity(dynamicSinkValue(groupId, OPTION_SINK_HANDLER_ID));
        // add custom property.
        final String keyPrefix = groupId + ".";
        for (Map.Entry<String, Object> entry : readableConfig.entrySet()) {
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
     * get dynamic nullable sink value.
     *
     * @param groupId groupId
     * @param configOption configOption
     * @return string value
     */
    private <T> T dynamicSinkValue(String groupId, ConfigOption<T> configOption) {
        final String realKey = String.join(".", groupId, configOption.key());
        final ConfigOption<T> newConfigOption = configOption.changeKey(realKey);
        return readableConfig.get(newConfigOption);
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
        for (Map.Entry<String, Object> entry : readableConfig.entrySet()) {
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

    /**
     * get metrics ip.
     *
     * @return ip
     */
    public String getMetricsIp() {
        return HostUtils.getLocalHostString(metricsIpPattern);
    }

    /**
     * get all sink group manager.
     *
     * @return sink group managers map
     */
    public Map<String, List<SinkGroupManager>> getSinkGroupManagerMap() {
        return sinkGroupManagerMap;
    }
}
