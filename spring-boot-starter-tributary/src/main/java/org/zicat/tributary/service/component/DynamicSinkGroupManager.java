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
import org.zicat.tributary.sink.handler.DefaultPartitionHandlerFactory;
import org.zicat.tributary.sink.utils.HostUtils;

import java.io.Closeable;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.zicat.tributary.common.ReadableConfig.DEFAULT_KEY_HANDLER;
import static org.zicat.tributary.sink.function.AbstractFunction.OPTION_METRICS_HOST;
import static org.zicat.tributary.sink.handler.AbstractPartitionHandler.OPTION_MAX_RETAIN_SIZE;

/** DynamicSinkGroupManager. */
@Component
public class DynamicSinkGroupManager implements Closeable {

    public static final ConfigOption<String> OPTION_SINK_HANDLER_ID =
            ConfigOptions.key("partition.handler.id")
                    .stringType()
                    .description(
                            "the id of partition handler, support [direct,multi_thread,default], default default")
                    .defaultValue(DefaultPartitionHandlerFactory.IDENTITY);

    public static final ConfigOption<String> OPTION_FUNCTION_ID =
            ConfigOptions.key("function.id")
                    .stringType()
                    .description("the id of function")
                    .noDefaultValue();

    /* key:group id, value: sink group manager consumed by the key */
    private final Map<String, List<SinkGroupManager>> sinkGroupManagerMap = new HashMap<>();
    private final AtomicBoolean closed = new AtomicBoolean(false);

    private final DynamicChannel dynamicChannel;
    private final ReadableConfig allSinkConfig;
    private final String metricsIp;

    public DynamicSinkGroupManager(
            @Autowired DynamicChannel dynamicChannel,
            @Autowired SinkGroupManagerConfiguration sinkGroupManagerConfiguration,
            @Value("${server.metrics.ip.pattern:.*}") String metricsIpPattern) {
        this.dynamicChannel = dynamicChannel;
        this.allSinkConfig = ReadableConfig.create(sinkGroupManagerConfiguration.getSink());
        this.metricsIp = HostUtils.getLocalHostString(metricsIpPattern);
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
        final ReadableConfig groupConfig = allSinkConfig.filterAndRemovePrefixKey(keyPrefix);
        groupConfig.forEach(configBuilder::addCustomProperty);
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
        final ConfigOption<T> newConfigOption = configOption.concatHead(groupId + ".");
        return allSinkConfig.get(newConfigOption);
    }

    /** init sink group configs. */
    private Map<String, SinkGroupConfigBuilder> buildSinkGroupConfigs() {
        final Map<String, SinkGroupConfigBuilder> sinkGroupConfigs = new HashMap<>();
        final Set<String> groups = allSinkConfig.groupKeys(DEFAULT_KEY_HANDLER);
        for (String groupId : groups) {
            sinkGroupConfigs.put(groupId, createSinkGroupConfigBuilderByGroupId(groupId));
        }
        return sinkGroupConfigs;
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
        return metricsIp;
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
