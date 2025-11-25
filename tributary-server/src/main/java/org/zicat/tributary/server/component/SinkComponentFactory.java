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

package org.zicat.tributary.server.component;

import io.prometheus.client.CollectorRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zicat.tributary.channel.Channel;
import org.zicat.tributary.common.*;
import org.zicat.tributary.common.config.ConfigOption;
import org.zicat.tributary.common.config.ConfigOptions;
import org.zicat.tributary.common.config.ReadableConfig;
import org.zicat.tributary.common.exception.TributaryRuntimeException;
import org.zicat.tributary.common.util.IOUtils;
import org.zicat.tributary.server.component.SinkComponent.SinkGroupManagerList;
import org.zicat.tributary.sink.config.SinkGroupConfig;
import org.zicat.tributary.sink.config.SinkGroupConfigBuilder;
import org.zicat.tributary.sink.SinkGroupManager;
import org.zicat.tributary.sink.handler.DefaultPartitionHandlerFactory;

import java.util.*;

import static org.zicat.tributary.common.config.ReadableConfig.DEFAULT_KEY_HANDLER;

/** SinkComponentFactory. */
public class SinkComponentFactory implements SafeFactory<SinkComponent> {

    private static final Logger LOG = LoggerFactory.getLogger(SinkComponentFactory.class);
    private static final String CREATE_SUCCESS_LOG =
            "create sink group manager success, groupId={}, topic={}, config={}";
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

    private final ReadableConfig sinkConfig;
    private final ChannelComponent channelComponent;
    private final CollectorRegistry registry;

    public SinkComponentFactory(
            ReadableConfig sinkConfig,
            ChannelComponent channelComponent,
            CollectorRegistry registry) {
        this.sinkConfig = sinkConfig;
        this.channelComponent = channelComponent;
        this.registry = registry;
    }

    @Override
    public SinkComponent create() {
        final Map<String, SinkGroupManagerList> sinkGroupManagers = new HashMap<>();
        final Map<String, SinkGroupConfigBuilder> sinkGroupConfigs = buildSinkGroupConfigs();
        try {
            for (Map.Entry<String, SinkGroupConfigBuilder> entry : sinkGroupConfigs.entrySet()) {
                final String group = entry.getKey();
                final List<Channel> channels = channelComponent.findChannels(group);
                if (channels.isEmpty()) {
                    throw new TributaryRuntimeException("group " + group + " not found channel");
                }
                final SinkGroupManagerList managerList = new SinkGroupManagerList(group);
                sinkGroupManagers.put(group, managerList);
                for (Channel channel : channels) {
                    final SinkGroupConfig config = entry.getValue().build();
                    final SinkGroupManager sinkGroupManager =
                            new SinkGroupManager(group, channel, config);
                    sinkGroupManager.createPartitionHandlesAndStart();
                    LOG.info(CREATE_SUCCESS_LOG, group, channel.topic(), config);
                    managerList.add(sinkGroupManager);
                }
            }
            return new SinkComponent(sinkGroupManagers, registry);
        } catch (Exception e) {
            sinkGroupManagers.forEach((k, v) -> IOUtils.closeQuietly(v));
            throw new TributaryRuntimeException(e);
        }
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
        final String keyPrefix = groupId + ".";
        final ReadableConfig groupConfig = sinkConfig.filterAndRemovePrefixKey(keyPrefix);
        groupConfig.forEach(configBuilder::addConfig);
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
        return sinkConfig.get(newConfigOption);
    }

    /** init sink group configs. */
    private Map<String, SinkGroupConfigBuilder> buildSinkGroupConfigs() {
        final Map<String, SinkGroupConfigBuilder> sinkGroupConfigs = new HashMap<>();
        final Set<String> groups = sinkConfig.groupKeys(DEFAULT_KEY_HANDLER);
        for (String groupId : groups) {
            sinkGroupConfigs.put(groupId, createSinkGroupConfigBuilderByGroupId(groupId));
        }
        return sinkGroupConfigs;
    }
}
