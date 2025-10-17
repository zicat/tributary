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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zicat.tributary.channel.Channel;
import org.zicat.tributary.channel.ChannelFactory;
import org.zicat.tributary.channel.file.FileChannelFactory;
import org.zicat.tributary.common.*;

import java.util.*;

import org.zicat.tributary.common.config.ReadableConfig;
import static org.zicat.tributary.common.config.ReadableConfig.DEFAULT_KEY_HANDLER;
import org.zicat.tributary.common.config.ConfigOption;
import org.zicat.tributary.common.config.ConfigOptions;
import org.zicat.tributary.common.exception.TributaryRuntimeException;
import org.zicat.tributary.common.util.IOUtils;

/** ChannelComponentFactory. */
public class ChannelComponentFactory implements SafeFactory<ChannelComponent> {

    private static final Logger LOG = LoggerFactory.getLogger(ChannelComponentFactory.class);
    private static final ConfigOption<String> OPTION_TYPE =
            ConfigOptions.key("type")
                    .stringType()
                    .description("channel type")
                    .defaultValue(FileChannelFactory.TYPE);

    private final ReadableConfig channelConfig;
    private final String metricsHost;

    public ChannelComponentFactory(ReadableConfig channelConfig, String metricsHost) {
        this.channelConfig = channelConfig;
        this.metricsHost = metricsHost;
    }

    @Override
    public ChannelComponent create() {
        final Set<String> topics = channelConfig.groupKeys(DEFAULT_KEY_HANDLER);
        final Map<String, Channel> channels = new HashMap<>();
        try {
            for (String topic : topics) {
                final String head = topic + ".";
                final ReadableConfig topicConfig = channelConfig.filterAndRemovePrefixKey(head);
                final String type = topicConfig.get(OPTION_TYPE);
                final ChannelFactory factory = SpiFactory.findFactory(type, ChannelFactory.class);
                channels.put(topic, factory.createChannel(topic, topicConfig));
            }
            LOG.info("create channel success, topics {}", topics);
            return new ChannelComponent(channels, metricsHost);
        } catch (Exception e) {
            channels.forEach((k, v) -> IOUtils.closeQuietly(v));
            throw new TributaryRuntimeException(e);
        }
    }
}
