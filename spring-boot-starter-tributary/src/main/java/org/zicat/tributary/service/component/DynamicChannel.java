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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.zicat.tributary.channel.Channel;
import org.zicat.tributary.channel.ChannelFactory;
import org.zicat.tributary.channel.file.FileChannelFactory;
import org.zicat.tributary.common.*;
import org.zicat.tributary.service.configuration.ChannelConfiguration;

import java.io.Closeable;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.zicat.tributary.common.ReadableConfig.DEFAULT_KEY_HANDLER;

/** DynamicChannel. */
@Component
public class DynamicChannel implements Closeable {

    private static final Logger LOG = LoggerFactory.getLogger(DynamicChannel.class);

    private static final ConfigOption<String> OPTION_TYPE =
            ConfigOptions.key("type")
                    .stringType()
                    .description("channel type")
                    .defaultValue(FileChannelFactory.TYPE);

    /* key:channel id, value: channel instance */
    private final Map<String, Channel> channels = new HashMap<>();
    private final AtomicBoolean closed = new AtomicBoolean(false);

    public DynamicChannel(@Autowired ChannelConfiguration channelConfiguration) throws Exception {
        final ReadableConfig allChannelConfig =
                ReadableConfig.create(channelConfiguration.getChannel());
        final Set<String> topics = allChannelConfig.groupKeys(DEFAULT_KEY_HANDLER);
        try {
            for (String topic : topics) {
                final String head = topic + ".";
                final ReadableConfig topicConfig = allChannelConfig.filterAndRemovePrefixKey(head);
                final String type = topicConfig.get(OPTION_TYPE);
                final ChannelFactory factory = SpiFactory.findFactory(type, ChannelFactory.class);
                channels.put(topic, factory.createChannel(topic, topicConfig));
            }
            LOG.info("create channel success, topics {}", topics);
        } catch (Throwable e) {
            IOUtils.closeQuietly(this);
            throw e;
        }
    }

    /**
     * get channel by topic.
     *
     * @param topic topic
     * @return Channel
     */
    public Channel getChannel(String topic) {
        return channels.get(topic);
    }

    /**
     * return channel count. for test
     *
     * @return int size
     */
    public int size() {
        return channels.size();
    }

    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            channels.forEach((k, v) -> IOUtils.closeQuietly(v));
        }
    }

    /** flush all channel. */
    public void flushAll() {
        for (Map.Entry<String, Channel> entry : channels.entrySet()) {
            try {
                entry.getValue().flush();
            } catch (Exception e) {
                LOG.warn("flush error", e);
            }
        }
    }

    /**
     * find channel by group id.
     *
     * @param groupId group id
     * @return channel set
     */
    public List<Channel> findChannels(String groupId) {
        final List<Channel> result = new ArrayList<>();
        for (Channel channel : channels.values()) {
            if (channel.groups().contains(groupId)) {
                result.add(channel);
            }
        }
        return result;
    }

    /**
     * get all channels.
     *
     * @return channel.
     */
    public Map<String, Channel> getChannels() {
        return channels;
    }
}
