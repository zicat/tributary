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

import org.apache.hadoop.fs.FileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.zicat.tributary.channel.Channel;
import org.zicat.tributary.channel.ChannelFactory;
import org.zicat.tributary.channel.file.FileChannelFactory;
import org.zicat.tributary.common.*;
import org.zicat.tributary.service.configuration.ChannelConfiguration;

import javax.annotation.PostConstruct;
import java.io.Closeable;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

/** DynamicChannel. */
@Component
public class DynamicChannel implements Closeable {

    private static final Logger LOG = LoggerFactory.getLogger(DynamicChannel.class);

    private static final ConfigOption<String> OPTION_TYPE =
            ConfigOptions.key("type")
                    .stringType()
                    .description("channel type")
                    .defaultValue(FileChannelFactory.TYPE);

    @Autowired ChannelConfiguration channelConfiguration;
    private final AtomicBoolean closed = new AtomicBoolean(false);

    /* key:channel id, value: channel instance */
    final Map<String, Channel> channels = new HashMap<>();

    @PostConstruct
    public void init() throws Throwable {
        if (channelConfiguration.getChannel() == null
                || channelConfiguration.getChannel().isEmpty()) {
            return;
        }
        try {
            for (String topic : getAllTopics()) {
                final ReadableConfig readableConfig = getTopicParams(topic);
                final String type = readableConfig.get(OPTION_TYPE);
                final ChannelFactory factory = ChannelFactory.findChannelFactory(type);
                channels.put(topic, factory.createChannel(topic, readableConfig));
            }
        } catch (Throwable e) {
            IOUtils.closeQuietly(this);
            throw e;
        }
    }

    /**
     * get channel params.
     *
     * @param channel channel
     * @return ReadableConfig
     */
    private ReadableConfig getTopicParams(String channel) {
        final DefaultReadableConfig config = new DefaultReadableConfig();
        final String prefix = channel + ".";
        for (Map.Entry<String, String> entry : channelConfiguration.getChannel().entrySet()) {
            final String key = entry.getKey();
            final int index = key.indexOf(prefix);
            if (index != -1) {
                config.put(key.substring(index + prefix.length()), entry.getValue());
            }
        }
        return config;
    }

    /**
     * parser channels.
     *
     * @return topic list
     */
    private Set<String> getAllTopics() {
        final Set<String> topics = new HashSet<>();
        for (Map.Entry<String, String> entry : channelConfiguration.getChannel().entrySet()) {
            final String key = entry.getKey();
            final String[] keySplit = key.split("\\.");
            topics.add(keySplit[0]);
        }
        return topics;
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
    public void close() throws IOException {
        if (closed.compareAndSet(false, true)) {
            try {
                channels.forEach((k, v) -> IOUtils.closeQuietly(v));
            } finally {
                FileSystem.closeAll();
            }
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
