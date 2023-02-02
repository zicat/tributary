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
import org.zicat.tributary.channel.utils.IOUtils;
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
    private static final String KEY_TYPE = "type";

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
            for (String channel : getAllChannels()) {
                final Map<String, String> params = getChannelParams(channel);
                final String type = params.getOrDefault(KEY_TYPE, FileChannelFactory.TYPE);
                final ChannelFactory factory = ChannelFactory.findChannelFactory(type);
                channels.put(channel, factory.createChannel(channel, params));
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
     * @return map
     */
    private Map<String, String> getChannelParams(String channel) {
        final Map<String, String> result = new HashMap<>();
        final String prefix = channel + ".";
        for (Map.Entry<String, String> entry : channelConfiguration.getChannel().entrySet()) {
            final String key = entry.getKey();
            final int index = key.indexOf(prefix);
            if (index != -1) {
                result.put(key.substring(index + prefix.length()), entry.getValue());
            }
        }
        return result;
    }

    /**
     * parser channels.
     *
     * @return topic list
     */
    private Set<String> getAllChannels() {
        final Set<String> channels = new HashSet<>();
        for (Map.Entry<String, String> entry : channelConfiguration.getChannel().entrySet()) {
            final String key = entry.getKey();
            final String[] keySplit = key.split("\\.");
            channels.add(keySplit[0]);
        }
        return channels;
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
