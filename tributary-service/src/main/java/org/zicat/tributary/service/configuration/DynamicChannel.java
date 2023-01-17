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
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;
import org.zicat.tributary.channel.Channel;
import org.zicat.tributary.channel.CompressionType;
import org.zicat.tributary.channel.file.PartitionFileChannelBuilder;
import org.zicat.tributary.channel.utils.IOUtils;

import javax.annotation.PostConstruct;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

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

    private static final String KEY_FILE_FLUSH_PERIOD_MILLS = "flushPeriodMills";
    private static final String DEFAULT_FILE_FLUSH_PERIOD_MILLS = String.valueOf(500);

    private static final String KEY_FILE_FLUSH_FORCE = "flushForce";
    private static final String DEFAULT_FILE_FLUSH_FORCE = "false";

    private static final String KEY_FILE_COMPRESSION = "compression";
    private static final String DEFAULT_FILE_COMPRESSION = "none";

    private static final String KEY_FILE_FLUSH_PAGE_CACHE_SIZE = "flushPageCacheSize";
    private static final String DEFAULT_FILE_FLUSH_PAGE_CACHE_SIZE =
            String.valueOf(1024L * 1024L * 32L);

    private static final String KEY_GROUPS = "groups";

    private final AtomicBoolean closed = new AtomicBoolean(false);

    /* key:channel id, value: channel instance */
    final Map<String, Channel> channels = new HashMap<>();
    Map<String, String> channel;

    @PostConstruct
    public void init() throws IOException {
        if (channel == null || channel.isEmpty()) {
            return;
        }
        try {
            for (String channel : getAllChannels()) {
                initChannelByTopic(channel);
            }
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
     * get dynamic field value.
     *
     * @param channelId channelId
     * @param key key
     * @param defaultValue defaultValue
     * @return string value
     */
    private String dynamicChannelValue(String channelId, String key, String defaultValue) {
        final String realKey = String.join(".", channelId, key);
        final String value = channel.get(realKey);
        if (value == null && defaultValue == null) {
            throw new IllegalStateException("file key not exist in configuration " + realKey);
        }
        return value == null ? defaultValue : value;
    }

    /**
     * init channel by topic.
     *
     * @param topic topic
     */
    private void initChannelByTopic(String topic) throws IOException {
        final String groupIds = dynamicChannelValue(topic, KEY_GROUPS, null);
        if (groupIds == null) {
            throw new IllegalStateException("topic has no sink group " + topic);
        }
        final Set<String> groupSet = new HashSet<>(Arrays.asList(groupIds.split(SPLIT_STR)));
        final List<String> dirs =
                Arrays.asList(dynamicChannelValue(topic, KEY_FILE_DIRS, null).split(SPLIT_STR));
        final int blockSize =
                Integer.parseInt(
                        dynamicChannelValue(topic, KEY_FILE_BLOCK_SIZE, DEFAULT_FILE_BLOCK_SIZE));
        final long segmentSize =
                Long.parseLong(
                        dynamicChannelValue(
                                topic, KEY_FILE_SEGMENT_SIZE, DEFAULT_FILE_SEGMENT_SIZE));
        final int flushPeriodMills =
                Integer.parseInt(
                        dynamicChannelValue(
                                topic,
                                KEY_FILE_FLUSH_PERIOD_MILLS,
                                DEFAULT_FILE_FLUSH_PERIOD_MILLS));
        final long flushPageCacheSize =
                Long.parseLong(
                        dynamicChannelValue(
                                topic,
                                KEY_FILE_FLUSH_PAGE_CACHE_SIZE,
                                DEFAULT_FILE_FLUSH_PAGE_CACHE_SIZE));
        final String compression =
                dynamicChannelValue(topic, KEY_FILE_COMPRESSION, DEFAULT_FILE_COMPRESSION);

        final boolean flushForce =
                Boolean.parseBoolean(
                        dynamicChannelValue(topic, KEY_FILE_FLUSH_FORCE, DEFAULT_FILE_FLUSH_FORCE));

        final PartitionFileChannelBuilder builder =
                PartitionFileChannelBuilder.newBuilder().dirs(createDir(dirs));
        builder.blockSize(blockSize)
                .segmentSize(segmentSize)
                .compressionType(CompressionType.getByName(compression))
                .flushPeriod(flushPeriodMills, TimeUnit.MILLISECONDS)
                .flushPageCacheSize(flushPageCacheSize)
                .flushForce(flushForce)
                .topic(topic)
                .consumerGroups(new ArrayList<>(groupSet));
        channels.put(topic, builder.build());
    }

    private List<File> createDir(List<String> dirs) throws IOException {
        final List<File> result = new ArrayList<>();
        for (String dir : dirs) {
            if (dir.startsWith("{TMP_DIR}")) {
                File parent = Files.createTempDirectory("").toFile();
                String child = dir.replace("{TMP_DIR}", "").trim();
                child = child.startsWith("/") ? child.substring(1) : child;
                File file = child.isEmpty() ? parent : new File(parent, child);
                result.add(file);
            } else {
                result.add(new File(dir));
            }
        }
        return result;
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
}
