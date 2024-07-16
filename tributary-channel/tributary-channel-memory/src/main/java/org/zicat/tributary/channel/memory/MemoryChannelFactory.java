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

package org.zicat.tributary.channel.memory;

import org.zicat.tributary.channel.*;
import org.zicat.tributary.common.ReadableConfig;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import static org.zicat.tributary.channel.ChannelConfigOption.*;
import static org.zicat.tributary.channel.group.MemoryGroupManager.createMemoryGroupManagerFactory;
import static org.zicat.tributary.channel.group.MemoryGroupManager.defaultGroupOffset;

/** MemoryChannelFactory. */
public class MemoryChannelFactory implements ChannelFactory {

    public static final String TYPE = "memory";

    @Override
    public String type() {
        return TYPE;
    }

    @Override
    public Channel createChannel(String topic, ReadableConfig config) throws IOException {
        final Set<String> groupSet = groupSet(config);
        return new DefaultChannel<>(
                new DefaultChannel.AbstractChannelArrayFactory<AbstractChannel<?>>() {
                    @Override
                    public String topic() {
                        return topic;
                    }

                    @Override
                    public Set<String> groups() {
                        return groupSet;
                    }

                    @Override
                    public AbstractChannel<?>[] create() {
                        final int partitionCounts = config.get(OPTION_PARTITION_COUNT);
                        final int blockSize = (int) config.get(OPTION_BLOCK_SIZE).getBytes();
                        final long segmentSize = config.get(OPTION_SEGMENT_SIZE).getBytes();
                        final CompressionType compression = config.get(OPTION_COMPRESSION);
                        final int blockCount = config.get(OPTION_BLOCK_CACHE_PER_PARTITION_SIZE);
                        return createChannels(
                                topic,
                                partitionCounts,
                                groupSet,
                                blockSize,
                                segmentSize,
                                compression,
                                blockCount);
                    }
                },
                config.get(OPTION_FLUSH_PERIOD).toMillis());
    }

    /**
     * create channels.
     *
     * @param topic topic
     * @param partitionCount partitionCount
     * @param groups groups
     * @param blockSize blockSize
     * @param segmentSize segmentSize
     * @param compressionType compressionType
     * @return MemoryChannel arrays
     */
    @SuppressWarnings("resource")
    public static MemoryChannel[] createChannels(
            String topic,
            int partitionCount,
            Set<String> groups,
            Integer blockSize,
            Long segmentSize,
            CompressionType compressionType,
            int blockCount) {

        final MemoryChannel[] channels = new MemoryChannel[partitionCount];
        final Set<GroupOffset> groupOffsets = new HashSet<>();
        for (String group : groups) {
            groupOffsets.add(defaultGroupOffset(group));
        }
        final AbstractChannel.MemoryGroupManagerFactory factory =
                createMemoryGroupManagerFactory(groupOffsets);
        for (int i = 0; i < partitionCount; i++) {
            channels[i] =
                    createMemoryChannel(
                            topic, factory, blockSize, segmentSize, compressionType, blockCount);
        }
        return channels;
    }

    /**
     * create channels.
     *
     * @param topic topic
     * @param partitionCount partitionCount
     * @param groups groups
     * @param blockSize blockSize
     * @param segmentSize segmentSize
     * @param compressionType compressionType
     * @return MemoryChannel arrays
     */
    public static MemoryChannel[] createChannels(
            String topic,
            int partitionCount,
            Set<String> groups,
            Integer blockSize,
            Long segmentSize,
            CompressionType compressionType) {
        return createChannels(
                topic,
                partitionCount,
                groups,
                blockSize,
                segmentSize,
                compressionType,
                OPTION_BLOCK_CACHE_PER_PARTITION_SIZE.defaultValue());
    }

    /**
     * create memory channel.
     *
     * @param topic topic
     * @param factory factory
     * @param blockSize blockSize
     * @param segmentSize segmentSize
     * @param compressionType compressionType
     * @param blockCount blockCount
     * @return MemoryChannel
     */
    public static MemoryChannel createMemoryChannel(
            String topic,
            AbstractChannel.MemoryGroupManagerFactory factory,
            int blockSize,
            long segmentSize,
            CompressionType compressionType,
            int blockCount) {
        return new MemoryChannel(
                topic, factory, blockSize, segmentSize, compressionType, blockCount);
    }
}
