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
import org.zicat.tributary.channel.AbstractSingleChannel.SingleGroupManagerFactory;
import org.zicat.tributary.channel.DefaultChannel.AbstractChannelArrayFactory;
import static org.zicat.tributary.channel.Offset.UNINITIALIZED_OFFSET;
import static org.zicat.tributary.channel.memory.MemoryChannelConfigOption.OPTION_CAPACITY_PROTECTED_PERCENT;
import org.zicat.tributary.common.MemorySize;
import org.zicat.tributary.common.ReadableConfig;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static org.zicat.tributary.channel.ChannelConfigOption.*;
import static org.zicat.tributary.channel.group.MemoryGroupManager.createSingleGroupManagerFactory;

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
        final int partitionCounts = config.get(OPTION_PARTITION_COUNT);
        final MemorySize blockSize = config.get(OPTION_BLOCK_SIZE);
        final MemorySize segmentSize = config.get(MemoryChannelConfigOption.OPTION_SEGMENT_SIZE);
        final CompressionType compression = config.get(OPTION_COMPRESSION);
        final int blockCount = config.get(OPTION_BLOCK_CACHE_PER_PARTITION_SIZE);
        final Duration flushPeriod = config.get(OPTION_FLUSH_PERIOD);
        final Duration cleanupExpiredSegmentPeriod =
                config.get(OPTION_CLEANUP_EXPIRED_SEGMENT_PERIOD);
        final double capacityPercent = config.get(OPTION_CAPACITY_PROTECTED_PERCENT).getPercent();
        final long capacity = (long) (Runtime.getRuntime().maxMemory() * capacityPercent);
        final Long[] capacityProtectedList = new Long[partitionCounts];
        Arrays.fill(capacityProtectedList, capacity);

        return new DefaultChannel<>(
                new AbstractChannelArrayFactory<AbstractSingleChannel<?>>(topic, groupSet) {
                    @Override
                    public AbstractSingleChannel<?>[] create() {
                        return createChannels(
                                topic,
                                partitionCounts,
                                groupSet,
                                (int) blockSize.getBytes(),
                                segmentSize.getBytes(),
                                compression,
                                blockCount);
                    }
                },
                flushPeriod.toMillis(),
                cleanupExpiredSegmentPeriod.toMillis(),
                capacityProtectedList);
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
    public static MemorySingleChannel[] createChannels(
            String topic,
            int partitionCount,
            Set<String> groups,
            Integer blockSize,
            Long segmentSize,
            CompressionType compressionType,
            int blockCount) {
        final MemorySingleChannel[] channels = new MemorySingleChannel[partitionCount];
        final Map<String, Offset> groupOffsets = new HashMap<>();
        for (String group : groups) {
            groupOffsets.put(group, UNINITIALIZED_OFFSET);
        }
        final SingleGroupManagerFactory factory = createSingleGroupManagerFactory(groupOffsets);
        for (int i = 0; i < partitionCount; i++) {
            channels[i] =
                    createMemorySingleChannel(
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
    public static MemorySingleChannel[] createChannels(
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
    public static MemorySingleChannel createMemorySingleChannel(
            String topic,
            SingleGroupManagerFactory factory,
            int blockSize,
            long segmentSize,
            CompressionType compressionType,
            int blockCount) {
        return new MemorySingleChannel(
                topic, factory, blockSize, segmentSize, compressionType, blockCount);
    }
}
