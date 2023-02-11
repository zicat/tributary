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

import java.util.*;
import java.util.concurrent.TimeUnit;

import static org.zicat.tributary.channel.ChannelConfigOption.*;

/** MemoryChannelFactory. */
public class MemoryChannelFactory implements ChannelFactory {

    public static final String SPLIT_STR = ",";
    public static final String TYPE = "memory";

    @Override
    public String type() {
        return TYPE;
    }

    @Override
    public Channel createChannel(String topic, ReadableConfig config) {
        final String groupIds = config.get(OPTION_GROUPS);
        final Set<String> groupSet = new HashSet<>(Arrays.asList(groupIds.split(SPLIT_STR)));
        final int partitionCounts = config.get(OPTION_PARTITION_COUNT);
        final int blockSize = config.get(OPTION_BLOCK_SIZE);
        final long segmentSize = config.get(OPTION_SEGMENT_SIZE);
        final CompressionType compression =
                CompressionType.getByName(config.get(OPTION_COMPRESSION));
        final int flushPeriodMills = config.get(OPTION_FLUSH_PERIOD_MILLS);
        final MemoryChannel[] channels =
                createChannels(
                        topic, partitionCounts, groupSet, blockSize, segmentSize, compression);
        return new DefaultChannel<>(channels, flushPeriodMills, TimeUnit.MILLISECONDS);
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

        final MemoryChannel[] channels = new MemoryChannel[partitionCount];
        final Map<String, RecordsOffset> groupOffsets = new HashMap<>();
        for (String group : groups) {
            groupOffsets.put(group, RecordsOffset.startRecordOffset());
        }
        for (int i = 0; i < partitionCount; i++) {
            final SingleGroupManager groupManager =
                    MemoryGroupManager.createUnPersistGroupManager(groupOffsets);
            channels[i] =
                    createMemoryChannel(
                            topic, groupManager, blockSize, segmentSize, compressionType);
        }
        return channels;
    }

    /**
     * create memory channel.
     *
     * @param topic topic
     * @param groupManager groupManager
     * @param blockSize blockSize
     * @param segmentSize segmentSize
     * @param compressionType compressionType
     * @return MemoryChannel
     */
    public static MemoryChannel createMemoryChannel(
            String topic,
            SingleGroupManager groupManager,
            int blockSize,
            long segmentSize,
            CompressionType compressionType) {
        final MemoryChannel memoryChannel =
                new MemoryChannel(topic, groupManager, blockSize, segmentSize, compressionType);
        memoryChannel.loadLastSegment();
        return memoryChannel;
    }
}
