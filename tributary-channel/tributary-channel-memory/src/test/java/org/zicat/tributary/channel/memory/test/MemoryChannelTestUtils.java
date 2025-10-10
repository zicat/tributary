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

package org.zicat.tributary.channel.memory.test;

import org.zicat.tributary.channel.*;
import org.zicat.tributary.channel.DefaultChannel.AbstractChannelArrayFactory;
import org.zicat.tributary.channel.memory.MemoryChannelFactory;
import org.zicat.tributary.channel.memory.MemorySingleChannel;
import org.zicat.tributary.common.ReadableConfig;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/** MemoryChannelTestUtils. */
public class MemoryChannelTestUtils {

    /**
     * createMemoryChannelFactory.
     *
     * @param groupIds groupIds
     * @return ChannelFactory
     */
    public static ChannelFactory memoryChannelFactory(String... groupIds) {
        return memoryChannelFactory(1, groupIds);
    }

    /**
     * createMemoryChannelFactory.
     *
     * @param partitions partitions
     * @param groupIds groupIds
     * @return ChannelFactory
     */
    public static ChannelFactory memoryChannelFactory(int partitions, String... groupIds) {
        return new ChannelFactory() {
            @Override
            public String type() {
                return "memory";
            }

            @Override
            public Channel createChannel(String topic, ReadableConfig readableConfig)
                    throws Exception {
                return MemoryChannelTestUtils.createChannel(topic, partitions, groupIds);
            }
        };
    }

    /**
     * create channel.
     *
     * @param topic topic
     * @param partitionCount partitionCount
     * @param groupIds groupIds
     * @return channel
     * @throws IOException IOException
     */
    public static Channel createChannel(String topic, int partitionCount, String... groupIds)
            throws IOException {
        return createChannel(topic, partitionCount, 1024 * 3, 102400L, groupIds);
    }

    /**
     * create channel.
     *
     * @param topic topic
     * @param partitionCount partitionCount
     * @param groupIds groupIds
     * @return channel
     * @throws IOException IOException
     */
    public static Channel createChannel(
            String topic, int partitionCount, int blockSize, long segmentSize, String... groupIds)
            throws IOException {
        return createChannel(
                topic, partitionCount, blockSize, segmentSize, CompressionType.SNAPPY, groupIds);
    }

    /**
     * create channel.
     *
     * @param topic topic
     * @param partitionCount partitionCount
     * @param groupIds groupIds
     * @return channel
     * @throws IOException IOException
     */
    public static DefaultChannel<MemorySingleChannel> createChannel(
            String topic,
            int partitionCount,
            int blockSize,
            long segmentSize,
            CompressionType compressionType,
            String... groupIds)
            throws IOException {
        final Long[] capacityProtectedList = new Long[partitionCount];
        Arrays.fill(capacityProtectedList, Runtime.getRuntime().maxMemory());
        return createChannel(
                topic,
                partitionCount,
                blockSize,
                segmentSize,
                compressionType,
                capacityProtectedList,
                groupIds);
    }

    public static DefaultChannel<MemorySingleChannel> createChannel(
            String topic,
            int partitionCount,
            int blockSize,
            long segmentSize,
            CompressionType compressionType,
            Long[] capacityProtectedList,
            String... groupIds)
            throws IOException {
        final Set<String> groups = new HashSet<>(Arrays.asList(groupIds));
        return new DefaultChannel<>(
                new AbstractChannelArrayFactory<MemorySingleChannel>(topic, groups) {
                    @Override
                    public MemorySingleChannel[] create() {
                        return MemoryChannelFactory.createChannels(
                                topic,
                                partitionCount,
                                groups,
                                blockSize,
                                segmentSize,
                                compressionType);
                    }
                },
                500,
                10000,
                capacityProtectedList);
    }

    /**
     * create channel.
     *
     * @param topic topic
     * @param groupIds groupIds
     * @return channel
     * @throws IOException IOException
     */
    public static Channel createChannel(String topic, String... groupIds) throws IOException {
        return createChannel(topic, 1, groupIds);
    }
}
