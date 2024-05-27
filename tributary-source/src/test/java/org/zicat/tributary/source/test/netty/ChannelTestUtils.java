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

package org.zicat.tributary.source.test.netty;

import org.zicat.tributary.channel.Channel;
import org.zicat.tributary.channel.ChannelFactory;
import org.zicat.tributary.channel.CompressionType;
import org.zicat.tributary.channel.DefaultChannel;
import org.zicat.tributary.channel.memory.MemoryChannel;
import org.zicat.tributary.channel.memory.MemoryChannelFactory;
import org.zicat.tributary.common.ReadableConfig;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/** ChannelTestUtils. */
public class ChannelTestUtils {

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
                return new DefaultChannel<>(
                        new DefaultChannel.AbstractChannelArrayFactory<MemoryChannel>() {
                            @Override
                            public String topic() {
                                return topic;
                            }

                            @Override
                            public Set<String> groups() {
                                return new HashSet<>(Arrays.asList(groupIds));
                            }

                            @Override
                            public MemoryChannel[] create() {
                                return MemoryChannelFactory.createChannels(
                                        topic,
                                        partitions,
                                        new HashSet<>(Arrays.asList(groupIds)),
                                        1024 * 3,
                                        102400L,
                                        CompressionType.SNAPPY);
                            }
                        },
                        0);
            }
        };
    }
}
