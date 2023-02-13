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

package org.zicat.tributary.sink.test;

import org.junit.Assert;
import org.junit.Test;
import org.zicat.tributary.channel.AbstractChannel;
import org.zicat.tributary.channel.Channel;
import org.zicat.tributary.channel.CompressionType;
import org.zicat.tributary.channel.DefaultChannel;
import org.zicat.tributary.channel.memory.MemoryChannelFactory;
import org.zicat.tributary.channle.file.test.SourceThread;
import org.zicat.tributary.common.IOUtils;
import org.zicat.tributary.common.Threads;
import org.zicat.tributary.sink.SinkGroupConfig;
import org.zicat.tributary.sink.SinkGroupConfigBuilder;
import org.zicat.tributary.sink.SinkGroupManager;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.zicat.tributary.sink.test.function.AssertCountFunction.OPTION_ASSERT_COUNT;

/** SinkManager test. */
public class SinkManagerTest {

    @Test
    public void testSinkGroupManager() throws IOException {
        final int partitionCount = 2;
        final long dataSize = 1024 * 10;
        final int sinkGroups = 2;
        final int maxRecordLength = 1024;
        test(partitionCount, dataSize, sinkGroups, maxRecordLength);
    }

    /**
     * test sink group manager.
     *
     * @param partitionCount partitionCount
     * @param dataSize dataSize
     * @param sinkGroups sinkGroups
     * @param maxRecordLength maxRecordLength
     * @throws IOException IOException
     */
    public static void test(int partitionCount, long dataSize, int sinkGroups, int maxRecordLength)
            throws IOException {

        final Set<String> consumerGroup = new HashSet<>(sinkGroups);
        for (int i = 0; i < sinkGroups; i++) {
            consumerGroup.add("consumer_group_" + i);
        }
        final Channel channel =
                new DefaultChannel<>(
                        (DefaultChannel.AbstractChannelArrayFactory<AbstractChannel<?>>)
                                () ->
                                        MemoryChannelFactory.createChannels(
                                                "voqa",
                                                partitionCount,
                                                consumerGroup,
                                                1024 * 3,
                                                1024 * 4L,
                                                CompressionType.SNAPPY),
                        0,
                        TimeUnit.SECONDS);

        // create sources
        final List<Thread> sourceThread = new ArrayList<>();
        for (int i = 0; i < partitionCount; i++) {
            Thread t = new SourceThread(channel, i, dataSize, maxRecordLength);
            sourceThread.add(t);
        }

        // start source and sink threads
        sourceThread.forEach(Thread::start);

        final SinkGroupConfigBuilder builder =
                SinkGroupConfigBuilder.newBuilder()
                        .handlerIdentity("direct")
                        .functionIdentity("assertCount");
        builder.addCustomProperty(OPTION_ASSERT_COUNT.key(), dataSize);
        final SinkGroupConfig sinkGroupConfig = builder.build();
        // waiting source threads finish and flush
        final List<SinkGroupManager> groupManagers = new ArrayList<>();
        consumerGroup.forEach(
                groupId -> {
                    SinkGroupManager sinkManager =
                            new SinkGroupManager(groupId, channel, sinkGroupConfig);
                    groupManagers.add(sinkManager);
                });

        groupManagers.forEach(SinkGroupManager::createPartitionHandlesAndStart);
        sourceThread.forEach(Threads::joinQuietly);
        channel.flush();
        groupManagers.forEach(IOUtils::closeQuietly);
        groupManagers.forEach(manager -> Assert.assertEquals(0, manager.lag()));
        IOUtils.closeQuietly(channel);
    }
}
