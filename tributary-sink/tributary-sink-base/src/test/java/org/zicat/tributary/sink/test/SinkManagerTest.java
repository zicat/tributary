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

import static org.zicat.tributary.sink.handler.PartitionHandler.KEY_SINK_LAG;
import static org.zicat.tributary.sink.test.function.AssertCountFunction.OPTION_ASSERT_COUNT;

import org.junit.Assert;
import org.junit.Test;
import org.zicat.tributary.channel.Channel;
import org.zicat.tributary.channel.memory.test.MemoryChannelTestUtils;
import org.zicat.tributary.channel.test.SourceThread;
import org.zicat.tributary.common.util.IOUtils;
import org.zicat.tributary.common.util.Threads;
import org.zicat.tributary.sink.SinkGroupConfig;
import org.zicat.tributary.sink.SinkGroupConfigBuilder;
import org.zicat.tributary.sink.SinkGroupManager;
import org.zicat.tributary.sink.handler.DefaultPartitionHandlerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

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
        try (final Channel channel =
                MemoryChannelTestUtils.createChannel(
                        "voqa", partitionCount, consumerGroup.toArray(new String[] {}))) {
            final SinkGroupConfigBuilder builder =
                    SinkGroupConfigBuilder.newBuilder()
                            .handlerIdentity(DefaultPartitionHandlerFactory.IDENTITY)
                            .functionIdentity("assertCount");
            builder.addCustomProperty(OPTION_ASSERT_COUNT, dataSize);
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

            // create sources
            final List<Thread> sourceThread = new ArrayList<>();
            for (int i = 0; i < partitionCount; i++) {
                Thread t = new SourceThread(channel, i, dataSize, maxRecordLength);
                sourceThread.add(t);
            }

            // start source and sink threads
            sourceThread.forEach(Thread::start);
            sourceThread.forEach(Threads::joinQuietly);
            channel.flush();
            groupManagers.forEach(IOUtils::closeQuietly);
            groupManagers.forEach(
                    manager ->
                            Assert.assertEquals(0, manager.gaugeFamily().get(KEY_SINK_LAG), 0.001));
        }
    }
}
