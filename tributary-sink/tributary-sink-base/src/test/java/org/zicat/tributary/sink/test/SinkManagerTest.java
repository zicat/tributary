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
import org.zicat.tributary.queue.LogQueue;
import org.zicat.tributary.queue.MockLogQueue;
import org.zicat.tributary.queue.test.file.FileLogQueueTest;
import org.zicat.tributary.queue.test.file.SourceThread;
import org.zicat.tributary.queue.utils.IOUtils;
import org.zicat.tributary.sink.SinkGroupConfig;
import org.zicat.tributary.sink.SinkGroupConfigBuilder;
import org.zicat.tributary.sink.SinkGroupManager;
import org.zicat.tributary.sink.test.function.AssertCountFunction;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

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

        final List<String> consumerGroup = new ArrayList<>(sinkGroups);
        for (int i = 0; i < sinkGroups; i++) {
            consumerGroup.add("consumer_group_" + i);
        }
        final LogQueue logQueue = new MockLogQueue("voqa", partitionCount);

        // create sources
        final List<Thread> sourceThread = new ArrayList<>();
        for (int i = 0; i < partitionCount; i++) {
            Thread t = new SourceThread(logQueue, i, dataSize, maxRecordLength);
            sourceThread.add(t);
        }

        // start source and sink threads
        sourceThread.forEach(Thread::start);

        final SinkGroupConfigBuilder builder =
                SinkGroupConfigBuilder.newBuilder()
                        .handlerIdentify("simple")
                        .processFunctionIdentify("assertCount");
        builder.addCustomProperty(AssertCountFunction.KEY_ASSERT_COUNT, dataSize);
        final SinkGroupConfig sinkGroupConfig = builder.build();
        // waiting source threads finish and flush
        final List<SinkGroupManager> groupManagers = new ArrayList<>();
        consumerGroup.forEach(
                groupId -> {
                    SinkGroupManager sinkManager =
                            new SinkGroupManager(groupId, logQueue, sinkGroupConfig);
                    groupManagers.add(sinkManager);
                });

        groupManagers.forEach(SinkGroupManager::createPartitionHandlesAndStart);
        sourceThread.forEach(FileLogQueueTest::join);
        logQueue.flush();
        groupManagers.forEach(IOUtils::closeQuietly);
        groupManagers.forEach(manager -> Assert.assertEquals(0, manager.lag()));
        IOUtils.closeQuietly(logQueue);
    }
}
