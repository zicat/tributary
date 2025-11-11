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

package org.zicat.tributary.sink.test.handler;

import static org.zicat.tributary.common.records.RecordsUtils.createStringRecords;

import org.junit.Assert;
import org.zicat.tributary.channel.Channel;
import org.zicat.tributary.channel.memory.test.MemoryChannelTestUtils;
import org.zicat.tributary.sink.config.SinkGroupConfig;
import org.zicat.tributary.sink.config.SinkGroupConfigBuilder;
import org.zicat.tributary.sink.SinkGroupManager;
import org.zicat.tributary.sink.handler.DefaultPartitionHandlerFactory;
import static org.zicat.tributary.sink.handler.DefaultPartitionHandlerFactory.OPTION_PARTITION_GRACEFUL_CLOSE_QUICK_EXIT;
import org.zicat.tributary.sink.test.function.AssertFunction;
import org.zicat.tributary.sink.test.function.AssertFunctionFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/** SinkHandlerTestBase. */
public class SinkHandlerTestBase {

    private static final String groupId = "base_test_group";

    /**
     * test sink handler.
     *
     * @param testData test data
     * @throws IOException IOException
     */
    public static void test(List<String> testData, int threads)
            throws IOException, InterruptedException {

        final List<String> copyData = new ArrayList<>(testData);
        final int partitionCount = 2;

        try (Channel channel =
                MemoryChannelTestUtils.createChannel("t1", partitionCount, groupId)) {
            final SinkGroupConfigBuilder builder =
                    SinkGroupConfigBuilder.newBuilder()
                            .handlerIdentity(DefaultPartitionHandlerFactory.IDENTITY)
                            .functionIdentity(AssertFunctionFactory.IDENTITY)
                            .addConfig(AssertFunction.KEY_ASSERT_DATA, testData)
                            .addConfig(DefaultPartitionHandlerFactory.OPTION_THREADS, threads)
                            .addConfig(OPTION_PARTITION_GRACEFUL_CLOSE_QUICK_EXIT.key(), false);
            final SinkGroupConfig sinkGroupConfig = builder.build();
            try (SinkGroupManager sinkManager =
                    new SinkGroupManager(groupId, channel, sinkGroupConfig)) {
                sinkManager.createPartitionHandlesAndStart();
                for (int i = 0; i < copyData.size(); i++) {
                    channel.append(
                            i % partitionCount,
                            createStringRecords("t1", copyData.get(i)).toByteBuffer());
                }
                channel.flush();
            }
        }

        Assert.assertEquals(0, testData.size());
    }
}
