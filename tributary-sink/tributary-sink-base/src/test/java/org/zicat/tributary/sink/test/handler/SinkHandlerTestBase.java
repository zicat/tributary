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

import org.junit.Assert;
import org.zicat.tributary.queue.LogQueue;
import org.zicat.tributary.queue.MockLogQueue;
import org.zicat.tributary.queue.utils.IOUtils;
import org.zicat.tributary.sink.SinkGroupConfig;
import org.zicat.tributary.sink.SinkGroupConfigBuilder;
import org.zicat.tributary.sink.SinkGroupManager;
import org.zicat.tributary.sink.test.function.AssertFunction;
import org.zicat.tributary.sink.test.function.AssertFunctionFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/** SinkHandlerTestBase. */
public class SinkHandlerTestBase {

    private static final String groupId = "test_group";

    /**
     * test sink handler.
     *
     * @param testData test data
     * @param handlerIdentify handlerIdentify
     * @throws IOException IOException
     */
    public static void test(List<String> testData, String handlerIdentify) throws IOException {

        final List<String> copyData = new ArrayList<>(testData);
        final int partitionCount = 2;
        final LogQueue logQueue = new MockLogQueue(partitionCount);
        final SinkGroupConfigBuilder builder =
                SinkGroupConfigBuilder.newBuilder()
                        .handlerIdentify(handlerIdentify)
                        .processFunctionIdentify(AssertFunctionFactory.IDENTIFY);
        builder.addCustomProperty(AssertFunction.KEY_ASSERT_DATA, testData);
        final SinkGroupConfig sinkGroupConfig = builder.build();
        final SinkGroupManager sinkManager =
                new SinkGroupManager(groupId, logQueue, sinkGroupConfig);
        sinkManager.createPartitionHandlesAndStart();

        for (int i = 0; i < copyData.size(); i++) {
            logQueue.append(i % partitionCount, copyData.get(i).getBytes(StandardCharsets.UTF_8));
        }
        logQueue.flush();

        IOUtils.closeQuietly(sinkManager);
        Assert.assertTrue(testData.isEmpty());
        IOUtils.closeQuietly(logQueue);
    }
}
