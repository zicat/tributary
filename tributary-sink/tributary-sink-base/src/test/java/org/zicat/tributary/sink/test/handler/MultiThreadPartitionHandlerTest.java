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

import static org.zicat.tributary.sink.handler.MultiThreadPartitionHandler.OPTION_THREADS;

import org.junit.Assert;
import org.junit.Test;
import org.zicat.tributary.channel.Channel;
import org.zicat.tributary.channel.memory.test.MemoryChannelTestUtils;
import org.zicat.tributary.sink.SinkGroupConfigBuilder;
import org.zicat.tributary.sink.handler.MultiThreadPartitionHandler;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

/** DisruptorMultiSinkHandlerTest. */
public class MultiThreadPartitionHandlerTest {

    @Test
    public void testThreadCount() throws IOException {
        final String topic = "t1";
        final String groupId = "g1";
        final SinkGroupConfigBuilder builder =
                SinkGroupConfigBuilder.newBuilder().functionIdentity("dummy");
        int threads = 0;
        try (Channel channel = MemoryChannelTestUtils.createChannel(topic, groupId)) {
            try (MultiThreadPartitionHandler handler =
                    new MultiThreadPartitionHandler(
                            groupId, channel, 0, threads, builder.build())) {
                try {
                    handler.open();
                    Assert.fail();
                } catch (IllegalStateException e) {
                    Assert.assertTrue(true);
                }
            }
            threads = 10;
            builder.addCustomProperty(OPTION_THREADS, threads);
            try (MultiThreadPartitionHandler handler2 =
                    new MultiThreadPartitionHandler(
                            groupId, channel, 0, threads, builder.build())) {
                handler2.open();
                Assert.assertEquals(threads, handler2.handlers().length);
            }
        }
    }

    @Test
    public void testFunctionId() throws IOException {

        final String topic = "t1";
        final String groupId = "g1";
        final SinkGroupConfigBuilder builder =
                SinkGroupConfigBuilder.newBuilder().functionIdentity("dummy");
        final int threads = 4;
        try (Channel channel = MemoryChannelTestUtils.createChannel(topic, groupId);
                MultiThreadPartitionHandler handler =
                        new MultiThreadPartitionHandler(
                                groupId, channel, 0, threads, builder.build())) {
            handler.open();
            Assert.assertEquals(threads, handler.handlers().length);
            final Set<String> distinctIds = new HashSet<>();
            for (MultiThreadPartitionHandler.DataHandler dataHandler : handler.handlers()) {
                distinctIds.add(dataHandler.functionId());
            }
            Assert.assertEquals(threads, distinctIds.size());
        }
    }
}
