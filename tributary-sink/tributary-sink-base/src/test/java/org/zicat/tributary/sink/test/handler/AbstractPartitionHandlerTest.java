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

import org.zicat.tributary.channel.DefaultChannel;
import org.zicat.tributary.channel.memory.MemorySingleChannel;
import static org.zicat.tributary.channel.test.StringTestUtils.createStringByLength;
import org.zicat.tributary.common.config.PercentSize;
import org.zicat.tributary.sink.function.Function;
import org.zicat.tributary.sink.handler.PartitionHandler;

import org.junit.Assert;
import org.junit.Test;
import org.zicat.tributary.channel.Channel;
import org.zicat.tributary.channel.CompressionType;
import org.zicat.tributary.channel.Offset;
import org.zicat.tributary.channel.memory.test.MemoryChannelTestUtils;
import org.zicat.tributary.common.util.IOUtils;
import org.zicat.tributary.sink.config.SinkGroupConfig;
import org.zicat.tributary.sink.config.SinkGroupConfigBuilder;
import org.zicat.tributary.sink.test.function.AssertFunctionFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/** AbstractSinkHandlerTest. */
public class AbstractPartitionHandlerTest {

    final String groupId = "test_group";

    @Test
    public void testIllegalCommittableOffset() throws IOException, InterruptedException {
        final SinkGroupConfig sinkGroupConfig =
                SinkGroupConfigBuilder.newBuilder()
                        .functionIdentity(AssertFunctionFactory.IDENTITY)
                        .build();

        PartitionHandler handler;
        final List<String> dataList = new ArrayList<>();
        final DefaultChannel<MemorySingleChannel> channel =
                MemoryChannelTestUtils.createChannel(
                        "t1", 1, 30, 50, CompressionType.NONE, PercentSize.ZERO, groupId);
        final int partitionId = 0;
        handler =
                new PartitionHandler(groupId, channel, partitionId, sinkGroupConfig) {

                    @Override
                    public void snapshot() {}

                    @Override
                    public List<Function> getFunctions() {
                        return null;
                    }

                    @Override
                    public void open() {}

                    @Override
                    public void process(Offset offset, Iterator<byte[]> iterator) {
                        while (iterator.hasNext()) {
                            dataList.add(new String(iterator.next(), StandardCharsets.UTF_8));
                        }
                    }

                    @Override
                    public Offset committableOffset() {
                        return Offset.ZERO;
                    }
                };
        try {
            final List<String> testData =
                    Arrays.asList(
                            createStringByLength(55),
                            createStringByLength(66),
                            createStringByLength(77),
                            createStringByLength(78),
                            createStringByLength(79),
                            createStringByLength(80));
            for (int i = 0; i < testData.size(); i++) {
                String data = testData.get(i);
                channel.append(partitionId, data.getBytes(StandardCharsets.UTF_8));
                if (i == 0) {
                    Assert.assertFalse(handler.runOneBatch());
                }
                channel.flush();
            }
            for (int i = 0; i < 5; i++) {
                channel.cleanUpExpiredSegmentsQuietly();
            }

            for (int i = 0; i < testData.size(); i++) {
                handler.runOneBatch();
            }
        } finally {
            IOUtils.closeQuietly(handler);
        }
        Assert.assertTrue(channel.getCleanupExpiredSegmentThread().isAlive());
        Assert.assertTrue(channel.getFlushSegmentThread().isAlive());

        IOUtils.closeQuietly(channel);
        Assert.assertEquals(2, dataList.size());
        Assert.assertEquals(createStringByLength(55), dataList.get(0));
        Assert.assertEquals(createStringByLength(80), dataList.get(1));
        Assert.assertFalse(channel.getCleanupExpiredSegmentThread().isAlive());
        Assert.assertFalse(channel.getFlushSegmentThread().isAlive());
    }

    @Test
    public void testRun() throws IOException, InterruptedException {

        try (Channel channel = MemoryChannelTestUtils.createChannel("t1", 2, groupId)) {
            final SinkGroupConfig sinkGroupConfig =
                    SinkGroupConfigBuilder.newBuilder()
                            .functionIdentity(AssertFunctionFactory.IDENTITY)
                            .build();
            final AtomicInteger counter = new AtomicInteger(0);
            final CountDownLatch countDownLatch = new CountDownLatch(1);
            final List<String> testData =
                    Arrays.asList(
                            createStringByLength(55),
                            createStringByLength(66),
                            createStringByLength(77));
            final List<String> consumerData =
                    Collections.synchronizedList(new ArrayList<>(testData));
            try (PartitionHandler handler =
                    new PartitionHandler(groupId, channel, 0, sinkGroupConfig) {

                        @Override
                        public void snapshot() {}

                        @Override
                        public List<Function> getFunctions() {
                            return null;
                        }

                        private Offset offset;

                        @Override
                        public void open() {
                            this.offset = startOffset;
                        }

                        @Override
                        public void process(Offset offset, Iterator<byte[]> iterator) {
                            int id = counter.incrementAndGet();
                            Assert.assertTrue(gaugeFamily().get(KEY_SINK_LAG) > 0);
                            if (id == 1) {
                                throw new RuntimeException("first");
                            }
                            while (iterator.hasNext()) {
                                consumerData.remove(
                                        new String(iterator.next(), StandardCharsets.UTF_8));
                            }
                            this.offset = offset;
                            if (id == 3) {
                                throw new RuntimeException("second");
                            }
                            if (consumerData.isEmpty()) {
                                countDownLatch.countDown();
                            }
                        }

                        @Override
                        public Offset committableOffset() {
                            return offset;
                        }
                    }) {

                try {
                    handler.open();
                    handler.start();

                    for (String data : testData) {
                        channel.append(0, data.getBytes(StandardCharsets.UTF_8));
                        channel.flush();
                    }

                    countDownLatch.await();
                    Assert.assertTrue(countDownLatch.await(10, TimeUnit.SECONDS));
                } finally {
                    IOUtils.closeQuietly(handler);
                }
            }
        }
    }
}
