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

import static org.zicat.tributary.channel.test.StringTestUtils.createStringByLength;
import static org.zicat.tributary.sink.handler.DefaultPartitionHandlerFactory.OPTION_CHECKPOINT_INTERVAL;
import static org.zicat.tributary.sink.handler.DefaultPartitionHandlerFactory.OPTION_MAX_RETAIN_SIZE;
import org.zicat.tributary.sink.handler.PartitionHandler;
import static org.zicat.tributary.sink.handler.PartitionHandler.OPTION_PARTITION_HANDLER_CLOCK;

import org.junit.Assert;
import org.junit.Test;
import org.zicat.tributary.channel.Channel;
import org.zicat.tributary.channel.CompressionType;
import org.zicat.tributary.channel.Offset;
import org.zicat.tributary.channel.memory.test.MemoryChannelTestUtils;
import org.zicat.tributary.common.IOUtils;
import org.zicat.tributary.sink.SinkGroupConfig;
import org.zicat.tributary.sink.SinkGroupConfigBuilder;
import org.zicat.tributary.sink.function.AbstractFunction;
import org.zicat.tributary.sink.test.function.AssertFunctionFactory;
import org.zicat.tributary.sink.test.function.MockClock;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/** AbstractSinkHandlerTest. */
public class AbstractPartitionHandlerTest {

    final String groupId = "test_group";

    @Test
    public void testIllegalCommittableOffset() throws IOException, InterruptedException {
        final SinkGroupConfigBuilder builder =
                SinkGroupConfigBuilder.newBuilder()
                        .functionIdentity(AssertFunctionFactory.IDENTITY);
        // configuration maxRetainSize = 80 to skip segment
        builder.addCustomProperty(OPTION_MAX_RETAIN_SIZE, 80L);
        final SinkGroupConfig sinkGroupConfig = builder.build();

        PartitionHandler handler;
        try (Channel channel =
                MemoryChannelTestUtils.createChannel(
                        "t1", 2, 50, 50, CompressionType.NONE, groupId)) {
            final int partitionId = 0;
            handler =
                    new PartitionHandler(groupId, channel, partitionId, sinkGroupConfig) {

                        @Override
                        public void snapshot() {}

                        @Override
                        public void closeCallback() {}

                        @Override
                        public List<AbstractFunction> getFunctions() {
                            return null;
                        }

                        @Override
                        public void open() {}

                        @Override
                        public void process(Offset offset, Iterator<byte[]> iterator) {}

                        @Override
                        public Offset committableOffset() {
                            return Offset.ZERO;
                        }
                    };
            try {
                handler.start();
                final List<String> testData =
                        Arrays.asList(
                                createStringByLength(55),
                                createStringByLength(66),
                                createStringByLength(77),
                                createStringByLength(77),
                                createStringByLength(77),
                                createStringByLength(77));
                for (String data : testData) {
                    channel.append(partitionId, data.getBytes(StandardCharsets.UTF_8));
                    channel.flush();
                }
            } finally {
                IOUtils.closeQuietly(handler);
            }
        }
        Assert.assertEquals(5, handler.committedOffset().segmentId());
    }

    @Test
    public void testMaxRetainPerPartition() throws IOException, InterruptedException {

        final AtomicBoolean skip = new AtomicBoolean(false);
        final MockClock clock = new MockClock().setCurrentTimeMillis(System.currentTimeMillis());
        final CountDownLatch countDownLatch = new CountDownLatch(1);
        PartitionHandler handler;
        try (Channel channel =
                MemoryChannelTestUtils.createChannel(
                        "t1", 1, 50, 50L, CompressionType.NONE, groupId)) {
            final SinkGroupConfigBuilder builder =
                    SinkGroupConfigBuilder.newBuilder()
                            .functionIdentity(AssertFunctionFactory.IDENTITY);
            // configuration maxRetainSize = 80 to skip segment
            builder.addCustomProperty(OPTION_MAX_RETAIN_SIZE, 80L);
            builder.addCustomProperty(OPTION_CHECKPOINT_INTERVAL, 100000);
            builder.addCustomProperty(OPTION_PARTITION_HANDLER_CLOCK, clock);

            final SinkGroupConfig sinkGroupConfig = builder.build();

            final List<String> testData =
                    Arrays.asList(
                            createStringByLength(55),
                            createStringByLength(66),
                            createStringByLength(77),
                            createStringByLength(77),
                            createStringByLength(77),
                            createStringByLength(77));
            final List<String> consumerData =
                    Collections.synchronizedList(new ArrayList<>(testData));

            final int partitionId = 0;
            handler =
                    new PartitionHandler(groupId, channel, partitionId, sinkGroupConfig) {

                        @Override
                        public void snapshot() {}

                        private int count = 0;

                        @Override
                        public void open() {}

                        @Override
                        public void process(Offset offset, Iterator<byte[]> iterator) {
                            while (iterator.hasNext()) {
                                Assert.assertTrue(
                                        consumerData.remove(
                                                new String(
                                                        iterator.next(), StandardCharsets.UTF_8)));
                            }
                            count++;
                            if (count == testData.size()) {
                                // trigger snapshot
                                ((MockClock) clock).addMillis(100000);
                            }
                        }

                        @Override
                        public Offset committableOffset() {
                            return Offset.ZERO;
                        }

                        @Override
                        public Offset skipGroupOffsetByMaxRetainSize(Offset offset) {
                            final Offset offset2 = super.skipGroupOffsetByMaxRetainSize(offset);
                            skip.set(!offset.equals(offset2));
                            countDownLatch.countDown();
                            return offset2;
                        }

                        @Override
                        public void closeCallback() {}

                        @Override
                        public List<AbstractFunction> getFunctions() {
                            return null;
                        }
                    };
            try {
                handler.start();
                for (String data : testData) {
                    channel.append(partitionId, data.getBytes(StandardCharsets.UTF_8));
                }
                channel.flush();
                countDownLatch.await();
                Assert.assertTrue(skip.get());
            } finally {
                IOUtils.closeQuietly(handler);
            }
        }
        Assert.assertEquals(5, handler.committedOffset().segmentId());
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
                        public void closeCallback() {}

                        @Override
                        public List<AbstractFunction> getFunctions() {
                            return null;
                        }

                        private Offset offset;

                        @Override
                        public void open() {}

                        @Override
                        public void process(Offset offset, Iterator<byte[]> iterator) {
                            int id = counter.incrementAndGet();
                            Assert.assertTrue(lag() > 0);
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
