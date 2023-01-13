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
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zicat.tributary.channel.Channel;
import org.zicat.tributary.channel.MockChannel;
import org.zicat.tributary.channel.RecordsOffset;
import org.zicat.tributary.channel.utils.IOUtils;
import org.zicat.tributary.sink.SinkGroupConfig;
import org.zicat.tributary.sink.SinkGroupConfigBuilder;
import org.zicat.tributary.sink.handler.AbstractPartitionHandler;
import org.zicat.tributary.sink.handler.DirectPartitionHandler;
import org.zicat.tributary.sink.handler.MultiThreadPartitionHandler;
import org.zicat.tributary.sink.test.function.AssertFunctionFactory;
import org.zicat.tributary.sink.test.function.MockIdleTriggerFactory;
import org.zicat.tributary.sink.test.function.MockTriggerFunction;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.zicat.tributary.channel.test.file.SegmentTest.createStringByLength;

/** AbstractSinkHandlerTest. */
public class AbstractPartitionHandlerTest {

    private static final Logger LOG = LoggerFactory.getLogger(AbstractPartitionHandlerTest.class);
    final String groupId = "test_group";

    @Test
    public void testIdleTrigger() throws InterruptedException {
        final int partitionCount = 1;
        final Channel channel = new MockChannel(partitionCount);
        final SinkGroupConfigBuilder builder =
                SinkGroupConfigBuilder.newBuilder().functionIdentify(MockIdleTriggerFactory.ID);
        final SinkGroupConfig sinkGroupConfig = builder.build();

        /*
           Because idle trigger depend on child class implements,
           Using SimpleSinkHandler & DisruptorMultiSinkHandler test idle trigger
        */
        final DirectPartitionHandler handler =
                new DirectPartitionHandler(groupId, channel, partitionCount, sinkGroupConfig);
        handler.open();
        handler.start();
        // wait for idle trigger
        Thread.sleep(100);
        int triggerTimes = ((MockTriggerFunction) handler.getFunction()).idleTriggerCounter.get();
        LOG.info("trigger times {}", triggerTimes);
        Assert.assertTrue(triggerTimes > 1 && triggerTimes <= 10);
        IOUtils.closeQuietly(handler);

        final MultiThreadPartitionHandler handler2 =
                new MultiThreadPartitionHandler(groupId, channel, partitionCount, sinkGroupConfig);
        handler2.open();
        handler2.start();
        Thread.sleep(100);
        triggerTimes =
                handler2.getFunctions().stream()
                        .mapToInt(v -> ((MockTriggerFunction) v).idleTriggerCounter.get())
                        .max()
                        .orElse(-1);
        Assert.assertTrue(triggerTimes > 3 && triggerTimes <= 10);
        triggerTimes =
                handler2.getFunctions().stream()
                        .mapToInt(v -> ((MockTriggerFunction) v).idleTriggerCounter.get())
                        .min()
                        .orElse(-1);
        Assert.assertTrue(triggerTimes > 3 && triggerTimes <= 10);

        IOUtils.closeQuietly(handler2);
        IOUtils.closeQuietly(channel);
    }

    @Test
    public void testIllegalCommittableOffset() throws IOException {
        final SinkGroupConfigBuilder builder =
                SinkGroupConfigBuilder.newBuilder()
                        .functionIdentify(AssertFunctionFactory.IDENTIFY);
        // configuration maxRetainSize = 80 to skip segment
        builder.addCustomProperty(AbstractPartitionHandler.KEY_MAX_RETAIN_SIZE, 80L);
        final SinkGroupConfig sinkGroupConfig = builder.build();

        final int partitionCount = 2;
        final Channel channel = new MockChannel(partitionCount);
        final int partitionId = 0;
        final AbstractPartitionHandler handler =
                new AbstractPartitionHandler(groupId, channel, partitionId, sinkGroupConfig) {
                    @Override
                    public void closeCallback() {}

                    private RecordsOffset recordsOffset;

                    @Override
                    public long idleTimeMillis() {
                        return -1;
                    }

                    @Override
                    public void idleTrigger() {}

                    @Override
                    public void open() {}

                    @Override
                    public void process(RecordsOffset recordsOffset, Iterator<byte[]> iterator) {
                        this.recordsOffset = recordsOffset;
                    }

                    @Override
                    public RecordsOffset committableOffset() {
                        return recordsOffset;
                    }
                };
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

        IOUtils.closeQuietly(handler);
        IOUtils.closeQuietly(channel);
        Assert.assertEquals(5, handler.commitOffsetWaterMark().segmentId());
    }

    @Test
    public void testMaxRetainPerPartition() throws IOException {

        final int partitionCount = 2;
        final Channel channel = new MockChannel(partitionCount);

        final SinkGroupConfigBuilder builder =
                SinkGroupConfigBuilder.newBuilder()
                        .functionIdentify(AssertFunctionFactory.IDENTIFY);
        // configuration maxRetainSize = 80 to skip segment
        builder.addCustomProperty(AbstractPartitionHandler.KEY_MAX_RETAIN_SIZE, 80L);

        final SinkGroupConfig sinkGroupConfig = builder.build();

        final List<String> testData =
                Arrays.asList(
                        createStringByLength(55),
                        createStringByLength(66),
                        createStringByLength(77),
                        createStringByLength(77),
                        createStringByLength(77),
                        createStringByLength(77));
        final List<String> consumerData = Collections.synchronizedList(new ArrayList<>(testData));
        final AtomicBoolean skip = new AtomicBoolean(false);
        final int partitionId = 0;
        final AbstractPartitionHandler handler =
                new AbstractPartitionHandler(groupId, channel, partitionId, sinkGroupConfig) {

                    @Override
                    public long idleTimeMillis() {
                        return -1;
                    }

                    @Override
                    public void idleTrigger() {}

                    @Override
                    public void open() {}

                    @Override
                    public void process(RecordsOffset recordsOffset, Iterator<byte[]> iterator) {
                        while (iterator.hasNext()) {
                            Assert.assertTrue(
                                    consumerData.remove(
                                            new String(iterator.next(), StandardCharsets.UTF_8)));
                        }
                    }

                    @Override
                    public RecordsOffset committableOffset() {
                        return RecordsOffset.startRecordOffset();
                    }

                    @Override
                    public void skipCommitOffsetWaterMarkByMaxRetainSize() {
                        final RecordsOffset recordsOffset = commitOffsetWaterMark();
                        super.skipCommitOffsetWaterMarkByMaxRetainSize();
                        final RecordsOffset recordsOffset2 = commitOffsetWaterMark();
                        skip.set(recordsOffset != recordsOffset2 || skip.get());
                    }

                    @Override
                    public void closeCallback() throws IOException {}
                };
        handler.start();

        for (String data : testData) {
            channel.append(partitionId, data.getBytes(StandardCharsets.UTF_8));
            channel.flush();
        }

        IOUtils.closeQuietly(handler);
        IOUtils.closeQuietly(channel);
        Assert.assertEquals(5, handler.commitOffsetWaterMark().segmentId());
        Assert.assertTrue(skip.get());
    }

    @Test
    public void testRun() throws IOException, InterruptedException {

        final int partitionCount = 2;
        final Channel channel = new MockChannel(partitionCount);

        final SinkGroupConfig sinkGroupConfig =
                SinkGroupConfigBuilder.newBuilder()
                        .functionIdentify(AssertFunctionFactory.IDENTIFY)
                        .build();
        final AtomicInteger counter = new AtomicInteger(0);
        final CountDownLatch countDownLatch = new CountDownLatch(1);
        final List<String> testData =
                Arrays.asList(
                        createStringByLength(55),
                        createStringByLength(66),
                        createStringByLength(77));
        final List<String> consumerData = Collections.synchronizedList(new ArrayList<>(testData));
        final AbstractPartitionHandler handler =
                new AbstractPartitionHandler(groupId, channel, 0, sinkGroupConfig) {

                    @Override
                    public void closeCallback() throws IOException {}

                    @Override
                    public long idleTimeMillis() {
                        return -1;
                    }

                    @Override
                    public void idleTrigger() {}

                    private RecordsOffset recordsOffset;

                    @Override
                    public void open() {}

                    @Override
                    public void process(RecordsOffset recordsOffset, Iterator<byte[]> iterator) {
                        int id = counter.incrementAndGet();
                        Assert.assertTrue(lag() > 0);
                        if (id == 1) {
                            throw new RuntimeException("first");
                        }
                        while (iterator.hasNext()) {
                            consumerData.remove(
                                    new String(iterator.next(), StandardCharsets.UTF_8));
                        }
                        this.recordsOffset = recordsOffset;
                        if (id == 3) {
                            throw new RuntimeException("second");
                        }
                        if (consumerData.isEmpty()) {
                            countDownLatch.countDown();
                        }
                    }

                    @Override
                    public RecordsOffset committableOffset() {
                        return recordsOffset;
                    }
                };
        handler.start();

        for (String data : testData) {
            channel.append(0, data.getBytes(StandardCharsets.UTF_8));
            channel.flush();
        }

        countDownLatch.await();
        IOUtils.closeQuietly(handler);
        IOUtils.closeQuietly(channel);
    }
}
