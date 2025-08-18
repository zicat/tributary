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

package org.zicat.tributary.channel.memory.test;

import static org.zicat.tributary.channel.ChannelConfigOption.*;
import static org.zicat.tributary.channel.group.GroupManager.uninitializedOffset;
import static org.zicat.tributary.channel.group.MemoryGroupManager.createMemoryGroupManagerFactory;
import static org.zicat.tributary.channel.memory.MemoryChannelFactory.createMemoryChannel;
import static org.zicat.tributary.channel.test.ChannelBaseTest.readChannel;
import static org.zicat.tributary.channel.test.ChannelBaseTest.testChannelCorrect;

import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zicat.tributary.channel.*;
import org.zicat.tributary.channel.Segment.AppendResult;
import org.zicat.tributary.channel.memory.MemoryChannel;
import org.zicat.tributary.channel.memory.MemoryChannelFactory;
import org.zicat.tributary.channel.test.ChannelBaseTest.DataOffset;
import org.zicat.tributary.channel.test.SinkGroup;
import org.zicat.tributary.channel.test.SourceThread;
import org.zicat.tributary.common.DefaultReadableConfig;
import org.zicat.tributary.common.Threads;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

/** OnePartitionMemoryChannelTest. */
public class MemoryChannelTest {

    private static final Logger LOG = LoggerFactory.getLogger(MemoryChannelTest.class);

    @Test
    public void testSyncAwait() throws IOException, InterruptedException {
        final Map<String, Offset> groupOffsets = new HashMap<>();
        final AbstractChannel.MemoryGroupManagerFactory factory =
                createMemoryGroupManagerFactory(groupOffsets);
        final AtomicBoolean finished = new AtomicBoolean();
        try (Channel channel =
                new MemoryChannel("t1", factory, 10240, 10240L, CompressionType.NONE, 10) {
                    @Override
                    public void append(ByteBuffer byteBuffer) throws IOException {
                        final AppendResult appendResult = innerAppend(byteBuffer);
                        if (!appendResult.appended()) {
                            throw new IOException("append fail");
                        }
                        try {
                            Assert.assertTrue(appendResult.await2Storage(10, TimeUnit.SECONDS));
                        } catch (InterruptedException e) {
                            throw new RuntimeException(e);
                        } finally {
                            finished.set(true);
                        }
                    }
                }) {
            final Thread t1 =
                    new Thread(
                            () -> {
                                try {
                                    channel.append(0, "aa".getBytes());
                                } catch (IOException | InterruptedException e) {
                                    throw new RuntimeException(e);
                                }
                            });
            t1.start();
            // try join 1s
            t1.join(1000);
            Assert.assertFalse(finished.get());
            channel.flush();
            t1.join();
            Assert.assertTrue(finished.get());
        }
    }

    @Test
    public void testBaseCorrect() throws Exception {
        final DefaultReadableConfig config = new DefaultReadableConfig();
        config.put(OPTION_PARTITION_COUNT, 4);
        config.put(OPTION_GROUPS, Arrays.asList("g1", "g2", "g3"));
        config.put(OPTION_COMPRESSION, CompressionType.ZSTD);
        final ChannelFactory factory = new MemoryChannelFactory();
        try (Channel channel = factory.createChannel("t1", config)) {
            testChannelCorrect(channel);
        }
    }

    @Test
    public void test() throws IOException, InterruptedException {

        final String groupId = "g1";
        final Map<String, Offset> groupOffsets = new HashMap<>();
        groupOffsets.put(groupId, uninitializedOffset());
        try (MemoryChannel channel =
                createMemoryChannel(
                        "t1",
                        createMemoryGroupManagerFactory(groupOffsets),
                        1024 * 4,
                        102400L,
                        CompressionType.NONE,
                        1)) {
            final Offset offset = channel.committedOffset(groupId);
            final Random random = new Random(1023312);
            final List<byte[]> result = new ArrayList<>();
            for (int i = 0; i < 200000; i++) {
                final int count = random.nextInt(60) + 60;
                final byte[] bs = new byte[count];
                for (int j = 0; j < count; j++) {
                    bs[j] = (byte) random.nextInt(256);
                }
                result.add(bs);
                channel.append(0, bs);
            }
            channel.flush();
            Assert.assertTrue(channel.activeSegment() > 1);
            final DataOffset dataOffset = readChannel(channel, 0, offset, result.size());
            Assert.assertEquals(result.size(), dataOffset.data.size());
            for (int i = 0; i < result.size(); i++) {
                Assert.assertArrayEquals(result.get(i), dataOffset.data.get(i));
            }
            channel.commit(groupId, dataOffset.offset);
            Assert.assertEquals(1, channel.activeSegment());
        }
    }

    @Test
    public void testSmallSegmentSize() throws IOException {

        final int partitionCount = 1;
        final long dataSize = 500001;
        final int sinkGroups = 2;
        final int blockSize = 28;
        final long segmentSize = 1024L * 512L;
        final int maxRecordLength = 32;

        final String topic = "test_small_segments";
        for (int j = 0; j < 10; j++) {
            final long totalSize = dataSize * partitionCount;
            Set<String> consumerGroup = new HashSet<>(sinkGroups);
            Map<String, Offset> consumerGroupOffset = new HashMap<>();
            for (int i = 0; i < sinkGroups; i++) {
                final String groupId = "consumer_group_" + i;
                consumerGroup.add(groupId);
                consumerGroupOffset.put(groupId, Offset.ZERO);
            }
            try (Channel channel =
                    createMemoryChannel(
                            topic,
                            createMemoryGroupManagerFactory(consumerGroupOffset),
                            blockSize,
                            segmentSize,
                            CompressionType.NONE,
                            1)) {
                // create sources
                final List<Thread> sourceThread = new ArrayList<>();
                for (int i = 0; i < partitionCount; i++) {
                    Thread t = new SourceThread(channel, i, dataSize, maxRecordLength, true);
                    sourceThread.add(t);
                }

                // create multi sink
                final List<SinkGroup> sinGroup =
                        consumerGroup.stream()
                                .map(
                                        groupName ->
                                                new SinkGroup(
                                                        partitionCount,
                                                        channel,
                                                        groupName,
                                                        totalSize))
                                .collect(Collectors.toList());

                long start = System.currentTimeMillis();
                // start source and sink threads
                sourceThread.forEach(Thread::start);
                sinGroup.forEach(Thread::start);

                // waiting source threads finish and flush
                sourceThread.forEach(Threads::joinQuietly);
                channel.flush();
                long writeSpend = System.currentTimeMillis() - start;

                // waiting sink threads finish.
                sinGroup.forEach(Threads::joinQuietly);
                Assert.assertEquals(totalSize, dataSize * partitionCount);
                LOG.info(
                        "write spend:"
                                + writeSpend
                                + "(ms),write count:"
                                + totalSize
                                + ",read spend:"
                                + (System.currentTimeMillis() - start)
                                + "(ms),read count:"
                                + sinGroup.stream().mapToLong(SinkGroup::getConsumerCount).sum()
                                + ".");
                Assert.assertEquals(
                        dataSize * partitionCount * sinkGroups,
                        sinGroup.stream().mapToLong(SinkGroup::getConsumerCount).sum());
            }
        }
    }
}
