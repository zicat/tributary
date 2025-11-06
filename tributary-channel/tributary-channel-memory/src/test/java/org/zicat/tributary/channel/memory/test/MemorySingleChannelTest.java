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

import org.zicat.tributary.channel.AbstractSingleChannel.SingleGroupManagerFactory;
import static org.zicat.tributary.channel.ChannelConfigOption.*;
import static org.zicat.tributary.channel.Offset.UNINITIALIZED_OFFSET;
import static org.zicat.tributary.channel.group.MemoryGroupManager.createSingleGroupManagerFactory;
import static org.zicat.tributary.channel.memory.MemoryChannelFactory.createMemorySingleChannel;
import org.zicat.tributary.channel.memory.MemorySegment;
import static org.zicat.tributary.channel.test.ChannelBaseTest.readChannel;
import static org.zicat.tributary.channel.test.ChannelBaseTest.testChannelCorrect;

import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zicat.tributary.channel.*;
import org.zicat.tributary.channel.Segment.AppendResult;
import org.zicat.tributary.channel.memory.MemorySingleChannel;
import org.zicat.tributary.channel.memory.MemoryChannelFactory;
import org.zicat.tributary.channel.test.ChannelBaseTest;
import org.zicat.tributary.channel.test.ChannelBaseTest.DataOffset;
import org.zicat.tributary.channel.test.SinkGroup;
import org.zicat.tributary.channel.test.SourceThread;
import org.zicat.tributary.common.config.DefaultReadableConfig;
import org.zicat.tributary.common.config.MemorySize;
import org.zicat.tributary.common.config.PercentSize;
import org.zicat.tributary.common.util.Threads;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

/** MemorySingleChannelTest. */
public class MemorySingleChannelTest {

    private static final Logger LOG = LoggerFactory.getLogger(MemorySingleChannelTest.class);
    private static final String LOG_FORMAT =
            "write spend:{},write count:{},read spend:{},read count:{}.";

    @Test
    public void testChannelStorage() throws Exception {
        final ChannelFactory factory = new MemoryChannelFactory();
        final DefaultReadableConfig config = new DefaultReadableConfig();
        config.put(OPTION_GROUPS, Arrays.asList("g1", "g2"));
        config.put(OPTION_PARTITION_COUNT, 2);
        ChannelBaseTest.testChannelStorage(factory, "test_channel_storage", config);
    }

    @Test
    public void testCheckCapacityAndCloseSegments() throws IOException, InterruptedException {
        final Map<String, Offset> groupOffsets = new HashMap<>();
        final SingleGroupManagerFactory factory = createSingleGroupManagerFactory(groupOffsets);
        try (MemorySingleChannelMock channel =
                new MemorySingleChannelMock("t1", factory, 10, 50L, CompressionType.NONE, 0)) {
            for (int i = 0; i < 100; i++) {
                channel.append(0, "aabbc".getBytes());
            }
            Assert.assertEquals(15, channel.activeSegment());
            final int count = 10;
            for (int i = 0; i < count; i++) {
                Assert.assertTrue(channel.checkCapacityExceed(PercentSize.ZERO));
                channel.cleanUpEarliestSegment();
            }
            Assert.assertEquals(5, channel.activeSegment());
            List<Segment> segments = new ArrayList<>(channel.segments().values());
            segments.sort(Comparator.comparingLong(Segment::segmentId));
            long offset = 10;
            long size = 0;
            for (Segment segment : segments) {
                Assert.assertEquals(offset, segment.segmentId());
                offset++;
                size += segment.position();
            }
            Assert.assertTrue(size <= 290 && size >= (290 - 50));

            // more segments close
            Assert.assertEquals(5, channel.activeSegment());
            segments = new ArrayList<>(channel.segments().values());
            segments.sort(Comparator.comparingLong(Segment::segmentId));
            offset = 10;
            size = 0;
            for (Segment segment : segments) {
                Assert.assertEquals(offset, segment.segmentId());
                offset++;
                size += segment.position();
            }
            Assert.assertTrue(size <= 290 && size >= (290 - 50));
        }
    }

    /** MemoryChannelMock. */
    private static class MemorySingleChannelMock extends MemorySingleChannel {

        public MemorySingleChannelMock(
                String topic,
                SingleGroupManagerFactory singleGroupManagerFactory,
                int blockSize,
                Long segmentSize,
                CompressionType compressionType,
                int blockCacheCount) {
            super(
                    topic,
                    singleGroupManagerFactory,
                    blockSize,
                    segmentSize,
                    compressionType,
                    blockCacheCount);
        }

        public Map<Long, MemorySegment> segments() {
            return cache;
        }
    }

    @Test
    public void testSyncAwait() throws IOException, InterruptedException {
        final Map<String, Offset> groupOffsets = new HashMap<>();
        final SingleGroupManagerFactory factory = createSingleGroupManagerFactory(groupOffsets);
        final AtomicBoolean finished = new AtomicBoolean();
        try (Channel channel =
                new MemorySingleChannel("t1", factory, 10240, 102400L, CompressionType.NONE, 10) {
                    @Override
                    public AppendResult append(ByteBuffer byteBuffer)
                            throws IOException, InterruptedException {
                        final AppendResult appendResult = super.append(byteBuffer);
                        if (!appendResult.appended()) {
                            throw new IOException("append fail");
                        }
                        try {
                            Assert.assertTrue(appendResult.await2Storage());
                        } catch (InterruptedException e) {
                            throw new RuntimeException(e);
                        } finally {
                            finished.set(true);
                        }
                        return appendResult;
                    }

                    @Override
                    protected MemorySegment createSegment(long id) {
                        return new MemorySegment(
                                id, blockWriter, compression, segmentSize, 10000L, bCache);
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
        groupOffsets.put(groupId, UNINITIALIZED_OFFSET);
        try (MemorySingleChannel channel =
                createMemorySingleChannel(
                        "t1",
                        createSingleGroupManagerFactory(groupOffsets),
                        1024 * 4,
                        102400L,
                        CompressionType.NONE,
                        1)) {
            final Offset offset = channel.committedOffset(groupId, 0);
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
            channel.cleanUpExpiredSegmentsQuietly();
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
            try (MemorySingleChannel channel =
                    createMemorySingleChannel(
                            topic,
                            createSingleGroupManagerFactory(consumerGroupOffset),
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
                // waiting sink threads finish.
                sinGroup.forEach(Threads::joinQuietly);
                Assert.assertEquals(totalSize, dataSize * partitionCount);
                Assert.assertEquals(
                        dataSize * partitionCount * sinkGroups,
                        sinGroup.stream().mapToLong(SinkGroup::getConsumerCount).sum());
            }
        }
    }

    @Test
    public void testPartitionAndSinkGroups() throws IOException {
        final int partitionCount = 3;
        final long dataSizePerPartition = 10000;
        final int sinkGroups = 4;
        final int blockSize = 32 * 1024;
        final long segmentSize = 1024L * 1024L * 512;
        final int maxRecordLength = 1024;

        final long totalSize = dataSizePerPartition * partitionCount;
        Set<String> consumerGroup = new HashSet<>(sinkGroups);
        for (int i = 0; i < sinkGroups; i++) {
            final String groupId = "consumer_group_" + i;
            consumerGroup.add(groupId);
        }

        try (Channel channel =
                createDefaultMemoryChannel(
                        "t1",
                        blockSize,
                        segmentSize,
                        CompressionType.NONE,
                        1,
                        partitionCount,
                        consumerGroup)) {
            // create sources
            final List<Thread> sourceThread = new ArrayList<>();
            for (int i = 0; i < partitionCount; i++) {
                Thread t =
                        new SourceThread(channel, i, dataSizePerPartition, maxRecordLength, true);
                sourceThread.add(t);
            }

            // create multi sink
            final List<SinkGroup> sinGroup =
                    consumerGroup.stream()
                            .map(
                                    groupName ->
                                            new SinkGroup(
                                                    partitionCount, channel, groupName, totalSize))
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
            Assert.assertEquals(totalSize, dataSizePerPartition * partitionCount);
            Assert.assertEquals(
                    dataSizePerPartition * partitionCount * sinkGroups,
                    sinGroup.stream().mapToLong(SinkGroup::getConsumerCount).sum());
            LOG.info(
                    LOG_FORMAT,
                    writeSpend,
                    totalSize,
                    System.currentTimeMillis() - start,
                    (sinGroup.stream().mapToLong(SinkGroup::getConsumerCount).sum()));
        }
    }

    @Test
    public void testTake() throws IOException {
        final long dataSize = 1000000;
        final int blockSize = 32 * 1024;
        final long segmentSize = 1024L * 1024L * 125;
        final int consumerGroupCount = 5;
        final Set<String> consumerGroups = new HashSet<>();
        final Map<String, Offset> groupOffsets = new HashMap<>();
        for (int i = 0; i < consumerGroupCount; i++) {
            String groupId = "group_" + i;
            consumerGroups.add(groupId);
            groupOffsets.put(groupId, Offset.ZERO);
        }
        try (MemorySingleChannel channel =
                createMemorySingleChannel(
                        "t1",
                        createSingleGroupManagerFactory(groupOffsets),
                        blockSize,
                        segmentSize,
                        CompressionType.NONE,
                        1)) {
            final int writeThread = 2;
            final List<Thread> writeThreads = new ArrayList<>();
            final byte[] data = new byte[1024];
            final Random random = new Random();
            for (int i = 0; i < data.length; i++) {
                data[i] = (byte) random.nextInt();
            }
            for (int i = 0; i < writeThread; i++) {
                final Thread thread =
                        new Thread(
                                () -> {
                                    for (int j = 0; j < dataSize; j++) {
                                        try {
                                            channel.append(0, data);
                                        } catch (IOException | InterruptedException ioException) {
                                            throw new RuntimeException(ioException);
                                        }
                                    }
                                });
                writeThreads.add(thread);
            }
            List<Thread> readTreads = new ArrayList<>();
            for (String groupId : consumerGroups) {
                final Offset startOffset = channel.committedOffset(groupId, 0);
                final Thread readTread =
                        new Thread(
                                () -> {
                                    RecordsResultSet result;
                                    int readSize = 0;
                                    Offset offset = startOffset;
                                    while (readSize < dataSize * writeThread) {
                                        try {
                                            result = channel.take(0, offset);
                                            while (result.hasNext()) {
                                                result.next();
                                                readSize++;
                                            }
                                            offset = result.nexOffset();
                                        } catch (IOException | InterruptedException ioException) {
                                            throw new RuntimeException(ioException);
                                        }
                                    }
                                    channel.commit(0, groupId, offset);
                                });
                readTreads.add(readTread);
            }
            writeThreads.forEach(Thread::start);
            readTreads.forEach(Thread::start);
            writeThreads.forEach(Threads::joinQuietly);
            channel.flush();
            readTreads.forEach(Threads::joinQuietly);
        }
    }

    @Test
    public void testOnePartitionMultiWriter() throws IOException {
        for (int j = 0; j < 10; j++) {
            final int blockSize = 32 * 1024;
            final long segmentSize = 1024L * 1024L * 51;

            final String consumerGroup = "consumer_group";
            final String consumerGroup2 = "consumer_group2";
            final Map<String, Offset> groupOffsets = new HashMap<>();
            groupOffsets.put(consumerGroup, Offset.ZERO);
            groupOffsets.put(consumerGroup2, Offset.ZERO);
            final int maxRecordLength = 1024;
            final long perThreadWriteCount = 100000;
            final int writeThread = 15;
            SinkGroup sinkGroup;
            SinkGroup sinkGroup2;
            try (MemorySingleChannel channel =
                    createMemorySingleChannel(
                            "t1",
                            createSingleGroupManagerFactory(groupOffsets),
                            blockSize,
                            segmentSize,
                            CompressionType.NONE,
                            1)) {

                final List<Thread> sourceThreads = new ArrayList<>();
                for (int i = 0; i < writeThread; i++) {
                    SourceThread sourceThread =
                            new SourceThread(
                                    channel, 0, perThreadWriteCount, maxRecordLength, true);
                    sourceThreads.add(sourceThread);
                }
                sinkGroup =
                        new SinkGroup(1, channel, consumerGroup, writeThread * perThreadWriteCount);
                sinkGroup2 =
                        new SinkGroup(
                                1, channel, consumerGroup2, writeThread * perThreadWriteCount);
                for (Thread thread : sourceThreads) {
                    thread.start();
                }
                sinkGroup.start();
                sinkGroup2.start();
                sourceThreads.forEach(Threads::joinQuietly);
                channel.flush();
                Threads.joinQuietly(sinkGroup);
                Threads.joinQuietly(sinkGroup2);
            }
            Assert.assertEquals(perThreadWriteCount * writeThread, sinkGroup.getConsumerCount());
            Assert.assertEquals(perThreadWriteCount * writeThread, sinkGroup2.getConsumerCount());
        }
    }

    public static Channel createDefaultMemoryChannel(
            String topic,
            int blockSize,
            long segmentSize,
            CompressionType compressionType,
            int blockCount,
            int partition,
            Set<String> groups)
            throws IOException {
        final DefaultReadableConfig config = new DefaultReadableConfig();
        config.put(OPTION_PARTITION_COUNT, partition);
        config.put(OPTION_BLOCK_SIZE, new MemorySize(blockSize));
        config.put(OPTION_SEGMENT_SIZE, new MemorySize(segmentSize));
        config.put(OPTION_COMPRESSION, compressionType);
        config.put(OPTION_BLOCK_CACHE_PER_PARTITION_SIZE, blockCount);
        config.put(OPTION_GROUPS, new ArrayList<>(groups));
        return new MemoryChannelFactory().createChannel(topic, config);
    }
}
