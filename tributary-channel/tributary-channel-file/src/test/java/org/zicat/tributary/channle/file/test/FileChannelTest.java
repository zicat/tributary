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

package org.zicat.tributary.channle.file.test;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zicat.tributary.channel.Channel;
import org.zicat.tributary.channel.CompressionType;
import org.zicat.tributary.channel.RecordsOffset;
import org.zicat.tributary.channel.RecordsResultSet;
import org.zicat.tributary.channel.file.PartitionFileChannelBuilder;
import org.zicat.tributary.common.IOUtils;
import org.zicat.tributary.common.test.FileUtils;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/** Test. */
public class FileChannelTest {

    private static final Logger LOG = LoggerFactory.getLogger(FileChannelTest.class);
    private static final File PARENT_DIR = FileUtils.createTmpDir("file_channel_test");

    @Test
    public void testEmptySegment() throws IOException, InterruptedException {
        final String dir = new File(PARENT_DIR, "test_empty_segment/partition-").getPath();
        final int blockSize = 28;
        final long segmentSize = 56;
        final String groupId = "group_1";
        final String topic = "topic_21";
        createChannel(
                        topic,
                        Collections.singletonList(groupId),
                        1,
                        dir,
                        segmentSize,
                        blockSize,
                        CompressionType.NONE)
                .close();
        createChannel(
                        topic,
                        Collections.singletonList(groupId),
                        1,
                        dir,
                        segmentSize,
                        blockSize,
                        CompressionType.NONE)
                .close();
        createChannel(
                        topic,
                        Collections.singletonList(groupId),
                        1,
                        dir,
                        segmentSize,
                        blockSize,
                        CompressionType.NONE)
                .close();
        // close channel to create 3 empty segment
        Channel channel =
                createChannel(
                        topic,
                        Collections.singletonList(groupId),
                        1,
                        dir,
                        segmentSize,
                        blockSize,
                        CompressionType.NONE);
        channel.append(0, "foo".getBytes(StandardCharsets.UTF_8));
        channel.flush();
        RecordsResultSet recordsResultSet =
                channel.poll(0, new RecordsOffset(0, 0), 1, TimeUnit.MILLISECONDS);
        Assert.assertFalse(recordsResultSet.isEmpty());
        while (recordsResultSet.hasNext()) {
            LOG.debug(new String(recordsResultSet.next(), StandardCharsets.UTF_8));
        }
        Assert.assertEquals(3, recordsResultSet.nexRecordsOffset().segmentId());
        channel.close();
    }

    @Test
    public void testCleanUp() throws IOException, InterruptedException {
        final String dir = new File(PARENT_DIR, "test_clean_up/partition-").getPath();
        final int blockSize = 28;
        final long segmentSize = 56;
        final String groupId = "group_1";
        final String topic = "topic_21";
        final Channel channel =
                createChannel(
                        topic,
                        Collections.singletonList(groupId),
                        1,
                        dir,
                        segmentSize,
                        blockSize,
                        CompressionType.NONE);
        channel.append(0, new byte[20]);
        channel.append(0, new byte[20]);
        channel.append(0, new byte[20]);
        channel.append(0, new byte[20]);
        channel.flush();
        Assert.assertEquals(2, channel.activeSegment());
        RecordsResultSet recordsResultSet =
                channel.poll(0, channel.getRecordsOffset(groupId, 0), 1, TimeUnit.MILLISECONDS);
        channel.commit(groupId, 0, recordsResultSet.nexRecordsOffset());
        Assert.assertEquals(1, channel.activeSegment());

        channel.commit(groupId, 0, recordsResultSet.nexRecordsOffset().skipNextSegmentHead());
        Assert.assertEquals(1, channel.activeSegment());
        IOUtils.closeQuietly(channel);
    }

    @Test
    public void testDataCorrection() throws IOException, InterruptedException {
        final String dir = new File(PARENT_DIR, "test_data_correction/partition-").getPath();
        final int blockSize = 28;
        final long segmentSize = 1024L * 512L;
        final String groupId = "group_1";
        final String topic = "topic_21";
        final Channel channel =
                createChannel(
                        topic, Collections.singletonList(groupId), 1, dir, segmentSize, blockSize);
        final String value = "test_data";
        channel.append(0, value.getBytes(StandardCharsets.UTF_8));
        channel.flush();
        RecordsOffset offset = channel.getRecordsOffset(groupId, 0);
        RecordsResultSet resultSet = channel.take(0, offset);
        Assert.assertTrue(resultSet.hasNext());
        Assert.assertEquals(value, new String(resultSet.next(), StandardCharsets.UTF_8));
        Assert.assertFalse(resultSet.hasNext());
        channel.commit(groupId, 0, resultSet.nexRecordsOffset());

        String value2 = "test_data2";
        channel.append(0, value2.getBytes(StandardCharsets.UTF_8));
        channel.flush();
        RecordsOffset nextOffset = resultSet.nexRecordsOffset();
        resultSet = channel.take(0, nextOffset);
        Assert.assertTrue(resultSet.hasNext());
        Assert.assertEquals(value2, new String(resultSet.next(), StandardCharsets.UTF_8));

        resultSet = channel.take(0, nextOffset);
        Assert.assertTrue(resultSet.hasNext());
        Assert.assertEquals(value2, new String(resultSet.next(), StandardCharsets.UTF_8));
        channel.commit(groupId, 0, resultSet.nexRecordsOffset());
        IOUtils.closeQuietly(channel);
    }

    @Test
    public void testSmallSegmentSize() throws IOException {
        final String dir = new File(PARENT_DIR, "test_small_segment_size/partition-").getPath();
        final int partitionCount = 1;
        final long dataSize = 500001;
        final int sinkGroups = 2;
        final int blockSize = 28;
        final long segmentSize = 1024L * 512L;
        final int maxRecordLength = 32;
        for (int i = 0; i < 5; i++) {
            test(
                    dir,
                    partitionCount,
                    dataSize,
                    sinkGroups,
                    blockSize,
                    segmentSize,
                    maxRecordLength,
                    true);
        }
    }

    @Test
    public void testPartitionAndSinkGroups() throws IOException {
        final String dir =
                new File(PARENT_DIR, "test_partition_and_sink_groups/partition-").getPath();
        final int partitionCount = 3;
        final long dataSize = 500000;
        final int sinkGroups = 4;
        final int blockSize = 32 * 1024;
        final long segmentSize = 1024L * 1024L * 512;
        final int maxRecordLength = 1024;
        for (int i = 0; i < 5; i++) {
            test(
                    dir,
                    partitionCount,
                    dataSize,
                    sinkGroups,
                    blockSize,
                    segmentSize,
                    maxRecordLength,
                    false);
        }
    }

    @Test
    public void testTake() throws IOException {
        final String dir = new File(PARENT_DIR, "test_take/partition-").getPath();
        final int partitionCount = 1;
        final long dataSize = 1000000;
        final int blockSize = 32 * 1024;
        final long segmentSize = 1024L * 1024L * 125;
        final int consumerGroupCount = 5;
        final List<String> consumerGroups = new ArrayList<>();
        for (int i = 0; i < consumerGroupCount; i++) {
            consumerGroups.add("group_" + i);
        }
        final Channel channel =
                createChannel(
                        "counter", consumerGroups, partitionCount, dir, segmentSize, blockSize);

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
                                    } catch (IOException ioException) {
                                        ioException.printStackTrace();
                                    }
                                }
                            });
            writeThreads.add(thread);
        }

        List<Thread> readTreads = new ArrayList<>();
        for (String groupId : consumerGroups) {
            final Thread readTread =
                    new Thread(
                            () -> {
                                RecordsOffset recordsOffset = channel.getRecordsOffset(groupId, 0);
                                RecordsResultSet result;
                                int readSize = 0;
                                while (readSize < dataSize * writeThread) {
                                    try {
                                        result = channel.take(0, recordsOffset);
                                        while (result.hasNext()) {
                                            result.next();
                                            readSize++;
                                        }
                                        recordsOffset = result.nexRecordsOffset();
                                    } catch (IOException | InterruptedException ioException) {
                                        ioException.printStackTrace();
                                    }
                                }
                                try {
                                    channel.commit(groupId, 0, recordsOffset);
                                } catch (IOException ioException) {
                                    ioException.printStackTrace();
                                }
                            });
            readTreads.add(readTread);
        }
        writeThreads.forEach(Thread::start);
        readTreads.forEach(Thread::start);
        writeThreads.forEach(FileChannelTest::join);
        channel.flush();
        readTreads.forEach(FileChannelTest::join);
        channel.close();
    }

    @Test
    public void testOnePartitionMultiWriter() throws IOException {
        for (int j = 0; j < 10; j++) {
            final String dir =
                    new File(PARENT_DIR, "test_one_partition_multi_writer/partition-").getPath();
            final int blockSize = 32 * 1024;
            final long segmentSize = 1024L * 1024L * 51;
            final int partitionCount = 1;
            final String consumerGroup = "consumer_group";
            final int maxRecordLength = 1024;
            final Channel channel =
                    FileChannelTest.createChannel(
                            "event",
                            Collections.singletonList(consumerGroup),
                            partitionCount,
                            dir,
                            segmentSize,
                            blockSize);

            final long perThreadWriteCount = 100000;
            final int writeThread = 15;
            List<Thread> sourceThreads = new ArrayList<>();
            for (int i = 0; i < writeThread; i++) {
                SourceThread sourceThread =
                        new SourceThread(channel, 0, perThreadWriteCount, maxRecordLength, true);
                sourceThreads.add(sourceThread);
            }
            SinkGroup sinkGroup =
                    new SinkGroup(1, channel, consumerGroup, writeThread * perThreadWriteCount);
            for (Thread thread : sourceThreads) {
                thread.start();
            }
            sinkGroup.start();

            sourceThreads.forEach(FileChannelTest::join);
            channel.flush();
            FileChannelTest.join(sinkGroup);
            channel.close();
            Assert.assertEquals(perThreadWriteCount * writeThread, sinkGroup.getConsumerCount());
        }
    }

    private static void test(
            String dir,
            int partitionCount,
            long dataSize,
            int sinkGroups,
            int blockSize,
            long segmentSize,
            int maxRecordLength,
            boolean random)
            throws IOException {

        final long totalSize = dataSize * partitionCount;
        List<String> consumerGroup = new ArrayList<>(sinkGroups);
        for (int i = 0; i < sinkGroups; i++) {
            consumerGroup.add("consumer_group_" + i);
        }

        final Channel channel =
                createChannel("voqa", consumerGroup, partitionCount, dir, segmentSize, blockSize);

        // create sources
        final List<Thread> sourceThread = new ArrayList<>();
        for (int i = 0; i < partitionCount; i++) {
            Thread t = new SourceThread(channel, i, dataSize, maxRecordLength, random);
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
        sourceThread.forEach(FileChannelTest::join);
        channel.flush();
        long writeSpend = System.currentTimeMillis() - start;

        // waiting sink threads finish.
        sinGroup.forEach(FileChannelTest::join);
        Assert.assertEquals(totalSize, dataSize * partitionCount);
        Assert.assertEquals(
                sinGroup.stream().mapToLong(SinkGroup::getConsumerCount).sum(),
                dataSize * partitionCount * sinkGroups);
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
        channel.close();
    }

    public static Channel createChannel(
            String topic,
            List<String> consumerGroup,
            int partitionCount,
            String dir,
            long segmentSize,
            int blockSize) {
        return createChannel(
                topic,
                consumerGroup,
                partitionCount,
                dir,
                segmentSize,
                blockSize,
                CompressionType.SNAPPY);
    }

    public static Channel createChannel(
            String topic,
            List<String> consumerGroup,
            int partitionCount,
            String dir,
            long segmentSize,
            int blockSize,
            CompressionType compressionType) {
        final List<File> dirs = new ArrayList<>(partitionCount);
        for (int i = 0; i < partitionCount; i++) {
            dirs.add(new File(dir + i));
        }
        final PartitionFileChannelBuilder builder = PartitionFileChannelBuilder.newBuilder();
        builder.segmentSize(segmentSize)
                .blockSize(blockSize)
                .consumerGroups(consumerGroup)
                .topic(topic)
                .compressionType(compressionType)
                .flushPeriod(500, TimeUnit.MILLISECONDS);
        return builder.dirs(dirs).build();
    }

    @AfterClass
    public static void afterTest() {
        beforeTest();
    }

    @BeforeClass
    public static void beforeTest() {
        IOUtils.deleteDir(PARENT_DIR);
    }

    public static void join(Thread t) {
        try {
            t.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}