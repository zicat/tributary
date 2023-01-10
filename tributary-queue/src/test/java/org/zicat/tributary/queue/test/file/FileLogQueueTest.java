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

package org.zicat.tributary.queue.test.file;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zicat.tributary.queue.CompressionType;
import org.zicat.tributary.queue.LogQueue;
import org.zicat.tributary.queue.RecordsOffset;
import org.zicat.tributary.queue.RecordsResultSet;
import org.zicat.tributary.queue.file.PartitionFileLogQueue;
import org.zicat.tributary.queue.file.PartitionFileLogQueueBuilder;
import org.zicat.tributary.queue.utils.IOUtils;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/** Test. */
public class FileLogQueueTest {

    private static final Logger LOG = LoggerFactory.getLogger(FileLogQueueTest.class);

    @Test
    public void testEmptySegment() throws IOException, InterruptedException {
        final String dir = "/tmp/test/test_log_empty/partition-";
        final int blockSize = 28;
        final long segmentSize = 56;
        final String groupId = "group_1";
        final String topic = "topic_21";
        createLogQueue(
                        topic,
                        Collections.singletonList(groupId),
                        1,
                        dir,
                        segmentSize,
                        blockSize,
                        7,
                        TimeUnit.SECONDS,
                        CompressionType.NONE)
                .close();
        createLogQueue(
                        topic,
                        Collections.singletonList(groupId),
                        1,
                        dir,
                        segmentSize,
                        blockSize,
                        7,
                        TimeUnit.SECONDS,
                        CompressionType.NONE)
                .close();
        createLogQueue(
                        topic,
                        Collections.singletonList(groupId),
                        1,
                        dir,
                        segmentSize,
                        blockSize,
                        7,
                        TimeUnit.SECONDS,
                        CompressionType.NONE)
                .close();
        // close log queue to create 3 empty segment
        LogQueue logQueue =
                createLogQueue(
                        topic,
                        Collections.singletonList(groupId),
                        1,
                        dir,
                        segmentSize,
                        blockSize,
                        7,
                        TimeUnit.SECONDS,
                        CompressionType.NONE);
        logQueue.append(0, "foo".getBytes(StandardCharsets.UTF_8));
        logQueue.flush();
        RecordsResultSet recordsResultSet =
                logQueue.poll(0, new RecordsOffset(0, 0), 1, TimeUnit.MILLISECONDS);
        Assert.assertFalse(recordsResultSet.isEmpty());
        while (recordsResultSet.hasNext()) {
            System.out.println(new String(recordsResultSet.next(), StandardCharsets.UTF_8));
        }
        Assert.assertEquals(3, recordsResultSet.nexRecordsOffset().segmentId());
        logQueue.close();
    }

    @Test
    public void testCleanUp()
            throws IOException, InterruptedException, NoSuchMethodException,
                    InvocationTargetException, IllegalAccessException {
        final String dir = "/tmp/test/test_log_2333/partition-";
        final int blockSize = 28;
        final long segmentSize = 56;
        final String groupId = "group_1";
        final String topic = "topic_21";
        final LogQueue logQueue =
                createLogQueue(
                        topic,
                        Collections.singletonList(groupId),
                        1,
                        dir,
                        segmentSize,
                        blockSize,
                        7,
                        TimeUnit.SECONDS,
                        CompressionType.NONE);
        logQueue.append(0, new byte[20]);
        logQueue.append(0, new byte[20]);
        logQueue.append(0, new byte[20]);
        logQueue.append(0, new byte[20]);
        logQueue.flush();
        Assert.assertEquals(2, logQueue.activeSegment());
        RecordsResultSet recordsResultSet =
                logQueue.poll(0, logQueue.getRecordsOffset(groupId, 0), 1, TimeUnit.MILLISECONDS);
        logQueue.commit(groupId, 0, recordsResultSet.nexRecordsOffset());
        Method method = PartitionFileLogQueue.class.getDeclaredMethod("cleanUpAll");
        method.setAccessible(true);
        method.invoke(logQueue);
        Assert.assertEquals(1, logQueue.activeSegment());

        logQueue.commit(groupId, 0, recordsResultSet.nexRecordsOffset().skipNextSegmentHead());
        method.invoke(logQueue);
        Assert.assertEquals(1, logQueue.activeSegment());
        IOUtils.closeQuietly(logQueue);
    }

    @Test
    public void testDataCorrection() throws IOException, InterruptedException {
        final String dir = "/tmp/test/test_log_21/partition-";
        final int blockSize = 28;
        final long segmentSize = 1024L * 512L;
        final String groupId = "group_1";
        final String topic = "topic_21";
        final LogQueue logQueue =
                createLogQueue(
                        topic, Collections.singletonList(groupId), 1, dir, segmentSize, blockSize);
        final String value = "test_data";
        logQueue.append(0, value.getBytes(StandardCharsets.UTF_8));
        logQueue.flush();
        RecordsOffset offset = logQueue.getRecordsOffset(groupId, 0);
        RecordsResultSet resultSet = logQueue.take(0, offset);
        Assert.assertTrue(resultSet.hasNext());
        Assert.assertEquals(value, new String(resultSet.next(), StandardCharsets.UTF_8));
        Assert.assertFalse(resultSet.hasNext());
        logQueue.commit(groupId, 0, resultSet.nexRecordsOffset());

        String value2 = "test_data2";
        logQueue.append(0, value2.getBytes(StandardCharsets.UTF_8));
        logQueue.flush();
        RecordsOffset nextOffset = resultSet.nexRecordsOffset();
        resultSet = logQueue.take(0, nextOffset);
        Assert.assertTrue(resultSet.hasNext());
        Assert.assertEquals(value2, new String(resultSet.next(), StandardCharsets.UTF_8));

        resultSet = logQueue.take(0, nextOffset);
        Assert.assertTrue(resultSet.hasNext());
        Assert.assertEquals(value2, new String(resultSet.next(), StandardCharsets.UTF_8));
        logQueue.commit(groupId, 0, resultSet.nexRecordsOffset());
        IOUtils.closeQuietly(logQueue);
    }

    @Test
    public void testSmallSegmentSize() throws IOException {
        final String dir = "/tmp/test/test_log_5/partition-";
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
        final String dir = "/tmp/test/test_log_3/partition-";
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
        final String dir = "/tmp/test/test_log_4/partition-";
        final int partitionCount = 1;
        final long dataSize = 1000000;
        final int blockSize = 32 * 1024;
        final long segmentSize = 1024L * 1024L * 125;
        final int consumerGroupCount = 5;
        final List<String> consumerGroups = new ArrayList<>();
        for (int i = 0; i < consumerGroupCount; i++) {
            consumerGroups.add("group_" + i);
        }
        final LogQueue logQueue =
                createLogQueue(
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
                                        logQueue.append(0, data);
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
                                RecordsOffset recordsOffset = logQueue.getRecordsOffset(groupId, 0);
                                RecordsResultSet result;
                                int readSize = 0;
                                while (readSize < dataSize * writeThread) {
                                    try {
                                        result = logQueue.take(0, recordsOffset);
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
                                    logQueue.commit(groupId, 0, recordsOffset);
                                } catch (IOException ioException) {
                                    ioException.printStackTrace();
                                }
                            });
            readTreads.add(readTread);
        }
        writeThreads.forEach(Thread::start);
        readTreads.forEach(Thread::start);
        writeThreads.forEach(FileLogQueueTest::join);
        logQueue.flush();
        readTreads.forEach(FileLogQueueTest::join);
        logQueue.close();
    }

    @Test
    public void testOnePartitionMultiWriter() throws IOException {
        for (int j = 0; j < 10; j++) {
            final String dir = "/tmp/test/test_log_6/partition-";
            final int blockSize = 32 * 1024;
            final long segmentSize = 1024L * 1024L * 51;
            final int partitionCount = 1;
            final String consumerGroup = "consumer_group";
            final int maxRecordLength = 1024;
            final LogQueue logQueue =
                    FileLogQueueTest.createLogQueue(
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
                        new SourceThread(logQueue, 0, perThreadWriteCount, maxRecordLength, true);
                sourceThreads.add(sourceThread);
            }
            SinkGroup sinkGroup =
                    new SinkGroup(1, logQueue, consumerGroup, writeThread * perThreadWriteCount);
            for (Thread thread : sourceThreads) {
                thread.start();
            }
            sinkGroup.start();

            sourceThreads.forEach(FileLogQueueTest::join);
            logQueue.flush();
            FileLogQueueTest.join(sinkGroup);
            logQueue.close();
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

        final LogQueue logQueue =
                createLogQueue("voqa", consumerGroup, partitionCount, dir, segmentSize, blockSize);

        // create sources
        final List<Thread> sourceThread = new ArrayList<>();
        for (int i = 0; i < partitionCount; i++) {
            Thread t = new SourceThread(logQueue, i, dataSize, maxRecordLength, random);
            sourceThread.add(t);
        }

        // create multi sink
        final List<SinkGroup> sinGroup =
                consumerGroup.stream()
                        .map(
                                groupName ->
                                        new SinkGroup(
                                                partitionCount, logQueue, groupName, totalSize))
                        .collect(Collectors.toList());

        long start = System.currentTimeMillis();
        // start source and sink threads
        sourceThread.forEach(Thread::start);
        sinGroup.forEach(Thread::start);

        // waiting source threads finish and flush
        sourceThread.forEach(FileLogQueueTest::join);
        logQueue.flush();
        long writeSpend = System.currentTimeMillis() - start;

        // waiting sink threads finish.
        sinGroup.forEach(FileLogQueueTest::join);
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
        logQueue.close();
    }

    public static LogQueue createLogQueue(
            String topic,
            List<String> consumerGroup,
            int partitionCount,
            String dir,
            long segmentSize,
            int blockSize) {
        return createLogQueue(
                topic,
                consumerGroup,
                partitionCount,
                dir,
                segmentSize,
                blockSize,
                7,
                TimeUnit.SECONDS,
                CompressionType.SNAPPY);
    }

    public static LogQueue createLogQueue(
            String topic,
            List<String> consumerGroup,
            int partitionCount,
            String dir,
            long segmentSize,
            int blockSize,
            int cleanUpPeriod,
            TimeUnit cleanUpTimeUnit,
            CompressionType compressionType) {
        final List<File> dirs = new ArrayList<>(partitionCount);
        for (int i = 0; i < partitionCount; i++) {
            dirs.add(new File(dir + i));
        }
        final PartitionFileLogQueueBuilder builder = PartitionFileLogQueueBuilder.newBuilder();
        builder.segmentSize(segmentSize)
                .blockSize(blockSize)
                .consumerGroups(consumerGroup)
                .topic(topic)
                .cleanUpPeriod(cleanUpPeriod, cleanUpTimeUnit)
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
        IOUtils.deleteDir(new File("/tmp/test"));
    }

    public static void join(Thread t) {
        try {
            t.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
