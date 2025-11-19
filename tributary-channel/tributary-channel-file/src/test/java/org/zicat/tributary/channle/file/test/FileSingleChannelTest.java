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

import static org.zicat.tributary.channel.AbstractSingleChannel.*;
import static org.zicat.tributary.channel.ChannelConfigOption.OPTION_GROUPS;
import static org.zicat.tributary.channel.file.FileChannelConfigOption.OPTION_PARTITION_PATHS;
import static org.zicat.tributary.channel.test.ChannelBaseTest.testChannelCorrect;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zicat.tributary.channel.*;
import org.zicat.tributary.channel.file.FileSingleChannel;
import org.zicat.tributary.channel.file.FileSingleChannelBuilder;
import org.zicat.tributary.channel.file.FileChannelFactory;
import org.zicat.tributary.channel.test.ChannelBaseTest;
import org.zicat.tributary.common.config.ReadableConfigBuilder;
import org.zicat.tributary.common.config.ReadableConfig;
import org.zicat.tributary.common.util.IOUtils;
import org.zicat.tributary.common.test.util.FileUtils;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.TimeUnit;

/** FileChannelTest. */
@SuppressWarnings("EmptyTryBlock")
public class FileSingleChannelTest {

    private static final Logger LOG = LoggerFactory.getLogger(FileSingleChannelTest.class);
    private static final File PARENT_DIR = FileUtils.createTmpDir("file_channel_test");

    @Test
    public void testChannelStorage() throws Exception {
        final ChannelFactory factory = new FileChannelFactory();
        final ReadableConfig config =
                new ReadableConfigBuilder()
                        .addConfig(
                                OPTION_PARTITION_PATHS,
                                Collections.singletonList(
                                        new File(PARENT_DIR, "test_channel_storage/partition-")
                                                .getPath()))
                        .addConfig(OPTION_GROUPS, Arrays.asList("g1", "g2"))
                        .build();
        ChannelBaseTest.testChannelStorage(factory, "test_channel_storage", config);
    }

    @Test
    public void testBaseCorrect() throws Exception {
        final String dir = new File(PARENT_DIR, "test_base_correct/partition-").getPath();
        try (Channel channel =
                createChannel(
                        "t1",
                        new HashSet<>(Arrays.asList("g1", "g2")),
                        1,
                        dir,
                        102400,
                        1024,
                        CompressionType.SNAPPY)) {
            testChannelCorrect(channel);
        }
    }

    @Test
    public void testEmptySegment() throws IOException, InterruptedException {
        final String dir = new File(PARENT_DIR, "test_empty_segment/partition-").getPath();
        final int blockSize = 28;
        final long segmentSize = 56;
        final String groupId = "group_1";
        final String topic = "topic_21";
        try (Channel ignore =
                createChannel(
                        topic,
                        Collections.singleton(groupId),
                        1,
                        dir,
                        segmentSize,
                        blockSize,
                        CompressionType.NONE)) {}
        try (Channel ignore =
                createChannel(
                        topic,
                        Collections.singleton(groupId),
                        1,
                        dir,
                        segmentSize,
                        blockSize,
                        CompressionType.NONE)) {}
        try (Channel ignore =
                createChannel(
                        topic,
                        Collections.singleton(groupId),
                        1,
                        dir,
                        segmentSize,
                        blockSize,
                        CompressionType.NONE)) {}
        // close channel to create 3 empty segment
        try (Channel channel =
                createChannel(
                        topic,
                        Collections.singleton(groupId),
                        1,
                        dir,
                        segmentSize,
                        blockSize,
                        CompressionType.NONE)) {
            channel.append(0, "foo".getBytes(StandardCharsets.UTF_8));
            channel.flush();
            RecordsResultSet recordsResultSet =
                    channel.poll(0, Offset.ZERO, 1, TimeUnit.MILLISECONDS);
            Assert.assertFalse(recordsResultSet.isEmpty());
            while (recordsResultSet.hasNext()) {
                LOG.debug(new String(recordsResultSet.next(), StandardCharsets.UTF_8));
            }
            Assert.assertEquals(3, recordsResultSet.nexOffset().segmentId());
        }
    }

    @Test
    public void testCleanUp() throws IOException, InterruptedException {
        final String dir = new File(PARENT_DIR, "test_clean_up/partition-").getPath();
        final int blockSize = 28;
        final long segmentSize = 56;
        final String groupId = "group_1";
        final String topic = "topic_21";
        try (DefaultChannel<?> channel =
                createChannel(
                        topic,
                        Collections.singleton(groupId),
                        1,
                        dir,
                        segmentSize,
                        blockSize,
                        CompressionType.NONE)) {

            Assert.assertTrue(channel.getCleanupExpiredSegmentThread().isAlive());
            Assert.assertTrue(channel.getFlushSegmentThread().isAlive());

            channel.append(0, new byte[20]);
            channel.append(0, new byte[20]);
            channel.append(0, new byte[20]);
            channel.append(0, new byte[20]);
            channel.flush();

            Assert.assertEquals(
                    2,
                    channel.gaugeFamily()
                            .get(KEY_ACTIVE_SEGMENT.addLabel(DefaultChannel.LABEL_PARTITION, 0))
                            .intValue());
            RecordsResultSet recordsResultSet =
                    channel.poll(0, channel.committedOffset(groupId, 0), 1, TimeUnit.MILLISECONDS);
            channel.commit(0, groupId, recordsResultSet.nexOffset());
            channel.cleanUpExpiredSegmentsQuietly();
            final File[] files =
                    new File(dir + "0")
                            .listFiles(
                                    pathname ->
                                            pathname.isFile()
                                                    && pathname.getName().endsWith(".log"));
            Assert.assertNotNull(files);
            Assert.assertEquals(1, files.length);
            Assert.assertEquals(topic + "_segment_1.log", files[0].getName());
            Assert.assertEquals(
                    1,
                    channel.gaugeFamily()
                            .get(KEY_ACTIVE_SEGMENT.addLabel(DefaultChannel.LABEL_PARTITION, 0))
                            .intValue());

            channel.commit(0, groupId, recordsResultSet.nexOffset().skipNextSegmentHead());
            Assert.assertEquals(
                    1,
                    channel.gaugeFamily()
                            .get(KEY_ACTIVE_SEGMENT.addLabel(DefaultChannel.LABEL_PARTITION, 0))
                            .intValue());
        }
    }

    @Test
    public void testDataCorrection() throws IOException, InterruptedException {
        final String dir = new File(PARENT_DIR, "test_data_correction/partition-").getPath();
        final int blockSize = 28;
        final long segmentSize = 1024L * 512L;
        final String groupId = "group_1";
        final String topic = "topic_21";
        try (Channel channel =
                createChannel(
                        topic, Collections.singleton(groupId), 1, dir, segmentSize, blockSize)) {
            final String value = "test_data";
            channel.append(0, value.getBytes(StandardCharsets.UTF_8));
            channel.flush();
            Offset offset = channel.committedOffset(groupId, 0);
            RecordsResultSet resultSet = channel.poll(0, offset, 1, TimeUnit.MILLISECONDS);
            Assert.assertFalse(resultSet.isEmpty());

            resultSet = channel.take(0, Offset.ZERO);
            Assert.assertTrue(resultSet.hasNext());
            Assert.assertEquals(value, new String(resultSet.next(), StandardCharsets.UTF_8));
            Assert.assertFalse(resultSet.hasNext());
            channel.commit(0, groupId, resultSet.nexOffset());

            String value2 = "test_data2";
            channel.append(0, value2.getBytes(StandardCharsets.UTF_8));
            channel.flush();
            Offset nextOffset = resultSet.nexOffset();
            resultSet = channel.take(0, nextOffset);
            Assert.assertTrue(resultSet.hasNext());
            Assert.assertEquals(value2, new String(resultSet.next(), StandardCharsets.UTF_8));

            resultSet = channel.take(0, nextOffset);
            Assert.assertTrue(resultSet.hasNext());
            Assert.assertEquals(value2, new String(resultSet.next(), StandardCharsets.UTF_8));
            channel.commit(0, groupId, resultSet.nexOffset());

            Assert.assertEquals(
                    4d,
                    channel.gaugeFamily()
                            .get(
                                    KEY_BLOCK_CACHE_QUERY_HIT_COUNT.addLabel(
                                            DefaultChannel.LABEL_PARTITION, 0)),
                    0.01);
            Assert.assertEquals(
                    4d,
                    channel.gaugeFamily()
                            .get(
                                    KEY_BLOCK_CACHE_QUERY_TOTAL_COUNT.addLabel(
                                            DefaultChannel.LABEL_PARTITION, 0)),
                    0.01);
        }
    }

    public static Channel createChannel(
            String topic,
            Set<String> consumerGroup,
            int partitionCount,
            String dir,
            long segmentSize,
            int blockSize)
            throws IOException {
        return createChannel(
                topic,
                consumerGroup,
                partitionCount,
                dir,
                segmentSize,
                blockSize,
                CompressionType.SNAPPY);
    }

    public static DefaultChannel<FileSingleChannel> createChannel(
            String topic,
            Set<String> consumerGroup,
            int partitionCount,
            String dir,
            long segmentSize,
            int blockSize,
            CompressionType compressionType)
            throws IOException {
        final List<String> dirs = new ArrayList<>(partitionCount);
        for (int i = 0; i < partitionCount; i++) {
            dirs.add(dir + i);
        }
        FileSingleChannelBuilder builder = new FileSingleChannelBuilder().flushPeriodMills(500);
        builder =
                builder.segmentSize(segmentSize)
                        .blockSize(blockSize)
                        .consumerGroups(consumerGroup)
                        .topic(topic)
                        .compressionType(compressionType)
                        .blockCacheCount(10);
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
}
