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
import org.zicat.tributary.queue.BufferRecordsResultSet;
import org.zicat.tributary.queue.BufferWriter;
import org.zicat.tributary.queue.RecordsOffset;
import org.zicat.tributary.queue.RecordsResultSet;
import org.zicat.tributary.queue.file.LogSegment;
import org.zicat.tributary.queue.file.LogSegmentBuilder;
import org.zicat.tributary.queue.utils.IOUtils;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.zicat.tributary.queue.utils.IOUtils.deleteDir;
import static org.zicat.tributary.queue.utils.IOUtils.makeDir;

/** LogSegment. */
public class LogSegmentTest {

    private static final File dir = new File("/tmp/segment_test/");

    @Test
    public void testAppend() throws IOException {
        final File childDir = new File(dir, "test_append");
        makeDir(childDir);
        final LogSegmentBuilder builder = new LogSegmentBuilder();
        final LogSegment segment =
                builder.segmentSize(64L).fileId(1).dir(childDir).build(new BufferWriter(16));
        Assert.assertTrue(segment.append("".getBytes(), 0, 0));

        testAppend(6, segment);
        testAppend(20, segment);
        testAppend(12, segment);
        testAppend(50, segment);
        Assert.assertFalse(
                segment.append(createStringByLength(50).getBytes(StandardCharsets.UTF_8), 0, 50));
        Assert.assertFalse(
                segment.append(createStringByLength(5).getBytes(StandardCharsets.UTF_8), 0, 5));
        IOUtils.closeQuietly(segment);
    }

    @Test
    public void testMultiThread() throws InterruptedException, IOException {
        final File childDir = new File(dir, "test_multi_thread");
        makeDir(childDir);
        final LogSegmentBuilder builder = new LogSegmentBuilder();
        final int fileId = 1;
        final LogSegment segment =
                builder.segmentSize(64L).fileId(fileId).dir(childDir).build(new BufferWriter(16));
        Thread writerThread =
                new Thread(
                        () -> {
                            try {
                                Assert.assertTrue(segment.append("".getBytes(), 0, 0));
                                testAppend(6, segment);
                                testAppend(20, segment);
                            } catch (IOException ioException) {
                                throw new RuntimeException(ioException);
                            }
                        });
        Thread readThread =
                new Thread(
                        () -> {
                            final List<String> result = new ArrayList<>();
                            result.add(createStringByLength(6));
                            result.add(createStringByLength(20));

                            RecordsOffset recordsOffset = new RecordsOffset(fileId, 0);
                            while (!result.isEmpty()) {
                                RecordsResultSet resultSet;
                                try {
                                    resultSet =
                                            segment.readBlock(
                                                    recordsOffset, 1, TimeUnit.MILLISECONDS);
                                    Assert.assertTrue(resultSet.hasNext());
                                    while (resultSet.hasNext()) {
                                        byte[] bs = resultSet.next();
                                        Assert.assertTrue(
                                                result.remove(
                                                        new String(bs, StandardCharsets.UTF_8)));
                                    }
                                    recordsOffset = resultSet.nexRecordsOffset();
                                } catch (Exception e) {
                                    throw new RuntimeException(e);
                                }
                            }
                        });
        writerThread.start();
        readThread.start();
        writerThread.join();
        segment.finish();
        readThread.join(1000);
        readThread.interrupt();
    }

    @Test
    public void testRead() throws IOException, InterruptedException {
        final File childDir = new File(dir, "test_read");
        makeDir(childDir);
        final LogSegmentBuilder builder = new LogSegmentBuilder();
        final int fileId = 1;
        final LogSegment segment =
                builder.segmentSize(64L).fileId(fileId).dir(childDir).build(new BufferWriter(16));
        Assert.assertTrue(segment.append("".getBytes(), 0, 0));
        testAppend(6, segment);
        testAppend(20, segment);
        segment.finish();
        Assert.assertFalse(
                segment.append(createStringByLength(6).getBytes(StandardCharsets.UTF_8), 0, 6));

        final List<String> result = new ArrayList<>();
        result.add(createStringByLength(6));
        result.add(createStringByLength(20));
        int i = 0;

        final BufferRecordsResultSet bufferRecordsResultSet = BufferRecordsResultSet.cast(fileId);

        RecordsResultSet resultSet =
                segment.readBlock(bufferRecordsResultSet, 1, TimeUnit.MILLISECONDS);
        Assert.assertTrue(resultSet.hasNext());
        while (resultSet.hasNext()) {
            byte[] bs = resultSet.next();
            Assert.assertEquals(result.get(i), new String(bs, StandardCharsets.UTF_8));
            i++;
        }

        resultSet = segment.readBlock(resultSet.nexRecordsOffset(), 1, TimeUnit.MILLISECONDS);
        Assert.assertTrue(resultSet.hasNext());
        while (resultSet.hasNext()) {
            byte[] bs = resultSet.next();
            Assert.assertEquals(result.get(i), new String(bs, StandardCharsets.UTF_8));
            i++;
        }

        resultSet = segment.readBlock(resultSet.nexRecordsOffset(), 1, TimeUnit.MILLISECONDS);
        Assert.assertFalse(resultSet.hasNext());

        IOUtils.closeQuietly(segment);
    }

    /**
     * test append.
     *
     * @param length length
     * @param segment segment
     * @throws IOException IOException
     */
    private void testAppend(int length, LogSegment segment) throws IOException {
        Assert.assertTrue(
                segment.append(
                        createStringByLength(length).getBytes(StandardCharsets.UTF_8), 0, length));
    }

    /**
     * create test string data.
     *
     * @param a a
     * @return string
     */
    public static String createStringByLength(int a) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < a; i++) {
            sb.append("a");
        }
        return sb.toString();
    }

    @BeforeClass
    public static void before() throws IOException {
        deleteDir(dir);
        if (!makeDir(dir)) {
            throw new IOException("create dir fail, " + dir.getPath());
        }
    }

    @AfterClass
    public static void after() {
        deleteDir(dir);
    }
}
