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

import org.zicat.tributary.channel.file.FileSegmentBuilder;
import static org.zicat.tributary.channel.test.StringTestUtils.createStringByLength;
import static org.zicat.tributary.common.IOUtils.deleteDir;
import static org.zicat.tributary.common.IOUtils.makeDir;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.zicat.tributary.channel.BlockReaderOffset;
import org.zicat.tributary.channel.BlockWriter;
import org.zicat.tributary.channel.RecordsResultSet;
import org.zicat.tributary.channel.file.FileSegment;
import org.zicat.tributary.common.IOUtils;
import org.zicat.tributary.common.test.FileUtils;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/** SegmentTest. */
public class FileSegmentTest {

    private static final File DIR = FileUtils.createTmpDir("log_segment_test");

    @Test
    public void testAppend() throws IOException {
        final File childDir = new File(DIR, "test_append");
        makeDir(childDir);
        final FileSegmentBuilder builder = new FileSegmentBuilder();
        final FileSegment segment =
                builder.segmentSize(64L).fileId(1).dir(childDir).build(new BlockWriter(16));
        Assert.assertTrue(segment.append("".getBytes(), 0, 0).appended());

        testAppend(6, segment);
        testAppend(20, segment);
        testAppend(12, segment);
        testAppend(50, segment);
        Assert.assertFalse(
                segment.append(createStringByLength(50).getBytes(StandardCharsets.UTF_8), 0, 50)
                        .appended());
        Assert.assertFalse(
                segment.append(createStringByLength(5).getBytes(StandardCharsets.UTF_8), 0, 5)
                        .appended());
        IOUtils.closeQuietly(segment);
    }

    @Test
    public void testRead() throws IOException, InterruptedException {
        final File childDir = new File(DIR, "test_read");
        makeDir(childDir);
        final FileSegmentBuilder builder = new FileSegmentBuilder();
        final int fileId = 1;
        final FileSegment segment =
                builder.segmentSize(64L).fileId(fileId).dir(childDir).build(new BlockWriter(16));
        Assert.assertTrue(segment.append("".getBytes(), 0, 0).appended());
        testAppend(6, segment);
        testAppend(20, segment);
        segment.readonly();
        Assert.assertFalse(
                segment.append(createStringByLength(6).getBytes(StandardCharsets.UTF_8), 0, 6)
                        .appended());

        final List<String> result = new ArrayList<>();
        result.add(createStringByLength(6));
        result.add(createStringByLength(20));
        int i = 0;

        final BlockReaderOffset blockReaderOffset = BlockReaderOffset.cast(fileId);

        RecordsResultSet resultSet =
                segment.readBlock(blockReaderOffset, 1, TimeUnit.MILLISECONDS).toResultSet();
        Assert.assertTrue(resultSet.hasNext());
        while (resultSet.hasNext()) {
            byte[] bs = resultSet.next();
            Assert.assertEquals(result.get(i), new String(bs, StandardCharsets.UTF_8));
            i++;
        }

        resultSet =
                segment.readBlock(
                                BlockReaderOffset.cast(resultSet.nexOffset()),
                                1,
                                TimeUnit.MILLISECONDS)
                        .toResultSet();
        Assert.assertTrue(resultSet.hasNext());
        while (resultSet.hasNext()) {
            byte[] bs = resultSet.next();
            Assert.assertEquals(result.get(i), new String(bs, StandardCharsets.UTF_8));
            i++;
        }

        resultSet =
                segment.readBlock(
                                BlockReaderOffset.cast(resultSet.nexOffset()),
                                1,
                                TimeUnit.MILLISECONDS)
                        .toResultSet();
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
    private void testAppend(int length, FileSegment segment) throws IOException {
        Assert.assertTrue(
                segment.append(
                                createStringByLength(length).getBytes(StandardCharsets.UTF_8),
                                0,
                                length)
                        .appended());
    }

    @BeforeClass
    public static void before() throws IOException {
        deleteDir(DIR);
        if (!makeDir(DIR)) {
            throw new IOException("create dir fail, " + DIR.getPath());
        }
    }

    @AfterClass
    public static void after() {
        deleteDir(DIR);
    }
}
