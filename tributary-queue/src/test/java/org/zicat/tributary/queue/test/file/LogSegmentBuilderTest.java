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
import org.zicat.tributary.queue.BufferWriter;
import org.zicat.tributary.queue.CompressionType;
import org.zicat.tributary.queue.file.LogSegment;
import org.zicat.tributary.queue.file.LogSegmentBuilder;
import org.zicat.tributary.queue.utils.IOUtils;

import java.io.File;
import java.io.IOException;

import static org.zicat.tributary.queue.utils.IOUtils.deleteDir;
import static org.zicat.tributary.queue.utils.IOUtils.makeDir;

/** LogSegmentBuilderTest. */
public class LogSegmentBuilderTest {

    private static final File dir = new File("/tmp/segment_build_test/");

    @Test
    public void test() {

        final LogSegmentBuilder builder = new LogSegmentBuilder();
        try {
            builder.segmentSize(1025L).fileId(1).dir(dir).build(new BufferWriter(1024));
            Assert.fail();
        } catch (RuntimeException e) {
            Assert.assertTrue(true);
        }

        final int blockSize = 1024;
        final BufferWriter bw1 = new BufferWriter(blockSize);
        final LogSegment logSegment =
                builder.segmentSize(1024000L).compressionType(CompressionType.SNAPPY).build(bw1);
        Assert.assertNotNull(logSegment);
        IOUtils.closeQuietly(logSegment);

        final BufferWriter bw2 = new BufferWriter(blockSize * 2);
        final LogSegment logSegment2 = builder.compressionType(CompressionType.ZSTD).build(bw2);
        Assert.assertEquals(CompressionType.SNAPPY, logSegment2.compressionType());
        Assert.assertEquals(1024, logSegment2.blockSize());
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
