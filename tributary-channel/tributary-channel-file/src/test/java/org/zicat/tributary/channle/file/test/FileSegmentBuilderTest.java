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
import static org.zicat.tributary.common.util.IOUtils.deleteDir;
import static org.zicat.tributary.common.util.IOUtils.makeDir;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.zicat.tributary.channel.BlockWriter;
import org.zicat.tributary.channel.CompressionType;
import org.zicat.tributary.channel.file.FileSegment;
import org.zicat.tributary.common.util.IOUtils;
import org.zicat.tributary.common.test.util.FileUtils;

import java.io.File;
import java.io.IOException;

/** FileSegmentBuilderTest. */
public class FileSegmentBuilderTest {

    private static final File DIR = FileUtils.createTmpDir("log_segment_builder_test");

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

    @Test
    public void test() throws IOException {

        final FileSegmentBuilder builder = new FileSegmentBuilder().fileId(1).dir(DIR);

        final int blockSize = 1024;
        final BlockWriter bw1 = new BlockWriter(blockSize);
        final FileSegment segment =
                builder.segmentSize(1024000L).compressionType(CompressionType.SNAPPY).build(bw1);
        Assert.assertNotNull(segment);
        IOUtils.closeQuietly(segment);

        final BlockWriter bw2 = new BlockWriter(blockSize * 2);
        final FileSegment segment2 = builder.compressionType(CompressionType.ZSTD).build(bw2);
        Assert.assertEquals(CompressionType.SNAPPY, segment2.compressionType());
        Assert.assertEquals(1024, segment2.blockSize());
    }
}
