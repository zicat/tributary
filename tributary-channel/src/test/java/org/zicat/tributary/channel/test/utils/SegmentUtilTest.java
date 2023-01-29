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

package org.zicat.tributary.channel.test.utils;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.zicat.tributary.channel.BufferWriter;
import org.zicat.tributary.channel.file.Segment;
import org.zicat.tributary.channel.file.SegmentBuilder;
import org.zicat.tributary.channel.file.SegmentUtil;

import java.io.File;
import java.io.IOException;

import static org.zicat.tributary.channel.utils.IOUtils.deleteDir;
import static org.zicat.tributary.channel.utils.IOUtils.makeDir;

/** SegmentUtilTest. */
public class SegmentUtilTest {

    private static final File DIR = FileUtils.createTmpDir("segment_util_test");

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
    public void testMinMax() {
        final File childDir = new File(DIR, "test_min_max");
        makeDir(childDir);
        final SegmentBuilder builder = new SegmentBuilder();
        final Segment segment1 =
                builder.segmentSize(64L).fileId(1).dir(childDir).build(new BufferWriter(16));
        final Segment segment2 =
                builder.segmentSize(64L).fileId(2).dir(childDir).build(new BufferWriter(16));
        final Segment segment3 =
                builder.segmentSize(64L).fileId(3).dir(childDir).build(new BufferWriter(16));

        Assert.assertSame(segment1, SegmentUtil.min(segment1, segment2));
        Assert.assertSame(segment2, SegmentUtil.max(segment1, segment2));
        Assert.assertTrue(
                segment1 == SegmentUtil.max(segment1, segment3)
                        || segment3 == SegmentUtil.max(segment1, segment3));
        Assert.assertTrue(
                segment1 == SegmentUtil.min(segment1, segment3)
                        || segment3 == SegmentUtil.min(segment1, segment3));
    }
}
