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
import org.zicat.tributary.channel.Channel;
import org.zicat.tributary.channel.file.FileChannelBuilder;
import org.zicat.tributary.channel.file.OnePartitionFileChannelBuilder;
import org.zicat.tributary.common.test.FileUtils;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;

import static org.zicat.tributary.common.IOUtils.deleteDir;
import static org.zicat.tributary.common.IOUtils.makeDir;

/** OnePartitionOneFileChannelBuilderTest. */
public class OnePartitionOneFileChannelBuilderTest {

    private static final File DIR = FileUtils.createTmpDir("file_channel_builder_test");

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
    public void testFileChannelBuild() {
        OnePartitionFileChannelBuilder builder = new OnePartitionFileChannelBuilder();
        try {
            builder.build();
            Assert.fail();
        } catch (Exception e) {
            Assert.assertTrue(true);
        }
        builder = new OnePartitionFileChannelBuilder().dir(DIR);
        try {
            builder.build();
            Assert.fail();
        } catch (Exception e) {
            Assert.assertTrue(true);
        }

        builder = new OnePartitionFileChannelBuilder().dir(DIR);
        builder.consumerGroups(Arrays.asList("1", "2"))
                .blockSize(100)
                .segmentSize(10000L)
                .topic("t2");

        Channel channel = builder.build();
        Assert.assertNotNull(builder.toString(), channel.topic());
    }

    @Test
    public void testPartitionFileChannel() {
        FileChannelBuilder builder = new FileChannelBuilder();
        builder.dirs(Collections.singletonList(DIR));
        builder.consumerGroups(Arrays.asList("3", "2"))
                .blockSize(200)
                .segmentSize(3000L)
                .topic("t3");
        Channel channel = builder.build();
        Assert.assertEquals(0, channel.lastSegmentId(0));
        Assert.assertNotNull(builder.toString(), channel.topic());
        try {
            channel.lastSegmentId(2);
            Assert.fail();
        } catch (Exception ignore) {
            Assert.assertTrue(true);
        }
    }
}
