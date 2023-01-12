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
import org.zicat.tributary.queue.LogQueue;
import org.zicat.tributary.queue.file.FileLogQueueBuilder;
import org.zicat.tributary.queue.file.PartitionFileLogQueueBuilder;
import org.zicat.tributary.queue.test.utils.FileUtils;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;

import static org.zicat.tributary.queue.utils.IOUtils.deleteDir;
import static org.zicat.tributary.queue.utils.IOUtils.makeDir;

/** FileLogQueueBuilderTest. */
public class FileLogQueueBuilderTest {

    private static final File DIR = FileUtils.createTmpDir("file_log_queue_builder_test");

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
    public void testFileLogQueueBuild() {
        FileLogQueueBuilder builder = new FileLogQueueBuilder();
        try {
            builder.build();
            Assert.fail();
        } catch (Exception e) {
            Assert.assertTrue(true);
        }
        builder = new FileLogQueueBuilder().dir(DIR);
        try {
            builder.build();
            Assert.fail();
        } catch (Exception e) {
            Assert.assertTrue(true);
        }

        builder = new FileLogQueueBuilder().dir(DIR);
        builder.consumerGroups(Arrays.asList("1", "2"))
                .blockSize(100)
                .segmentSize(10000L)
                .topic("t2");
        LogQueue logQueue = builder.build();
        Assert.assertNotNull(builder.toString(), logQueue.topic());
    }

    @Test
    public void testPartitionFileLogQueue() {
        PartitionFileLogQueueBuilder builder = new PartitionFileLogQueueBuilder();
        builder.dirs(Collections.singletonList(DIR));
        builder.consumerGroups(Arrays.asList("3", "2"))
                .blockSize(200)
                .segmentSize(3000L)
                .topic("t3");
        LogQueue logQueue = builder.build();
        Assert.assertEquals(0, logQueue.lastSegmentId(0));
        Assert.assertNotNull(builder.toString(), logQueue.topic());
        try {
            logQueue.lastSegmentId(2);
            Assert.fail();
        } catch (Exception ignore) {
            Assert.assertTrue(true);
        }
    }
}
