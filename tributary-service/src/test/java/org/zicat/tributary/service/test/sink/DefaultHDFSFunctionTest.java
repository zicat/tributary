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

package org.zicat.tributary.service.test.sink;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.zicat.tributary.channel.RecordsOffset;
import org.zicat.tributary.channel.test.utils.FileUtils;
import org.zicat.tributary.service.sink.DefaultHDFSFunction;
import org.zicat.tributary.sink.function.ContextBuilder;
import org.zicat.tributary.sink.test.function.MockClock;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;

import static org.zicat.tributary.channel.utils.IOUtils.deleteDir;
import static org.zicat.tributary.channel.utils.IOUtils.makeDir;
import static org.zicat.tributary.service.sink.DefaultHDFSFunction.KEY_BUCKET_DATE_FORMAT;
import static org.zicat.tributary.service.sink.DefaultHDFSFunction.KEY_IDLE_MILLIS;
import static org.zicat.tributary.sink.Config.CLOCK;
import static org.zicat.tributary.sink.hdfs.AbstractHDFSFunction.BASE_SINK_PATH;

/** DefaultHDFSFunctionTest. */
public class DefaultHDFSFunctionTest {

    private static final File DIR = FileUtils.createTmpDir("default_hdfs_function_test");

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
    public void test() throws Throwable {
        final DefaultHDFSFunction defaultHDFSFunction = new DefaultHDFSFunction();
        final MockClock mockClock = new MockClock();
        mockClock.setCurrentTimeMillis(System.currentTimeMillis());
        final ContextBuilder builder =
                new ContextBuilder()
                        .groupId("g1")
                        .id("id1")
                        .partitionId(0)
                        .startRecordsOffset(null)
                        .topic("t1");
        builder.addCustomProperty(BASE_SINK_PATH, DIR.getCanonicalFile().getPath())
                .addCustomProperty(KEY_IDLE_MILLIS, 10000)
                .addCustomProperty(KEY_BUCKET_DATE_FORMAT, "yyyyMMdd_HHmm")
                .addCustomProperty(CLOCK, mockClock);

        defaultHDFSFunction.open(builder.build());

        final RecordsOffset recordsOffset = new RecordsOffset(1, 0);
        defaultHDFSFunction.process(
                recordsOffset, Arrays.asList("aa".getBytes(), "bb".getBytes()).listIterator());
        String currentBucketPath = currentBucketPath(defaultHDFSFunction);

        // refresh by time rolling
        mockClock.setCurrentTimeMillis(mockClock.currentTimeMillis() + 120 * 1000);
        defaultHDFSFunction.refresh(false);
        Assert.assertEquals(
                1,
                Objects.requireNonNull(
                                new File(currentBucketPath)
                                        .listFiles(
                                                pathname -> pathname.getName().endsWith(".snappy")))
                        .length);
        Assert.assertEquals(recordsOffset, defaultHDFSFunction.committableOffset());

        final RecordsOffset recordsOffset2 = new RecordsOffset(2, 0);
        defaultHDFSFunction.process(
                recordsOffset2, Arrays.asList("aa".getBytes(), "bb".getBytes()).listIterator());
        currentBucketPath = currentBucketPath(defaultHDFSFunction);
        defaultHDFSFunction.idleTrigger();
        defaultHDFSFunction.idleTrigger();
        Assert.assertEquals(
                1,
                Objects.requireNonNull(
                                new File(currentBucketPath)
                                        .listFiles(
                                                pathname -> pathname.getName().endsWith(".snappy")))
                        .length);

        // because clock not set new current time over 1 min, committable offset not changed
        Assert.assertEquals(recordsOffset, defaultHDFSFunction.committableOffset());
    }

    /**
     * get current bucket path.
     *
     * @param function function
     * @return path
     * @throws IOException IOException
     */
    private String currentBucketPath(DefaultHDFSFunction function) throws IOException {
        return DIR.getCanonicalFile().getPath() + "/" + function.getBucket(null);
    }
}
