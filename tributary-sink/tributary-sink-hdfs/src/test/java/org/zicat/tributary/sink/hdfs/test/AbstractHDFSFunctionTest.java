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

package org.zicat.tributary.sink.hdfs.test;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.zicat.tributary.channel.GroupOffset;
import org.zicat.tributary.common.IOUtils;
import org.zicat.tributary.common.records.Records;
import org.zicat.tributary.common.test.FileUtils;
import org.zicat.tributary.sink.function.Context;
import org.zicat.tributary.sink.function.ContextBuilder;
import org.zicat.tributary.sink.hdfs.AbstractHDFSFunction;
import org.zicat.tributary.sink.hdfs.BucketWriter;
import org.zicat.tributary.sink.hdfs.HDFSWriter;
import org.zicat.tributary.sink.hdfs.HDFSWriterFactory;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import static org.zicat.tributary.common.records.RecordsUtils.createStringRecords;
import static org.zicat.tributary.sink.function.AbstractFunction.OPTION_METRICS_HOST;
import static org.zicat.tributary.sink.hdfs.AbstractHDFSFunction.OPTION_SINK_PATH;

/** AbstractHDFSFunctionTest. */
public class AbstractHDFSFunctionTest {

    final File dir = FileUtils.createTmpDir("abstract_hdfs_function_test");
    final String bucketPath = dir.getPath();

    @Before
    @After
    public void cleanup() {
        IOUtils.deleteDir(dir);
    }

    @Test
    public void testBucketClosed() throws Exception {
        final String bucket = "counter";
        final MockHDFSWriter mockWriter = new MockHDFSWriter("snappy");
        final AbstractHDFSFunction function =
                new AbstractHDFSFunction() {
                    @Override
                    public void process(GroupOffset groupOffset, Iterator<Records> iterator)
                            throws Exception {
                        while (iterator.hasNext()) {
                            appendData(bucket, iterator.next());
                        }
                        commit(groupOffset, null);
                        closeBucket(bucket);
                    }

                    @Override
                    protected String prefixFileNameInBucket() {
                        return "aa";
                    }

                    @Override
                    protected BucketWriter initializeBucketWriter(
                            String bucketPath, String realName) {
                        return new BucketWriter(
                                null,
                                bucketPath,
                                realName,
                                new HDFSWriterFactory() {
                                    @Override
                                    public HDFSWriter create(Context context) {
                                        return mockWriter;
                                    }

                                    @Override
                                    public String identity() {
                                        return "mock";
                                    }
                                },
                                privilegedExecutor,
                                rollSize,
                                maxRetry);
                    }
                };
        final ContextBuilder contextBuilder =
                ContextBuilder.newBuilder()
                        .partitionId(0)
                        .topic("t1")
                        .startGroupOffset(new GroupOffset(0, 0, "g1"));
        contextBuilder.addCustomProperty(OPTION_SINK_PATH.key(), bucketPath);
        final Context context = contextBuilder.build();
        function.open(context);
        final List<String> testData = Arrays.asList("1", "2", "3");
        function.process(
                new GroupOffset(1, 1, "g1"),
                Collections.singletonList(createStringRecords("t1", testData)).iterator());
        function.process(
                new GroupOffset(1, 1, "g1"),
                Collections.singletonList(createStringRecords("t1", testData)).iterator());
        function.close();

        Assert.assertEquals(6, mockWriter.getEventsWritten());
    }

    @Test
    public void testAppend() throws Exception {

        final String bucket = "counter";
        final MockHDFSWriter mockWriter = new MockHDFSWriter("snappy");
        final AbstractHDFSFunction function =
                new AbstractHDFSFunction() {
                    @Override
                    public void process(GroupOffset groupOffset, Iterator<Records> iterator)
                            throws IOException {
                        while (iterator.hasNext()) {
                            appendData(bucket, iterator.next());
                        }
                        commit(groupOffset, null);
                    }

                    @Override
                    protected String prefixFileNameInBucket() {
                        return "aa";
                    }

                    @Override
                    protected BucketWriter initializeBucketWriter(
                            String bucketPath, String realName) {
                        return new BucketWriter(
                                null,
                                bucketPath,
                                realName,
                                new HDFSWriterFactory() {
                                    @Override
                                    public HDFSWriter create(Context context) {
                                        return mockWriter;
                                    }

                                    @Override
                                    public String identity() {
                                        return "mock";
                                    }
                                },
                                privilegedExecutor,
                                rollSize,
                                maxRetry);
                    }
                };
        final ContextBuilder contextBuilder =
                ContextBuilder.newBuilder()
                        .partitionId(0)
                        .topic("t1")
                        .startGroupOffset(new GroupOffset(0, 0, "g1"));

        contextBuilder.addCustomProperty(OPTION_SINK_PATH.key(), bucketPath);
        contextBuilder.addCustomProperty(OPTION_METRICS_HOST.key(), "localhost");
        final Context context = contextBuilder.build();
        function.open(context);
        final List<String> testData = Arrays.asList("1", "2", "3");
        function.process(
                new GroupOffset(1, 1, "g1"),
                Collections.singletonList(createStringRecords("t1", testData)).iterator());
        function.process(
                new GroupOffset(1, 1, "g1"),
                Collections.singletonList(createStringRecords("t1", testData)).iterator());
        function.close();
        Assert.assertEquals(6, mockWriter.getEventsWritten());
    }
}
