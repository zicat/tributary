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
import org.zicat.tributary.common.util.IOUtils;
import org.zicat.tributary.common.test.util.FileUtils;
import org.zicat.tributary.sink.function.ContextBuilder;
import org.zicat.tributary.sink.hdfs.DefaultRecordsWriterManager;
import org.zicat.tributary.sink.hdfs.RecordsWriter;
import org.zicat.tributary.sink.hdfs.RecordsWriterManager;

import java.io.File;
import java.util.Arrays;
import java.util.List;

import static org.zicat.tributary.common.records.RecordsUtils.createStringRecords;
import static org.zicat.tributary.sink.hdfs.HDFSSinkOptions.OPTION_SINK_PATH;
import static org.zicat.tributary.sink.hdfs.HDFSSinkOptions.OPTION_WRITER_IDENTITY;

/** AbstractHDFSFunctionTest. */
public class RecordsWriterManagerTest {

    final File dir = FileUtils.createTmpDir("abstract_hdfs_function_test");
    final String bucketPath = dir.getPath();

    @Before
    @After
    public void cleanup() {
        IOUtils.deleteDir(dir);
    }

    @Test
    public void test() throws Exception {
        final String bucket = "bucket_1";
        final String bucket2 = "bucket_2";
        final MockHDFSRecordsWriter mockWriter = new MockHDFSRecordsWriter();
        final ContextBuilder builder = new ContextBuilder();
        builder.addCustomProperty(OPTION_SINK_PATH, bucketPath);
        builder.addCustomProperty(OPTION_WRITER_IDENTITY, MockHDFSRecordsWriterFactory.ID);
        builder.addCustomProperty(MockHDFSRecordsWriterFactory.OPTION_WRITER, mockWriter);
        builder.topic("t1").groupId("g1").id("1");
        try (final RecordsWriterManager recordsWriterManager = new DefaultRecordsWriterManager()) {
            recordsWriterManager.open(builder.build());
            final List<String> testData = Arrays.asList("1", "2", "3");
            final RecordsWriter writer = recordsWriterManager.getOrCreateRecordsWriter(bucket);
            writer.append(createStringRecords("t1", testData));
            writer.append(createStringRecords("t1", testData));
            Assert.assertEquals(1, recordsWriterManager.bucketsCount());
            Assert.assertEquals(1, mockWriter.getFilesOpened());
            final RecordsWriter writer2 = recordsWriterManager.getOrCreateRecordsWriter(bucket2);
            writer2.append(createStringRecords("t1", testData));
            Assert.assertEquals(2, recordsWriterManager.bucketsCount());
            Assert.assertEquals(2, mockWriter.getFilesOpened());
            Assert.assertTrue(recordsWriterManager.contains(bucket));
            Assert.assertTrue(recordsWriterManager.contains(bucket2));
            Assert.assertFalse(recordsWriterManager.contains("bucket_3"));
            recordsWriterManager.closeAllBuckets();
            Assert.assertEquals(0, recordsWriterManager.bucketsCount());
            Assert.assertEquals(2, mockWriter.getFilesOpened());
            Assert.assertFalse(recordsWriterManager.contains(bucket));
            Assert.assertFalse(recordsWriterManager.contains(bucket2));
            Assert.assertFalse(recordsWriterManager.contains("bucket_3"));
        }
        Assert.assertEquals(9, mockWriter.getEventsWritten());
    }
}
