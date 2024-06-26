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

import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.zicat.tributary.channel.GroupOffset;
import org.zicat.tributary.common.records.DefaultRecord;
import org.zicat.tributary.common.records.DefaultRecords;
import org.zicat.tributary.common.records.Record;
import org.zicat.tributary.common.records.Records;
import org.zicat.tributary.common.test.FileUtils;
import org.zicat.tributary.sink.function.ContextBuilder;
import org.zicat.tributary.sink.hdfs.DefaultHDFSFunction;
import org.zicat.tributary.sink.test.function.MockClock;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;

import static org.zicat.tributary.common.BytesUtils.toBytes;
import static org.zicat.tributary.common.IOUtils.deleteDir;
import static org.zicat.tributary.common.IOUtils.makeDir;
import static org.zicat.tributary.common.records.RecordsUtils.HEAD_KEY_SENT_TS;
import static org.zicat.tributary.common.records.RecordsUtils.createStringRecords;
import static org.zicat.tributary.sink.hdfs.DefaultHDFSFunction.*;
import static org.zicat.tributary.sink.hdfs.ParquetHDFSWriter.*;

/** DefaultHDFSFunctionTest. */
public class DefaultHDFSFunctionTest {

    private static final File DIR = FileUtils.createTmpDir("default_hdfs_function_test");
    private static final String topic = "t1";
    private static final int partition = 1;

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

    @SuppressWarnings("unchecked")
    @Test
    public void test() throws Throwable {

        final Map<String, byte[]> recordHeader1 = new HashMap<>();
        recordHeader1.put("rhk1", "rhv1".getBytes());
        final Record record1 = new DefaultRecord(recordHeader1, "rk1".getBytes(), "rv1".getBytes());
        final Map<String, byte[]> recordHeader2 = new HashMap<>();
        recordHeader2.put("rhk2", "rhv2".getBytes());
        Record record2 = new DefaultRecord(recordHeader2, "rk2".getBytes(), "rv2".getBytes());

        final Map<String, byte[]> recordsHeader = new HashMap<>();
        recordHeader1.put("rshk1", "rshv1".getBytes());
        final Records records =
                new DefaultRecords(
                        topic, partition, recordsHeader, Arrays.asList(record1, record2));

        final DefaultHDFSFunction defaultHDFSFunction = new DefaultHDFSFunction();
        final MockClock mockClock = new MockClock();
        mockClock.setCurrentTimeMillis(System.currentTimeMillis());
        final String timeFormat = "yyyyMMdd_HHmm";
        final String timeZoneId = "GMT+8";
        final ContextBuilder builder =
                new ContextBuilder()
                        .id("id1")
                        .partitionId(0)
                        .startGroupOffset(new GroupOffset(0, 0, "g1"))
                        .topic(topic);
        builder.addCustomProperty(OPTION_SINK_PATH.key(), DIR.getCanonicalFile().getPath())
                .addCustomProperty(OPTION_IDLE_MILLIS.key(), 10000)
                .addCustomProperty(OPTION_BUCKET_DATE_FORMAT.key(), timeFormat)
                .addCustomProperty(OPTION_BUCKET_DATE_TIMEZONE.key(), timeZoneId)
                .addCustomProperty(OPTION_OUTPUT_COMPRESSION_CODEC.key(), "snappy")
                .addCustomProperty(OPTION_CLOCK.key(), mockClock);
        builder.addCustomProperty(OPTION_METRICS_HOST.key(), "localhost");

        defaultHDFSFunction.open(builder.build());

        final GroupOffset groupOffset = new GroupOffset(1, 0, "g1");
        defaultHDFSFunction.process(groupOffset, Collections.singletonList(records).iterator());
        String currentBucketPath = currentBucketPath(defaultHDFSFunction);

        Assert.assertTrue(
                currentBucketPath.contains(mockClock.currentTime(timeFormat, timeZoneId)));

        // refresh by time rolling
        mockClock.setCurrentTimeMillis(mockClock.currentTimeMillis() + 120 * 1000);
        defaultHDFSFunction.refresh(false);

        List<File> parquetFiles =
                Arrays.asList(
                        Objects.requireNonNull(
                                new File(currentBucketPath)
                                        .listFiles(
                                                pathname ->
                                                        pathname.getName()
                                                                .endsWith(".snappy.parquet"))));
        Assert.assertEquals(1, parquetFiles.size());
        Assert.assertEquals(groupOffset, defaultHDFSFunction.committableOffset());

        final File parquetFile = parquetFiles.get(0);
        Assert.assertTrue(parquetFile.getParent().endsWith("/" + topic));
        final Configuration conf = new Configuration();
        try (ParquetReader<GenericRecord> reader =
                AvroParquetReader.<GenericRecord>builder(
                                HadoopInputFile.fromPath(new Path(parquetFile.toURI()), conf))
                        .withConf(conf)
                        .build()) {

            GenericRecord record;
            Assert.assertNotNull((record = reader.read()));
            Assert.assertEquals(topic, record.get(FIELD_TOPIC).toString());
            Assert.assertEquals(partition, record.get(FIELD_PARTITION));
            Assert.assertArrayEquals(record1.key(), toBytes((ByteBuffer) record.get(FIELD_KEY)));
            Assert.assertArrayEquals(
                    record1.value(), toBytes((ByteBuffer) record.get(FIELD_VALUE)));
            Map<Utf8, ByteBuffer> headers = (Map<Utf8, ByteBuffer>) record.get(FIELD_HEADERS);
            Assert.assertEquals(
                    records.headers().size() + record1.headers().size() + 1, headers.size());
            Map<String, byte[]> comboHeaders = new HashMap<>(record1.headers());
            comboHeaders.putAll(records.headers());
            for (String key : comboHeaders.keySet()) {
                Assert.assertArrayEquals(
                        comboHeaders.get(key), toBytes(headers.get(new Utf8(key))));
            }
            Assert.assertTrue(headers.containsKey(new Utf8(HEAD_KEY_SENT_TS)));

            Assert.assertNotNull((record = reader.read()));
            Assert.assertEquals(topic, record.get(FIELD_TOPIC).toString());
            Assert.assertEquals(partition, record.get(FIELD_PARTITION));
            Assert.assertArrayEquals(record2.key(), toBytes((ByteBuffer) record.get(FIELD_KEY)));
            Assert.assertArrayEquals(
                    record2.value(), toBytes((ByteBuffer) record.get(FIELD_VALUE)));
        }

        final GroupOffset groupOffset2 = new GroupOffset(2, 0, "g1");
        defaultHDFSFunction.process(
                groupOffset2,
                Collections.singletonList(createStringRecords(topic, "aa", "bb")).iterator());
        currentBucketPath = currentBucketPath(defaultHDFSFunction);
        defaultHDFSFunction.idleTrigger();
        defaultHDFSFunction.idleTrigger();

        parquetFiles =
                Arrays.asList(
                        Objects.requireNonNull(
                                new File(currentBucketPath)
                                        .listFiles(
                                                pathname ->
                                                        pathname.getName()
                                                                .endsWith(".snappy.parquet"))));
        Assert.assertEquals(1, parquetFiles.size());

        // because clock not set new current time over 1 min, committable offset not changed
        Assert.assertEquals(groupOffset2, defaultHDFSFunction.committableOffset());
    }

    /**
     * get current bucket path.
     *
     * @param function function
     * @return path
     * @throws IOException IOException
     */
    private String currentBucketPath(DefaultHDFSFunction function) throws IOException {
        return DIR.getCanonicalFile().getPath() + "/" + function.getTimeBucket() + "/" + topic;
    }
}
