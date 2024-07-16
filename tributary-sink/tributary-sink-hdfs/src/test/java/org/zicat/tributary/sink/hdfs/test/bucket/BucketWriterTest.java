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

package org.zicat.tributary.sink.hdfs.test.bucket;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zicat.tributary.common.MemorySize;
import org.zicat.tributary.common.records.Records;
import org.zicat.tributary.common.records.RecordsUtils;
import org.zicat.tributary.sink.function.ContextBuilder;
import org.zicat.tributary.sink.hdfs.HDFSRecordsWriter;
import org.zicat.tributary.sink.hdfs.bucket.BucketWriter;
import org.zicat.tributary.sink.hdfs.test.MockFileSystem;
import org.zicat.tributary.sink.hdfs.test.MockHDFSRecordsWriter;
import org.zicat.tributary.sink.hdfs.test.MockHDFSRecordsWriterFactory;
import org.zicat.tributary.sink.hdfs.test.MockParquetHDFSRecordsWriter;

import java.io.IOException;
import java.util.Calendar;
import java.util.concurrent.atomic.AtomicInteger;

import static org.zicat.tributary.sink.hdfs.HDFSSinkOptions.*;
import static org.zicat.tributary.sink.hdfs.ParquetHDFSRecordsWriterFactory.OPTION_OUTPUT_COMPRESSION_CODEC;

/** BucketWriterTest. */
public class BucketWriterTest {
    private static final Logger logger = LoggerFactory.getLogger(BucketWriterTest.class);

    @Test
    public void testSizeRoller() throws IOException {
        int maxBytes = 300;
        final MockHDFSRecordsWriter hdfsWriter = new MockHDFSRecordsWriter();
        final BucketWriter bucketWriter =
                new BucketWriterBuilder(hdfsWriter).setRollSize(maxBytes).build();

        for (int i = 0; i < 1000; i++) {
            bucketWriter.append(RecordsUtils.createStringRecords("t1", "foo"));
        }

        logger.info("Number of events written: {}", hdfsWriter.getEventsWritten());
        logger.info("Number of bytes written: {}", hdfsWriter.getBytesWritten());
        logger.info("Number of files opened: {}", hdfsWriter.getFilesOpened());

        Assert.assertEquals("events written", 1000, hdfsWriter.getEventsWritten());
        Assert.assertEquals("bytes written", 3000, hdfsWriter.getBytesWritten());
        Assert.assertEquals("files opened", 10, hdfsWriter.getFilesOpened());
    }

    @Test
    public void testFileSuffixNotGiven() throws IOException {
        // Need to override system time use for test, so we know what to expect

        MockHDFSRecordsWriter hdfsWriter = new MockHDFSRecordsWriter();
        BucketWriter bucketWriter = new BucketWriterBuilder(hdfsWriter).setCodeC("snappy").build();

        bucketWriter.append(RecordsUtils.createStringRecords("t1", "foo"));

        Assert.assertTrue(hdfsWriter.getOpenedFilePath().endsWith("1.snappy.mock.tmp"));
    }

    @Test
    public void testInUseSuffix() throws IOException {
        final String suffix = "WELCOME_TO_THE_HELLMOUNTH";

        final MockHDFSRecordsWriter hdfsWriter = new MockHDFSRecordsWriter();
        final BucketWriter bucketWriter =
                new BucketWriterBuilder(hdfsWriter).setInUseSuffix(suffix).build();

        bucketWriter.append(RecordsUtils.createStringRecords("t1", "foo"));
        Assert.assertTrue(
                "Incorrect in use suffix", hdfsWriter.getOpenedFilePath().contains(suffix));
    }

    @Test
    public void testSequenceFileRenameRetries() throws Exception {
        sequenceFileRenameRetryCoreTest(1);
        sequenceFileRenameRetryCoreTest(5);
        sequenceFileRenameRetryCoreTest(2);
    }

    @Test
    public void testRotateFail() throws IOException {
        final String bucketPath = "/tmp/bucket_writer";
        final String fileName =
                "zicat-test."
                        + Calendar.getInstance().getTimeInMillis()
                        + "."
                        + Thread.currentThread().getId();

        final Configuration conf = new Configuration();
        final FileSystem fs = FileSystem.get(conf);
        final Path dirPath = new Path(bucketPath);
        fs.delete(dirPath, true);
        try {
            fs.mkdirs(dirPath);
            final String codec = "snappy";
            final MockFileSystem mockFs = new MockFileSystem(fs, 2, true);
            final MockParquetHDFSRecordsWriter writer =
                    new MockParquetHDFSRecordsWriter(mockFs, codec);
            final AtomicInteger renameCount = new AtomicInteger();
            final BucketWriter bucketWriter =
                    new BucketWriterBuilder(writer)
                            .setBucketPath(bucketPath)
                            .setFileName(fileName)
                            .setCodeC(codec)
                            .setRollSize(1)
                            .setMaxRetries(1)
                            .setWriter(writer)
                            .setFileSystem(mockFs)
                            .renameCount(renameCount)
                            .build();

            // At this point, we checked if isFileClosed is available in
            // this JVM, so lets make it check again.
            final Records records = RecordsUtils.createStringRecords("t1", "test");
            bucketWriter.append(records);
            for (int i = 0; i < 2; i++) {
                try {
                    bucketWriter.append(records);
                } catch (Exception e) {
                    Assert.assertTrue(true);
                }
            }
            bucketWriter.append(records);
        } finally {
            fs.delete(dirPath, true);
        }
    }

    /**
     * sequenceFileRenameRetryCoreTest.
     *
     * @param numberOfRetriesRequired numberOfRetriesRequired
     * @throws Exception Exception
     */
    public void sequenceFileRenameRetryCoreTest(int numberOfRetriesRequired) throws Exception {

        final String bucketPath = "/tmp/bucket_writer";
        final String fileName =
                "zicat-test."
                        + Calendar.getInstance().getTimeInMillis()
                        + "."
                        + Thread.currentThread().getId();

        final Configuration conf = new Configuration();
        final FileSystem fs = FileSystem.get(conf);
        final Path dirPath = new Path(bucketPath);
        final String codec = "snappy";
        fs.delete(dirPath, true);
        fs.mkdirs(dirPath);
        final MockFileSystem mockFs = new MockFileSystem(fs, numberOfRetriesRequired, true);
        final MockParquetHDFSRecordsWriter writer = new MockParquetHDFSRecordsWriter(mockFs, codec);
        final AtomicInteger renameCount = new AtomicInteger();

        final BucketWriter bucketWriter =
                new BucketWriterBuilder(writer)
                        .setBucketPath(bucketPath)
                        .setFileName(fileName)
                        .setCodeC(codec)
                        .setRollSize(1)
                        .setMaxRetries(numberOfRetriesRequired)
                        .setWriter(writer)
                        .setFileSystem(mockFs)
                        .renameCount(renameCount)
                        .build();

        // At this point, we checked if isFileClosed is available in
        // this JVM, so lets make it check again.
        bucketWriter.append(RecordsUtils.createStringRecords("t1", "test"));
        // This is what triggers the close, so a 2nd append is required :/
        bucketWriter.append(RecordsUtils.createStringRecords("t1", "test"));

        Assert.assertEquals(
                "Expected " + numberOfRetriesRequired + " " + "but got " + renameCount.get(),
                renameCount.get(),
                numberOfRetriesRequired);
    }

    @Test
    public void testRotateBucketOnIOException() throws IOException {
        final MockHDFSRecordsWriter hdfsWriter = Mockito.spy(new MockHDFSRecordsWriter());
        // Cause a roll after every successful append().
        final int rollSize = 1;
        final BucketWriter bucketWriter =
                new BucketWriterBuilder(hdfsWriter).setRollSize(rollSize).build();

        Records records = RecordsUtils.createStringRecords("t1", "foo");
        // Write one event successfully.
        bucketWriter.append(records);

        // Fail the next write.
        final IOException expectedIOException = new IOException("Test injected IOException");
        Mockito.doThrow(expectedIOException).when(hdfsWriter).append(records);

        try {
            bucketWriter.append(records);
            Assert.fail("append wait fail");
        } catch (IOException ioException) {
            Assert.assertEquals(expectedIOException, ioException);
            bucketWriter.close();
            bucketWriter.close();
        }
        Assert.assertEquals("events written", 1, hdfsWriter.getEventsWritten());
        Assert.assertEquals("2 files should be closed", 2, hdfsWriter.getFilesClosed());
    }

    /** BucketWriterBuilder. */
    private static class BucketWriterBuilder {

        private String bucketPath = "/tmp";
        private String fileName = "file";
        private long rollSize = 0;
        private String codeC = "snappy";
        private HDFSRecordsWriter writer;
        private FileSystem fileSystem;

        private int maxRetries = 3;
        private String inUseSuffix = BucketWriter.IN_USE_SUFFIX;
        private AtomicInteger renameCount = new AtomicInteger();

        public BucketWriterBuilder setMaxRetries(int maxRetries) {
            this.maxRetries = maxRetries;
            return this;
        }

        public BucketWriterBuilder(HDFSRecordsWriter writer) {
            this.writer = writer;
        }

        public BucketWriterBuilder setBucketPath(String bucketPath) {
            this.bucketPath = bucketPath;
            return this;
        }

        public BucketWriterBuilder renameCount(AtomicInteger renameCount) {
            this.renameCount = renameCount;
            return this;
        }

        public BucketWriterBuilder setFileSystem(FileSystem fileSystem) {
            this.fileSystem = fileSystem;
            return this;
        }

        public BucketWriterBuilder setFileName(String fileName) {
            this.fileName = fileName;
            return this;
        }

        public BucketWriterBuilder setInUseSuffix(String inUseSuffix) {
            this.inUseSuffix = inUseSuffix;
            return this;
        }

        public BucketWriterBuilder setCodeC(String codeC) {
            this.codeC = codeC;
            return this;
        }

        public BucketWriterBuilder setWriter(HDFSRecordsWriter writer) {
            this.writer = writer;
            return this;
        }

        public BucketWriterBuilder setRollSize(long rollSize) {
            this.rollSize = rollSize;
            return this;
        }

        public BucketWriter build() {
            if (writer == null) {
                writer = new MockHDFSRecordsWriter();
            }
            ContextBuilder builder = new ContextBuilder();
            builder.addCustomProperty(OPTION_ROLL_SIZE.key(), new MemorySize(rollSize));
            builder.addCustomProperty(OPTION_MAX_RETRIES.key(), maxRetries);
            builder.addCustomProperty(
                    OPTION_WRITER_IDENTITY.key(), MockHDFSRecordsWriterFactory.ID);
            builder.addCustomProperty(MockHDFSRecordsWriterFactory.OPTION_WRITER.key(), writer);
            builder.addCustomProperty(OPTION_OUTPUT_COMPRESSION_CODEC.key(), codeC);

            return new BucketWriter(builder.build(), bucketPath, fileName) {
                @Override
                protected FileSystem getFileSystem(Path path, Configuration config)
                        throws IOException {
                    return fileSystem == null ? super.getFileSystem(path, config) : fileSystem;
                }

                @Override
                protected void renameBucket(
                        String bucketPath, String targetPath, final FileSystem fs)
                        throws IOException {
                    renameCount.incrementAndGet();
                    super.renameBucket(bucketPath, targetPath, fs);
                }

                @Override
                protected String inUseSuffix() {
                    return inUseSuffix;
                }

                @Override
                protected long sleepOnFail() {
                    return 0;
                }
            };
        }
    }
}
