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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zicat.tributary.sink.authentication.PrivilegedExecutor;
import org.zicat.tributary.sink.authentication.TributaryAuthenticationUtil;
import org.zicat.tributary.sink.hdfs.BucketWriter;
import org.zicat.tributary.sink.hdfs.HDFSWriter;
import org.zicat.tributary.sink.hdfs.HDFSWriterFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Calendar;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;

/** BucketWriterTest. */
public class BucketWriterTest {
    private static final Logger logger = LoggerFactory.getLogger(BucketWriterTest.class);

    private static PrivilegedExecutor proxy;

    @BeforeClass
    public static void setup() {
        proxy = TributaryAuthenticationUtil.getAuthenticator(null, null).proxyAs(null);
    }

    @Test
    public void testSizeRoller() throws IOException {
        int maxBytes = 300;
        final MockHDFSWriter hdfsWriter = new MockHDFSWriter();
        final BucketWriter<Void> bucketWriter =
                new BucketWriterBuilder(hdfsWriter).setRollSize(maxBytes).build();

        final byte[] e = "foo".getBytes(StandardCharsets.UTF_8);
        for (int i = 0; i < 1000; i++) {
            bucketWriter.append(e);
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
        // Need to override system time use for test so we know what to expect

        MockHDFSWriter hdfsWriter = new MockHDFSWriter();
        BucketWriter<Void> bucketWriter =
                new BucketWriterBuilder(hdfsWriter).setCodeC(new GzipCodec()).build();

        final byte[] e = "foo".getBytes(StandardCharsets.UTF_8);
        bucketWriter.append(e);

        Assert.assertTrue("Incorrect suffix", hdfsWriter.getOpenedFilePath().endsWith("1.gz.tmp"));
    }

    @Test
    public void testInUseSuffix() throws IOException {
        final String suffix = "WELCOME_TO_THE_HELLMOUNTH";

        final MockHDFSWriter hdfsWriter = new MockHDFSWriter();
        final BucketWriter<Void> bucketWriter =
                new BucketWriterBuilder(hdfsWriter).setInUseSuffix(suffix).build();

        final byte[] e = "foo".getBytes(StandardCharsets.UTF_8);
        bucketWriter.append(e);
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
        fs.mkdirs(dirPath);
        final SnappyCodec codec = new SnappyCodec();
        codec.setConf(conf);
        final MockFileSystem mockFs = new MockFileSystem(fs, 2, true);
        final MockLengthBodyHDFSWriter writer = new MockLengthBodyHDFSWriter(mockFs, codec);
        final AtomicInteger renameCount = new AtomicInteger();
        final BucketWriter<Void> bucketWriter =
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
        final byte[] event = "test".getBytes(StandardCharsets.UTF_8);
        bucketWriter.append(event);
        for (int i = 0; i < 2; i++) {
            try {
                bucketWriter.append(event);
            } catch (Exception e) {
                Assert.assertTrue(true);
            }
        }
        bucketWriter.append(event);
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
        final SnappyCodec codec = new SnappyCodec();
        codec.setConf(conf);
        fs.delete(dirPath, true);
        fs.mkdirs(dirPath);
        final MockFileSystem mockFs = new MockFileSystem(fs, numberOfRetriesRequired, true);
        final MockLengthBodyHDFSWriter writer = new MockLengthBodyHDFSWriter(mockFs, codec);
        final AtomicInteger renameCount = new AtomicInteger();

        final BucketWriter<Void> bucketWriter =
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
        final byte[] event = "test".getBytes(StandardCharsets.UTF_8);
        bucketWriter.append(event);
        // This is what triggers the close, so a 2nd append is required :/
        bucketWriter.append(event);

        Assert.assertEquals(
                "Expected " + numberOfRetriesRequired + " " + "but got " + renameCount.get(),
                renameCount.get(),
                numberOfRetriesRequired);
    }

    @Test
    public void testRotateBucketOnIOException() throws IOException {
        final MockHDFSWriter hdfsWriter = Mockito.spy(new MockHDFSWriter());
        final PrivilegedExecutor ugiProxy =
                TributaryAuthenticationUtil.getAuthenticator(null, null).proxyAs("alice");

        // Cause a roll after every successful append().
        final int rollSize = 1;
        final BucketWriter<Void> bucketWriter =
                new BucketWriterBuilder(hdfsWriter)
                        .setProxyUser(ugiProxy)
                        .setRollSize(rollSize)
                        .build();
        final byte[] e = "foo".getBytes(StandardCharsets.UTF_8);

        // Write one event successfully.
        bucketWriter.append(e, 0, e.length);

        // Fail the next write.
        final IOException expectedIOException = new IOException("Test injected IOException");
        Mockito.doThrow(expectedIOException).when(hdfsWriter).append(e, 0, e.length);

        try {
            bucketWriter.append(e, 0, e.length);
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
        private CompressionCodec codeC = null;
        private HDFSWriter writer;
        private PrivilegedExecutor proxyUser = BucketWriterTest.proxy;
        private FileSystem fileSystem;

        private int maxRetries = 3;
        private String inUseSuffix = BucketWriter.IN_USE_SUFFIX;
        private AtomicInteger renameCount = new AtomicInteger();

        public BucketWriterBuilder setMaxRetries(int maxRetries) {
            this.maxRetries = maxRetries;
            return this;
        }

        public BucketWriterBuilder(HDFSWriter writer) {
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

        public BucketWriterBuilder setCodeC(CompressionCodec codeC) {
            this.codeC = codeC;
            return this;
        }

        @SuppressWarnings("unused")
        public BucketWriterBuilder setProxyUser(PrivilegedExecutor proxyUser) {
            this.proxyUser = proxyUser;
            return this;
        }

        @SuppressWarnings("unused")
        public BucketWriterBuilder setCallTimeoutPool(ExecutorService callTimeoutPool) {
            return this;
        }

        public BucketWriterBuilder setWriter(HDFSWriter writer) {
            this.writer = writer;
            return this;
        }

        public BucketWriterBuilder setRollSize(long rollSize) {
            this.rollSize = rollSize;
            return this;
        }

        public BucketWriter<Void> build() {
            if (writer == null) {
                writer = new MockHDFSWriter();
            }

            if (codeC == null) {
                SnappyCodec codeC = new SnappyCodec();
                codeC.setConf(new Configuration());
                this.codeC = codeC;
            }
            final HDFSWriterFactory factory =
                    new HDFSWriterFactory() {
                        @Override
                        public String fileExtension() {
                            return codeC.getDefaultExtension();
                        }

                        @Override
                        public HDFSWriter create() {
                            return writer;
                        }
                    };
            return new BucketWriter<Void>(
                    bucketPath, fileName, factory, proxyUser, rollSize, maxRetries, null) {
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
