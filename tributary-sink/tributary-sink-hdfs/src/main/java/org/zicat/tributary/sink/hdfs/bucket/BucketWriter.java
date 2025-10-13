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

package org.zicat.tributary.sink.hdfs.bucket;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zicat.tributary.common.Functions.Runnable;
import org.zicat.tributary.common.SpiFactory;
import org.zicat.tributary.common.records.Records;
import org.zicat.tributary.sink.authentication.PrivilegedExecutor;
import org.zicat.tributary.sink.function.Context;
import org.zicat.tributary.sink.hdfs.HDFSRecordsWriter;
import org.zicat.tributary.sink.hdfs.HDFSRecordsWriterFactory;
import org.zicat.tributary.sink.hdfs.RecordsWriter;

import java.io.IOException;
import java.nio.channels.ClosedChannelException;
import java.security.PrivilegedExceptionAction;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.zicat.tributary.common.Functions.runWithRetry;
import static org.zicat.tributary.sink.authentication.TributaryAuthenticationUtil.getAuthenticator;
import static org.zicat.tributary.sink.hdfs.HDFSSinkOptions.*;
import static org.zicat.tributary.common.Exceptions.castAsIOException;

/** This class does file rolling and handles file formats and serialization. */
public class BucketWriter extends BucketMeta implements RecordsWriter {

    private static final Logger LOG = LoggerFactory.getLogger(BucketWriter.class);

    private final HDFSRecordsWriter writer;
    private final PrivilegedExecutor proxyUser;
    private final String fileSuffixName;
    private final AtomicLong fileExtensionCounter;
    private final Configuration config = new Configuration();

    private long processSize;
    private FileSystem fileSystem;
    private String tmpWritePath;
    protected String fullFileName;
    protected String targetPath;
    private boolean open = false;

    public BucketWriter(Context context, String bucketPath, String fileName) {
        super(
                bucketPath,
                fileName,
                context.get(OPTION_ROLL_SIZE).getBytes(),
                context.get(OPTION_MAX_RETRIES),
                context.get(OPTION_RETRY_INTERVAL).toMillis());
        final String writerId = context.get(OPTION_WRITER_IDENTITY);
        final HDFSRecordsWriterFactory factory =
                SpiFactory.findFactory(writerId, HDFSRecordsWriterFactory.class);
        this.writer = factory.create(context);
        this.fileSuffixName = factory.fileExtension(context);
        this.proxyUser =
                getAuthenticator(context.get(OPTION_PRINCIPLE), context.get(OPTION_KEYTAB));
        this.fileExtensionCounter = new AtomicLong(0L);
        // set auto close as false, the close hook method will close file system cause SinkFunction
        config.setBoolean(CommonConfigurationKeysPublic.FS_AUTOMATIC_CLOSE_KEY, false);
    }

    /**
     * @throws IOException IOException
     */
    public void open() throws IOException {
        synchronized (BucketWriter.class) {
            try {
                callWithPrivileged(
                        () -> {
                            fullFileName = createNewFullFileName();
                            targetPath = bucketPath + "/" + fullFileName;
                            tmpWritePath = targetPath + inUseSuffix();
                            LOG.info("Creating {}", tmpWritePath);
                            final Path path = new Path(tmpWritePath);
                            fileSystem = getFileSystem(path, config);
                            writer.open(fileSystem, path);
                        });
            } catch (Throwable ex) {
                throw castAsIOException(ex);
            }
        }
        processSize = 0;
        open = true;
    }

    /**
     * get file system by path and config.
     *
     * @param path path
     * @param config config
     * @return FileSystem
     * @throws IOException IOException
     */
    protected FileSystem getFileSystem(Path path, Configuration config) throws IOException {
        return path.getFileSystem(config);
    }

    /**
     * close file and rename it.
     *
     * @throws IOException IOException
     */
    public void close() throws IOException {
        if (!open) {
            return;
        }
        closeWriter();
        if (tmpWritePath != null && targetPath != null && fileSystem != null) {
            final Runnable renameBucket = () -> renameBucket(tmpWritePath, targetPath, fileSystem);
            final Throwable exception = runWithRetry(renameBucket, maxRetries, retryIntervalMs());
            if (exception != null) {
                throw castAsIOException(exception);
            }
            tmpWritePath = null;
            fullFileName = null;
            targetPath = null;
            fileSystem = null;
        }
        open = false;
    }

    /**
     * rename bucket filename from .tmp to targetPath.
     *
     * @param bucketPath bucketPath
     * @param targetPath targetPath
     * @param fs fs
     * @throws IOException IOException
     */
    protected void renameBucket(String bucketPath, String targetPath, final FileSystem fs)
            throws IOException {

        if (bucketPath.equals(targetPath)) {
            return;
        }

        final Path srcPath = new Path(bucketPath);
        final Path dstPath = new Path(targetPath);
        callWithPrivileged(
                () -> {
                    if (fs.exists(srcPath)) {
                        LOG.info("Renaming {} to {}", srcPath, dstPath);
                        fs.rename(srcPath, dstPath);
                    }
                });
    }

    /** close writer with recover lease. */
    private void closeWriter() throws IOException {
        try {
            LOG.info("Closing {}", tmpWritePath);
            callWithPrivileged(writer::close);
        } catch (IOException e) {
            if (e instanceof ClosedChannelException) {
                LOG.info("{} already closed", tmpWritePath);
                return;
            }
            LOG.warn("Closing file: " + tmpWritePath + " failed. Will retry recover lease", e);
            if (fileSystem instanceof DistributedFileSystem && tmpWritePath != null) {
                ((DistributedFileSystem) fileSystem).recoverLease(new Path(tmpWritePath));
            }
        }
    }

    /**
     * call with privileged.
     *
     * @param runnable runnable
     * @throws IOException IOException
     */
    private void callWithPrivileged(final Runnable runnable) throws IOException {
        try {
            proxyUser.execute(
                    (PrivilegedExceptionAction<Object>)
                            () -> {
                                runnable.run();
                                return null;
                            });
        } catch (Throwable e) {
            throw castAsIOException(e);
        }
    }

    @Override
    public int append(Records records) throws IOException {

        if (!open) {
            open();
        }
        if (shouldRotate()) {
            close();
            open();
        }

        final AtomicInteger total = new AtomicInteger();
        final Throwable e =
                runWithRetry(
                        () -> callWithPrivileged(() -> total.set(writer.append(records))),
                        maxRetries,
                        retryIntervalMs());
        if (e != null) {
            throw castAsIOException(e);
        }
        processSize += total.get();
        return total.get();
    }

    /** check if time to rotate the file. */
    private boolean shouldRotate() {
        if ((rollSize > 0) && (rollSize <= processSize)) {
            LOG.debug("rolling: rollSize: {}, bytes: {}", rollSize, processSize);
            return true;
        }
        return false;
    }

    /**
     * create new full file name by fileName & file extension counter & fileSuffixName.
     *
     * @return string
     */
    protected String createNewFullFileName() {
        return fileName + "." + fileExtensionCounter.incrementAndGet() + fileSuffixName;
    }
}
