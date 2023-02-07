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

package org.zicat.tributary.channel.file;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zicat.tributary.channel.*;
import org.zicat.tributary.common.TributaryRuntimeException;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.zicat.tributary.channel.file.FileSegmentUtil.getIdByName;
import static org.zicat.tributary.channel.file.FileSegmentUtil.isLogSegment;
import static org.zicat.tributary.common.Functions.loopCloseableFunction;

/**
 * OnePartitionFileChannel implements {@link Channel} to Storage records and {@link RecordsOffset}
 * in local file system.
 *
 * <p>All public methods in FileChannel are @ThreadSafe.
 *
 * <p>FileChannel ignore partition params and append/pull all records in the {@link
 * OnePartitionFileChannel#dir}. {@link FileChannel} support multi partitions operations.
 *
 * <p>Data files {@link FileSegment} name start with {@link OnePartitionFileChannel#topic}
 *
 * <p>Only one {@link FileSegment} is writeable and support multi threads write it, multi threads
 * can read writable segment or other segments tagged as finished(not writable).
 *
 * <p>FileChannel support commit RecordsOffset by {@link OnePartitionFileChannel#groupManager} and
 * support clean up expired segments(all group ids has commit the offset over this segments) async}
 */
public class OnePartitionFileChannel extends OnePartitionAbstractChannel<FileSegment> {

    private static final Logger LOG = LoggerFactory.getLogger(OnePartitionFileChannel.class);
    private static final String FLUSH_THREAD_NAME = "segment_flush_thread";
    protected final File dir;
    private long flushPeriodMill;
    private Thread flushSegmentThread;
    private final long flushPageCacheSize;

    protected OnePartitionFileChannel(
            String topic,
            OnePartitionGroupManager groupManager,
            Integer blockSize,
            Long segmentSize,
            CompressionType compressionType,
            boolean flushForce,
            long flushPeriod,
            TimeUnit flushUnit,
            long flushPageCacheSize,
            File dir) {
        super(topic, groupManager, blockSize, segmentSize, compressionType, flushForce);
        this.dir = dir;
        this.flushPageCacheSize = flushPageCacheSize;
        loadSegments();
        initFlushSegmentThread(flushPeriod, flushUnit);
    }

    @Override
    protected FileSegment createSegment(long id) {
        return new FileSegmentBuilder()
                .fileId(id)
                .dir(dir)
                .segmentSize(segmentSize)
                .filePrefix(topic)
                .compressionType(compressionType)
                .build(blockWriter);
    }

    @Override
    protected void appendSuccessCallback(FileSegment segment) throws IOException {
        if (segment.cacheUsed() >= flushPageCacheSize) {
            segment.flush(false);
        }
    }

    @Override
    protected void closeCallback() {
        if (flushSegmentThread != null) {
            flushSegmentThread.interrupt();
        }
    }

    @Override
    protected void recycleSegment(FileSegment segment) {
        if (segment.recycle()) {
            LOG.info("expired file " + segment.filePath() + " deleted success");
        } else {
            LOG.info("expired file " + segment.filePath() + " deleted fail");
        }
    }

    /** load segments. */
    private void loadSegments() {

        final File[] files = dir.listFiles(file -> isLogSegment(topic, file.getName()));
        if (files == null || files.length == 0) {
            // create new log segment if dir is empty, start id = 0
            setLastSegment(createSegment(0));
            return;
        }

        // load segment and find max.
        FileSegment maxSegment = null;
        for (File file : files) {
            final String fileName = file.getName();
            final long id = getIdByName(topic, fileName);
            final FileSegment segment = createSegment(id);
            addSegment(segment, false);
            maxSegment = SegmentUtil.max(segment, maxSegment);
        }
        for (Map.Entry<Long, FileSegment> entry : segmentCache.entrySet()) {
            try {
                entry.getValue().finish();
            } catch (IOException e) {
                throw new TributaryRuntimeException("finish history segment fail", e);
            }
        }
        setLastSegment(createSegment(maxSegment.segmentId() + 1));
        cleanUp();
    }

    /** flush with force. */
    public boolean periodFlush() {
        try {
            flush(flushForce);
            return true;
        } catch (Throwable e) {
            LOG.warn("period flush error", e);
            return false;
        }
    }

    /**
     * init flush segment thread if flushPeriod > 0.
     *
     * @param flushPeriod flushPeriod
     * @param flushUnit flushUnit
     */
    private void initFlushSegmentThread(long flushPeriod, TimeUnit flushUnit) {
        if (flushPeriod <= 0) {
            return;
        }
        flushPeriodMill = flushUnit.toMillis(flushPeriod);
        flushSegmentThread =
                new Thread(
                        () -> loopCloseableFunction(t -> periodFlush(), flushPeriodMill, closed),
                        FLUSH_THREAD_NAME);
        flushSegmentThread.start();
    }
}
