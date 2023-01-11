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

package org.zicat.tributary.queue.file;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zicat.tributary.queue.*;
import org.zicat.tributary.queue.utils.IOUtils;
import org.zicat.tributary.queue.utils.TributaryQueueException;
import org.zicat.tributary.queue.utils.TributaryQueueRuntimeException;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import static org.zicat.tributary.queue.file.LogSegmentUtil.getIdByName;
import static org.zicat.tributary.queue.file.LogSegmentUtil.isLogSegment;
import static org.zicat.tributary.queue.utils.Functions.loopCloseableFunction;

/**
 * FileLogQueue implements {@link LogQueue} to Storage records and {@link RecordsOffset} in local
 * file system.
 *
 * <p>All public methods in FileLogQueue are @ThreadSafe.
 *
 * <p>FileLogQueue ignore partition params and append/pull all records in the {@link
 * FileLogQueue#dir}. {@link PartitionFileLogQueue} support multi partitions operations.
 *
 * <p>Data files {@link LogSegment} name start with {@link FileLogQueue#topic}
 *
 * <p>Only one {@link LogSegment} is writeable and support multi threads write it, multi threads can
 * read writable segment or other segments tagged as finished(not writable).
 *
 * <p>FileLogQueue support commit RecordsOffset by {@link FileLogQueue#groupManager} and support
 * clean up expired segments(all group ids has commit the offset over this segments) async, see
 * {@link FileLogQueue#cleanUpThread}
 */
public class FileLogQueue implements OnePartitionLogQueue, Closeable {

    private static final Logger LOG = LoggerFactory.getLogger(FileLogQueue.class);
    private final ReentrantLock lock = new ReentrantLock();
    private final Condition newSegmentCondition = lock.newCondition();
    protected final Map<Long, LogSegment> segmentCache = new ConcurrentHashMap<>();
    protected final File dir;
    protected final Long segmentSize;
    protected final CompressionType compressionType;
    protected final OnePartitionGroupManager groupManager;
    private volatile LogSegment lastSegment;
    private final AtomicBoolean closed = new AtomicBoolean();
    private long flushPeriodMill;
    private long cleanupMill;
    private Thread cleanUpThread;
    private Thread flushSegmentThread;
    private final String topic;
    private final long flushPageCacheSize;
    private final boolean flushForce;
    private final BufferWriter bufferWriter;
    private final AtomicLong writeBytes = new AtomicLong();
    private final AtomicLong readBytes = new AtomicLong();

    protected FileLogQueue(
            String topic,
            OnePartitionGroupManager groupManager,
            File dir,
            Integer blockSize,
            Long segmentSize,
            CompressionType compressionType,
            long cleanUpPeriod,
            TimeUnit cleanUpUnit,
            long flushPeriod,
            TimeUnit flushUnit,
            long flushPageCacheSize,
            boolean flushForce) {
        this.topic = topic;
        this.dir = dir;
        this.bufferWriter = new BufferWriter(blockSize);
        this.segmentSize = segmentSize;
        this.compressionType = compressionType;
        this.groupManager = groupManager;
        this.flushPageCacheSize = flushPageCacheSize;
        this.flushForce = flushForce;
        loadSegments();
        if (cleanUpPeriod > 0) {
            cleanupMill = cleanUpUnit.toMillis(cleanUpPeriod);
            cleanUpThread = new Thread(this::periodCleanUp, "cleanup_segment_thread");
            cleanUpThread.start();
        }
        if (flushPeriod > 0) {
            flushPeriodMill = flushUnit.toMillis(flushPeriod);
            flushSegmentThread = new Thread(this::periodForceSegment, "segment_flush_thread");
            flushSegmentThread.start();
        }
    }

    /** load segments. */
    private void loadSegments() {

        final File[] files = dir.listFiles(file -> isLogSegment(topic, file.getName()));
        if (files == null || files.length == 0) {
            // create new log segment if dir is empty, start id = 0
            this.lastSegment = createLogSegment(0);
            return;
        }

        // load segment and find max.
        LogSegment maxSegment = null;
        for (File file : files) {
            final String fileName = file.getName();
            final long id = getIdByName(topic, fileName);
            final LogSegment segment = createLogSegment(id);
            maxSegment = LogSegmentUtil.max(segment, maxSegment);
        }

        for (Map.Entry<Long, LogSegment> entry : segmentCache.entrySet()) {
            try {
                entry.getValue().finish();
            } catch (IOException e) {
                throw new TributaryQueueRuntimeException("finish history segment fail", e);
            }
        }
        this.lastSegment = createLogSegment(maxSegment.fileId() + 1);
        cleanUp();
    }

    /**
     * create segment by id.
     *
     * @param id id
     * @return LogSegment
     */
    private LogSegment createLogSegment(long id) {
        final LogSegment logSegment =
                new LogSegmentBuilder()
                        .fileId(id)
                        .dir(dir)
                        .segmentSize(segmentSize)
                        .filePrefix(topic)
                        .compressionType(compressionType)
                        .build(bufferWriter);
        segmentCache.put(id, logSegment);
        return logSegment;
    }

    /**
     * append record data without partition.
     *
     * @param record record
     * @throws IOException IOException
     */
    @Override
    public void append(byte[] record, int offset, int length) throws IOException {

        final LogSegment segment = this.lastSegment;
        if (segment.append(record, offset, length)) {
            checkSyncForce(segment);
        } else {
            final ReentrantLock lock = this.lock;
            lock.lock();
            try {
                segment.finish();
                final LogSegment retrySegment = this.lastSegment;
                if (!retrySegment.append(record, offset, length)) {
                    retrySegment.finish();
                    final LogSegment newSegment = createLogSegment(retrySegment.fileId() + 1L);
                    newSegment.append(record, offset, length);
                    writeBytes.addAndGet(lastSegment.readablePosition());
                    this.lastSegment = newSegment;
                    newSegmentCondition.signalAll();
                }
            } finally {
                lock.unlock();
            }
        }
    }

    /**
     * check whether sync flush.
     *
     * @param segment segment
     */
    private void checkSyncForce(LogSegment segment) throws IOException {
        if (segment.pageCacheUsed() >= flushPageCacheSize) {
            segment.flush(false);
        }
    }

    @Override
    public long lag(RecordsOffset recordsOffset) {
        return lastSegment.lag(recordsOffset);
    }

    @Override
    public long lastSegmentId() {
        return lastSegment.fileId();
    }

    @Override
    public long writeBytes() {
        return writeBytes.get() + lastSegment.readablePosition();
    }

    @Override
    public long readBytes() {
        return readBytes.get();
    }

    @Override
    public long pageCache() {
        return lastSegment.pageCacheUsed();
    }

    @Override
    public RecordsResultSet poll(RecordsOffset recordsOffset, long time, TimeUnit unit)
            throws IOException, InterruptedException {
        final RecordsResultSet resultSet = read(recordsOffset, time, unit);
        readBytes.addAndGet(resultSet.readBytes());
        return resultSet;
    }

    @Override
    public RecordsOffset getRecordsOffset(String groupId) {
        return groupManager.getRecordsOffset(groupId);
    }

    @Override
    public void commit(String groupId, RecordsOffset recordsOffset) throws IOException {
        groupManager.commit(groupId, recordsOffset);
    }

    @Override
    public RecordsOffset getMinRecordsOffset() {
        return groupManager.getMinRecordsOffset();
    }

    /**
     * read file log.
     *
     * @param originalRecordsOffset originalRecordsOffset
     * @param time time
     * @param unit unit
     * @return LogResult
     * @throws IOException IOException
     * @throws InterruptedException InterruptedException
     */
    private RecordsResultSet read(RecordsOffset originalRecordsOffset, long time, TimeUnit unit)
            throws IOException, InterruptedException {

        final LogSegment lastSegment = this.lastSegment;
        final BufferRecordsOffset recordsOffset = cast(originalRecordsOffset);
        final LogSegment segment =
                lastSegment.matchSegment(recordsOffset)
                        ? lastSegment
                        : segmentCache.get(recordsOffset.segmentId());
        if (segment == null) {
            throw new TributaryQueueException(
                    "segment not found segment id = " + recordsOffset.segmentId());
        }
        // try read it first
        final RecordsResultSet recordsResultSet =
                segment.readBlock(recordsOffset, time, unit).toResultSet();
        if (!segment.isFinish() || !recordsResultSet.isEmpty()) {
            return recordsResultSet;
        }

        // searchSegment is full
        final ReentrantLock lock = this.lock;
        lock.lock();
        try {
            // searchSegment point to currentSegment, await new segment
            if (segment == this.lastSegment) {
                if (time == 0) {
                    newSegmentCondition.await();
                } else if (!newSegmentCondition.await(time, unit)) {
                    return recordsOffset.reset().toResultSet();
                }
            }
        } finally {
            lock.unlock();
        }
        return read(recordsOffset.skipNextSegmentHead(), time, unit);
    }

    /**
     * read from last segment at beginning if null or over lastSegment.
     *
     * @param recordsOffset recordsOffset
     * @return BufferReader
     */
    protected BufferRecordsOffset cast(RecordsOffset recordsOffset) {
        if (recordsOffset == null) {
            return BufferRecordsOffset.cast(lastSegment.fileId());
        }
        final BufferRecordsOffset newRecordOffset = BufferRecordsOffset.cast(recordsOffset);
        return recordsOffset.segmentId() < 0 || recordsOffset.segmentId() > lastSegment.fileId()
                ? newRecordOffset.skip2TargetHead(lastSegment.fileId())
                : newRecordOffset;
    }

    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            IOUtils.closeQuietly(groupManager);
            segmentCache.forEach((k, v) -> IOUtils.closeQuietly(v));
            segmentCache.clear();
            if (cleanUpThread != null) {
                cleanUpThread.interrupt();
            }
            if (flushSegmentThread != null) {
                flushSegmentThread.interrupt();
            }
        }
    }

    @Override
    public void flush() throws IOException {
        lastSegment.flush();
    }

    /** force segment to dish. */
    protected void periodForceSegment() {
        loopCloseableFunction(
                t -> {
                    try {
                        flush(flushForce);
                        return true;
                    } catch (Exception e) {
                        LOG.warn("period flush error", e);
                        return false;
                    }
                },
                flushPeriodMill,
                closed);
    }

    /**
     * flush with force.
     *
     * @param force boolean force
     * @throws IOException IOException
     */
    public void flush(boolean force) throws IOException {
        lastSegment.flush(force);
    }

    /**
     * clean up old segment.
     *
     * @return boolean clean up
     */
    protected boolean cleanUp() {

        final RecordsOffset min = groupManager.getMinRecordsOffset();
        if (min == null || min.segmentId() < 0) {
            return false;
        }

        final List<LogSegment> expiredSegments = new ArrayList<>();
        for (Map.Entry<Long, LogSegment> entry : segmentCache.entrySet()) {
            final LogSegment segment = entry.getValue();
            if (segment.fileId() < min.segmentId() && segment.fileId() < lastSegment.fileId()) {
                expiredSegments.add(segment);
            }
        }

        for (LogSegment segment : expiredSegments) {
            segmentCache.remove(segment.fileId());
            if (segment.delete()) {
                LOG.info("expired file " + segment.filePath() + " deleted success");
            } else {
                LOG.info("expired file " + segment.filePath() + " deleted fail");
            }
        }
        return true;
    }

    /** clean up expired log. */
    protected void periodCleanUp() {
        loopCloseableFunction(t -> cleanUp(), cleanupMill, closed);
    }

    @Override
    public String topic() {
        return topic;
    }

    @Override
    public int activeSegment() {
        return segmentCache.size();
    }
}
