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
import org.zicat.tributary.channel.utils.IOUtils;
import org.zicat.tributary.channel.utils.TributaryChannelException;
import org.zicat.tributary.channel.utils.TributaryChannelRuntimeException;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import static org.zicat.tributary.channel.file.SegmentUtil.getIdByName;
import static org.zicat.tributary.channel.file.SegmentUtil.isLogSegment;
import static org.zicat.tributary.channel.utils.Functions.loopCloseableFunction;

/**
 * FileChannel implements {@link Channel} to Storage records and {@link RecordsOffset} in local file
 * system.
 *
 * <p>All public methods in FileChannel are @ThreadSafe.
 *
 * <p>FileChannel ignore partition params and append/pull all records in the {@link
 * FileChannel#dir}. {@link PartitionFileChannel} support multi partitions operations.
 *
 * <p>Data files {@link Segment} name start with {@link FileChannel#topic}
 *
 * <p>Only one {@link Segment} is writeable and support multi threads write it, multi threads can
 * read writable segment or other segments tagged as finished(not writable).
 *
 * <p>FileChannel support commit RecordsOffset by {@link FileChannel#groupManager} and support clean
 * up expired segments(all group ids has commit the offset over this segments) async}
 */
public class FileChannel implements OnePartitionChannel, Closeable {

    private static final Logger LOG = LoggerFactory.getLogger(FileChannel.class);
    private final ReentrantLock lock = new ReentrantLock();
    private final Condition newSegmentCondition = lock.newCondition();
    protected final Map<Long, Segment> segmentCache = new ConcurrentHashMap<>();
    protected final File dir;
    protected final Long segmentSize;
    protected final CompressionType compressionType;
    protected final OnePartitionGroupManager groupManager;
    private volatile Segment lastSegment;
    private final AtomicBoolean closed = new AtomicBoolean();
    private long flushPeriodMill;
    private Thread flushSegmentThread;
    private final String topic;
    private final long flushPageCacheSize;
    private final boolean flushForce;
    private final BufferWriter bufferWriter;
    private final AtomicLong writeBytes = new AtomicLong();
    private final AtomicLong readBytes = new AtomicLong();
    private long minCommitSegmentId;

    protected FileChannel(
            String topic,
            OnePartitionGroupManager groupManager,
            File dir,
            Integer blockSize,
            Long segmentSize,
            CompressionType compressionType,
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
        this.minCommitSegmentId = groupManager.getMinRecordsOffset().segmentId();
        loadSegments();
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
        Segment maxSegment = null;
        for (File file : files) {
            final String fileName = file.getName();
            final long id = getIdByName(topic, fileName);
            final Segment segment = createLogSegment(id);
            maxSegment = SegmentUtil.max(segment, maxSegment);
        }

        for (Map.Entry<Long, Segment> entry : segmentCache.entrySet()) {
            try {
                entry.getValue().finish();
            } catch (IOException e) {
                throw new TributaryChannelRuntimeException("finish history segment fail", e);
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
    private Segment createLogSegment(long id) {
        final Segment segment =
                new SegmentBuilder()
                        .fileId(id)
                        .dir(dir)
                        .segmentSize(segmentSize)
                        .filePrefix(topic)
                        .compressionType(compressionType)
                        .build(bufferWriter);
        segmentCache.put(id, segment);
        return segment;
    }

    /**
     * append record data without partition.
     *
     * @param record record
     * @throws IOException IOException
     */
    @Override
    public void append(byte[] record, int offset, int length) throws IOException {

        final Segment segment = this.lastSegment;
        if (segment.append(record, offset, length)) {
            checkSyncForce(segment);
        } else {
            final ReentrantLock lock = this.lock;
            lock.lock();
            try {
                segment.finish();
                final Segment retrySegment = this.lastSegment;
                if (!retrySegment.append(record, offset, length)) {
                    retrySegment.finish();
                    final Segment newSegment = createLogSegment(retrySegment.fileId() + 1L);
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
    private void checkSyncForce(Segment segment) throws IOException {
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
        final RecordsOffset min = groupManager.getMinRecordsOffset();
        if (minCommitSegmentId < min.segmentId()) {
            minCommitSegmentId = min.segmentId();
            cleanUp();
        }
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

        final Segment lastSegment = this.lastSegment;
        final BufferRecordsOffset recordsOffset = cast(originalRecordsOffset);
        final Segment segment =
                lastSegment.matchSegment(recordsOffset)
                        ? lastSegment
                        : segmentCache.get(recordsOffset.segmentId());
        if (segment == null) {
            throw new TributaryChannelException(
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

    /** clean up old segment. */
    protected void cleanUp() {

        if (minCommitSegmentId <= 0) {
            return;
        }

        final List<Segment> expiredSegments = new ArrayList<>();
        for (Map.Entry<Long, Segment> entry : segmentCache.entrySet()) {
            final Segment segment = entry.getValue();
            if (segment.fileId() < minCommitSegmentId && segment.fileId() < lastSegment.fileId()) {
                expiredSegments.add(segment);
            }
        }

        for (Segment segment : expiredSegments) {
            segmentCache.remove(segment.fileId());
            if (segment.delete()) {
                LOG.info("expired file " + segment.filePath() + " deleted success");
            } else {
                LOG.info("expired file " + segment.filePath() + " deleted fail");
            }
        }
    }

    @Override
    public String topic() {
        return topic;
    }

    @Override
    public Set<String> groups() {
        return groupManager.groups();
    }

    @Override
    public int activeSegment() {
        return segmentCache.size();
    }
}
