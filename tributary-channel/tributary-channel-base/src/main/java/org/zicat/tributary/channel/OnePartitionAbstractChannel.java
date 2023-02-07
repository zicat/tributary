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

package org.zicat.tributary.channel;

import org.zicat.tributary.common.IOUtils;
import org.zicat.tributary.common.TributaryIOException;

import java.io.Closeable;
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

/** OnePartitionAbstractChannel. */
public abstract class OnePartitionAbstractChannel<S extends Segment>
        implements OnePartitionChannel, Closeable {

    private final ReentrantLock lock = new ReentrantLock();
    private final Condition newSegmentCondition = lock.newCondition();

    protected final AtomicBoolean closed = new AtomicBoolean();
    protected final Map<Long, S> segmentCache = new ConcurrentHashMap<>();
    protected final Long segmentSize;
    protected final CompressionType compressionType;
    protected final OnePartitionGroupManager groupManager;
    protected volatile S lastSegment;
    protected final String topic;
    protected final boolean flushForce;
    protected final BlockWriter blockWriter;
    protected final AtomicLong writeBytes = new AtomicLong();
    protected final AtomicLong readBytes = new AtomicLong();
    protected long minCommitSegmentId;

    protected OnePartitionAbstractChannel(
            String topic,
            OnePartitionGroupManager groupManager,
            Integer blockSize,
            Long segmentSize,
            CompressionType compressionType,
            boolean flushForce) {
        this.topic = topic;
        this.blockWriter = new BlockWriter(blockSize);
        this.segmentSize = segmentSize;
        this.compressionType = compressionType;
        this.groupManager = groupManager;
        this.flushForce = flushForce;
        this.minCommitSegmentId = groupManager.getMinRecordsOffset().segmentId();
    }

    /**
     * create segment by id.
     *
     * @param id id
     * @return LogSegment
     */
    protected abstract S createSegment(long id);

    /**
     * append record data without partition.
     *
     * @param record record
     * @throws IOException IOException
     */
    @Override
    public void append(byte[] record, int offset, int length) throws IOException {

        final S segment = this.lastSegment;
        if (segment.append(record, offset, length)) {
            appendSuccessCallback(segment);
        } else {
            final ReentrantLock lock = this.lock;
            lock.lock();
            try {
                segment.finish();
                final S retrySegment = this.lastSegment;
                if (!retrySegment.append(record, offset, length)) {
                    retrySegment.finish();
                    final long newSegmentId = retrySegment.segmentId() + 1L;
                    final S newSegment = createSegment(newSegmentId);
                    newSegment.append(record, offset, length);
                    writeBytes.addAndGet(lastSegment.position());
                    setLastSegment(newSegment);
                    newSegmentCondition.signalAll();
                }
            } finally {
                lock.unlock();
            }
        }
    }

    /**
     * set new last segment.
     *
     * @param segment segment
     */
    public void setLastSegment(S segment) {
        addSegment(segment, true);
    }

    /**
     * add segment.
     *
     * @param segment segment
     * @param last last
     */
    public void addSegment(S segment, boolean last) {
        this.segmentCache.put(segment.segmentId(), segment);
        if (last) {
            this.lastSegment = segment;
        }
    }

    /**
     * append success callback.
     *
     * @param segment segment
     * @throws IOException ioException
     */
    protected abstract void appendSuccessCallback(S segment) throws IOException;

    @Override
    public long lag(RecordsOffset recordsOffset) {
        return lastSegment.lag(recordsOffset);
    }

    @Override
    public long lastSegmentId() {
        return lastSegment.segmentId();
    }

    @Override
    public long writeBytes() {
        return writeBytes.get() + lastSegment.position();
    }

    @Override
    public long readBytes() {
        return readBytes.get();
    }

    @Override
    public long pageCache() {
        return lastSegment.cacheUsed();
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
    public synchronized void commit(String groupId, RecordsOffset recordsOffset)
            throws IOException {
        groupManager.commit(groupId, recordsOffset);
        final RecordsOffset min = groupManager.getMinRecordsOffset();
        if (min != null && minCommitSegmentId < min.segmentId()) {
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

        final S lastSegment = this.lastSegment;
        final BlockRecordsOffset recordsOffset = cast(originalRecordsOffset);
        final S segment =
                lastSegment.matchSegment(recordsOffset)
                        ? lastSegment
                        : segmentCache.get(recordsOffset.segmentId());
        if (segment == null) {
            throw new TributaryIOException(
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
     * @return BlockRecordsOffset
     */
    protected BlockRecordsOffset cast(RecordsOffset recordsOffset) {
        if (recordsOffset == null) {
            return BlockRecordsOffset.cast(lastSegment.segmentId());
        }
        final BlockRecordsOffset newRecordOffset = BlockRecordsOffset.cast(recordsOffset);
        return recordsOffset.segmentId() < 0 || recordsOffset.segmentId() > lastSegment.segmentId()
                ? newRecordOffset.skip2TargetHead(lastSegment.segmentId())
                : newRecordOffset;
    }

    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            IOUtils.closeQuietly(groupManager);
            segmentCache.forEach((k, v) -> IOUtils.closeQuietly(v));
            segmentCache.clear();
            closeCallback();
        }
    }

    /** close callback. */
    protected abstract void closeCallback();

    @Override
    public void flush() throws IOException {
        flush(true);
    }

    /**
     * flush with force.
     *
     * @param force force
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
        final List<S> expiredSegments = new ArrayList<>();
        for (Map.Entry<Long, S> entry : segmentCache.entrySet()) {
            final S segment = entry.getValue();
            if (segment.segmentId() < minCommitSegmentId
                    && segment.segmentId() < lastSegment.segmentId()) {
                expiredSegments.add(segment);
            }
        }
        for (S segment : expiredSegments) {
            segmentCache.remove(segment.segmentId());
            recycleSegment(segment);
        }
    }

    /** recycle segment. */
    protected void recycleSegment(S segment) {
        segment.recycle();
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
