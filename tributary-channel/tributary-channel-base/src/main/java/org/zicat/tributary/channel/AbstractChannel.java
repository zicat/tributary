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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zicat.tributary.channel.group.SingleGroupManager;
import org.zicat.tributary.common.IOUtils;
import org.zicat.tributary.common.SafeFactory;

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

import static org.zicat.tributary.channel.group.MemoryGroupManager.defaultGroupOffset;

/** AbstractChannel. */
public abstract class AbstractChannel<S extends Segment> implements SingleChannel, Closeable {

    private static final Logger LOG = LoggerFactory.getLogger(AbstractChannel.class);

    private final ReentrantLock lock = new ReentrantLock();
    private final Condition newSegmentCondition = lock.newCondition();
    private final Map<Long, S> cache = new ConcurrentHashMap<>();
    private final AtomicBoolean closed = new AtomicBoolean();
    private final AtomicLong writeBytes = new AtomicLong();
    private final AtomicLong readBytes = new AtomicLong();
    private final SingleGroupManagerFactory groupManagerFactory;
    private final SingleGroupManager groupManager;
    private final String topic;

    protected long minCommitSegmentId;
    protected volatile S latestSegment;

    protected AbstractChannel(String topic, SingleGroupManagerFactory groupManagerFactory) {
        this.topic = topic;
        this.groupManagerFactory = groupManagerFactory;
        this.groupManager = groupManagerFactory.create();
        this.minCommitSegmentId = groupManager.getMinGroupOffset().segmentId();
    }

    /**
     * init last segment.
     *
     * @param lastSegment lastSegment
     */
    protected void initLastSegment(S lastSegment) {
        this.latestSegment = lastSegment;
        addSegment(lastSegment);
    }

    /** SingleGroupManagerFactory. */
    public interface SingleGroupManagerFactory extends SafeFactory<SingleGroupManager> {}

    /**
     * create segment by id.
     *
     * @param id id
     * @return Segment
     */
    protected abstract S createSegment(long id) throws IOException;

    /**
     * append record data without partition.
     *
     * @param record record
     * @throws IOException IOException
     */
    @Override
    public void append(byte[] record, int offset, int length) throws IOException {

        final S segment = this.latestSegment;
        if (append2Segment(segment, record, offset, length)) {
            return;
        }
        final ReentrantLock lock = this.lock;
        lock.lock();
        try {
            segment.finish();
            final S retrySegment = this.latestSegment;
            if (!retrySegment.append(record, offset, length)) {
                retrySegment.finish();
                final long newSegmentId = retrySegment.segmentId() + 1L;
                final S newSegment = createSegment(newSegmentId);
                append2Segment(newSegment, record, offset, length);
                writeBytes.addAndGet(latestSegment.position());
                addSegment(newSegment);
                this.latestSegment = newSegment;
                newSegmentCondition.signalAll();
            }
        } finally {
            lock.unlock();
        }
    }

    /**
     * append record 2 segment.
     *
     * @param segment segment
     * @param record record
     * @param offset offset
     * @param length length
     * @return boolean append
     * @throws IOException IOException
     */
    protected boolean append2Segment(Segment segment, byte[] record, int offset, int length)
            throws IOException {
        return segment.append(record, offset, length);
    }

    /**
     * add segment.
     *
     * @param segment segment
     */
    public void addSegment(S segment) {
        this.cache.put(segment.segmentId(), segment);
    }

    @Override
    public long lag(GroupOffset groupOffset) {
        return latestSegment.lag(groupOffset);
    }

    @Override
    public long writeBytes() {
        return writeBytes.get() + latestSegment.position();
    }

    @Override
    public long readBytes() {
        return readBytes.get();
    }

    @Override
    public long bufferUsage() {
        return latestSegment.cacheUsed();
    }

    @Override
    public RecordsResultSet poll(GroupOffset groupOffset, long time, TimeUnit unit)
            throws IOException, InterruptedException {
        final RecordsResultSet resultSet = read(groupOffset, time, unit);
        readBytes.addAndGet(resultSet.readBytes());
        return resultSet;
    }

    @Override
    public GroupOffset committedGroupOffset(String groupId) {
        final GroupOffset groupOffset = groupManager.committedGroupOffset(groupId);
        return defaultGroupOffset(groupId).equals(groupOffset)
                ? new GroupOffset(latestSegment.segmentId(), latestSegment.position(), groupId)
                : groupOffset;
    }

    @Override
    public synchronized void commit(GroupOffset groupOffset) throws IOException {
        if (latestSegment.segmentId() < groupOffset.segmentId()) {
            LOG.warn("commit group offset {} over latest segment", groupOffset);
            return;
        }
        groupManager.commit(groupOffset);
        final GroupOffset min = groupManager.getMinGroupOffset();
        if (min != null && minCommitSegmentId < min.segmentId()) {
            minCommitSegmentId = min.segmentId();
            cleanUp();
        }
    }

    @Override
    public GroupOffset getMinGroupOffset() {
        return groupManager.getMinGroupOffset();
    }

    /**
     * read file log.
     *
     * @param originalGroupOffset originalGroupOffset
     * @param time time
     * @param unit unit
     * @return LogResult
     * @throws IOException IOException
     * @throws InterruptedException InterruptedException
     */
    private RecordsResultSet read(GroupOffset originalGroupOffset, long time, TimeUnit unit)
            throws IOException, InterruptedException {

        final S latest = this.latestSegment;
        final BlockGroupOffset offset = cast(originalGroupOffset);
        S segment = latest.match(offset) ? latest : cache.get(offset.segmentId());
        if (segment == null) {
            LOG.warn("segment id {} not found, start to consume from latest offset", offset);
            segment = latest;
        }
        // try read it first
        final RecordsResultSet resultSet = segment.readBlock(offset, time, unit).toResultSet();
        if (!segment.isFinish() || !resultSet.isEmpty()) {
            return resultSet;
        }

        // searchSegment is full
        final ReentrantLock lock = this.lock;
        lock.lock();
        try {
            // searchSegment point to currentSegment, await new segment
            if (segment == this.latestSegment) {
                if (time == 0) {
                    newSegmentCondition.await();
                } else if (!newSegmentCondition.await(time, unit)) {
                    return offset.reset().toResultSet();
                }
            }
        } finally {
            lock.unlock();
        }
        return read(offset.skipNextSegmentHead(), time, unit);
    }

    /**
     * read from last segment if null or over lastSegment.
     *
     * @param groupOffset groupOffset
     * @return BlockGroupOffset
     */
    protected BlockGroupOffset cast(GroupOffset groupOffset) {
        final BlockGroupOffset newRecordOffset = BlockGroupOffset.cast(groupOffset);
        return newRecordOffset.segmentId() < minCommitSegmentId
                        || newRecordOffset.segmentId() > latestSegment.segmentId()
                ? newRecordOffset.skip2Target(
                        latestSegment.segmentId(),
                        latestSegment.position(),
                        newRecordOffset.groupId())
                : newRecordOffset;
    }

    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            groupManagerFactory.destroy(groupManager);
            cache.forEach((k, v) -> IOUtils.closeQuietly(v));
            cache.clear();
        }
    }

    @Override
    public void flush() throws IOException {
        latestSegment.flush();
    }

    /** clean up old segment. */
    protected void cleanUp() {

        if (minCommitSegmentId <= 0) {
            return;
        }
        final List<S> expiredSegments = new ArrayList<>();
        for (Map.Entry<Long, S> entry : cache.entrySet()) {
            final S segment = entry.getValue();
            if (segment.segmentId() < minCommitSegmentId
                    && segment.segmentId() < latestSegment.segmentId()) {
                expiredSegments.add(segment);
            }
        }
        for (S segment : expiredSegments) {
            cache.remove(segment.segmentId());
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
        return cache.size();
    }

    /**
     * flush quietly.
     *
     * @return true if flush success
     */
    public boolean flushQuietly() {
        try {
            flush();
            return true;
        } catch (Throwable e) {
            LOG.warn("period flush error", e);
            return false;
        }
    }
}
