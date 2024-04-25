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

import static org.zicat.tributary.channel.group.MemoryGroupManager.defaultGroupOffset;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zicat.tributary.channel.group.MemoryGroupManager;
import org.zicat.tributary.common.GaugeFamily;
import org.zicat.tributary.common.GaugeKey;
import org.zicat.tributary.common.IOUtils;
import org.zicat.tributary.common.SafeFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/** AbstractChannel. */
public abstract class AbstractChannel<S extends Segment> implements SingleChannel, Closeable {

    private static final Logger LOG = LoggerFactory.getLogger(AbstractChannel.class);

    public static final GaugeKey KEY_WRITE_BYTES =
            new GaugeKey("channel_write_bytes", "channel write bytes");
    public static final GaugeKey KEY_READ_BYTES =
            new GaugeKey("channel_read_bytes", "channel read bytes");
    public static final GaugeKey KEY_BUFFER_USAGE =
            new GaugeKey("channel_buffer_usage", "channel buffer usage");
    public static final GaugeKey KEY_ACTIVE_SEGMENT =
            new GaugeKey("channel_active_segment", "channel active segment");

    public static final GaugeKey KEY_BLOCK_CACHE_QUERY_HIT_COUNT =
            new GaugeKey(
                    "channel_block_cache_query_hit_count", "channel block cache query hit count");

    public static final GaugeKey KEY_BLOCK_CACHE_QUERY_TOTAL_COUNT =
            new GaugeKey(
                    "channel_block_cache_query_total_count",
                    "channel block cache query total count");

    private final ReentrantLock lock = new ReentrantLock();
    private final Condition newSegmentCondition = lock.newCondition();
    private final Map<Long, S> cache = new ConcurrentHashMap<>();
    private final AtomicBoolean closed = new AtomicBoolean();
    private final AtomicLong writeBytes = new AtomicLong();
    private final AtomicLong readBytes = new AtomicLong();
    private final MemoryGroupManagerFactory groupManagerFactory;
    private final MemoryGroupManager groupManager;
    private final String topic;
    protected final ChannelBlockCache bCache;
    protected volatile S latestSegment;

    protected AbstractChannel(
            String topic, int blockCacheCount, MemoryGroupManagerFactory groupManagerFactory) {
        this.topic = topic;
        this.bCache = blockCacheCount <= 0 ? null : new ChannelBlockCache(blockCacheCount);
        this.groupManagerFactory = groupManagerFactory;
        this.groupManager = groupManagerFactory.create();
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

    /** MemoryGroupManagerFactory. */
    public interface MemoryGroupManagerFactory extends SafeFactory<MemoryGroupManager> {}

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
            segment.readonly();
            final S retrySegment = this.latestSegment;
            if (!retrySegment.append(record, offset, length)) {
                retrySegment.readonly();
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
    protected void addSegment(S segment) {
        this.cache.put(segment.segmentId(), segment);
    }

    @Override
    public long lag(GroupOffset groupOffset) {

        if (groupOffset == null) {
            return 0L;
        }

        final long latestSegmentId = latestSegment.segmentId();
        long totalLag = 0;
        for (long segmentId = groupOffset.segmentId; segmentId <= latestSegmentId; segmentId++) {
            final S segment = cache.get(segmentId);
            if (segment == null) {
                continue;
            }
            totalLag += segment.lag(groupOffset);
        }
        return totalLag;
    }

    @Override
    public Map<GaugeKey, GaugeFamily> gaugeFamily() {
        final Map<GaugeKey, GaugeFamily> families = new HashMap<>();
        KEY_WRITE_BYTES.value(writeBytes.get() + latestSegment.writeBytes()).register(families);
        KEY_READ_BYTES.value(readBytes.get()).register(families);
        KEY_BUFFER_USAGE.value(latestSegment.cacheUsed()).register(families);
        KEY_ACTIVE_SEGMENT.value(cache.size()).register(families);
        if (bCache != null) {
            KEY_BLOCK_CACHE_QUERY_HIT_COUNT.value(bCache.matchCount()).register(families);
            KEY_BLOCK_CACHE_QUERY_TOTAL_COUNT.value(bCache.totalCount()).register(families);
        }
        return families;
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
    public void commit(GroupOffset groupOffset) {
        if (latestSegment.segmentId() < groupOffset.segmentId()) {
            LOG.warn("commit group offset {} over latest segment", groupOffset);
            return;
        }
        groupManager.commit(groupOffset);
        cleanUp();
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

        final S latestSegment = this.latestSegment;

        BlockGroupOffset offset = cast(originalGroupOffset);
        S segment = latestSegment.match(offset) ? latestSegment : cache.get(offset.segmentId());
        if (segment == null) {
            final Offset latestOffset = this.latestSegment.latestOffset();
            LOG.warn("{} not found, consume from latest offset {}", offset, latestOffset);
            segment = latestSegment;
            offset = offset.skip2Target(latestOffset);
        }

        // try read it first
        final RecordsResultSet resultSet = segment.readBlock(offset, time, unit).toResultSet();
        if (!segment.isReadonly() || !resultSet.isEmpty()) {
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
        return legalOffset(newRecordOffset)
                ? newRecordOffset
                : newRecordOffset.skip2Target(latestSegment.segmentId(), latestSegment.position());
    }

    /**
     * check offset is legal.
     *
     * @param offset offset
     * @return legal offset
     */
    private boolean legalOffset(Offset offset) {
        return offset.compareTo(groupManager.getMinGroupOffset()) >= 0
                || offset.compareTo(latestSegment.latestOffset()) <= 0;
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
    @SuppressWarnings("resource")
    protected void cleanUp() {
        final long minSegmentId = groupManager.getMinGroupOffset().segmentId();
        final List<S> expiredSegments = new ArrayList<>();
        for (Map.Entry<Long, S> entry : cache.entrySet()) {
            final S segment = entry.getValue();
            if (segment.segmentId() < minSegmentId
                    && segment.segmentId() < latestSegment.segmentId()) {
                expiredSegments.add(segment);
            }
        }
        for (S segment : expiredSegments) {
            cache.remove(segment.segmentId());
            segment.recycle();
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

    /**
     * active segment count.
     *
     * @return count
     */
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
