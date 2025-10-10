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

import static java.lang.Math.min;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zicat.tributary.channel.Segment.AppendResult;
import org.zicat.tributary.channel.group.SingleGroupManager;
import org.zicat.tributary.common.MetricKey;
import org.zicat.tributary.common.IOUtils;
import org.zicat.tributary.common.SafeFactory;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/** AbstractSingleChannel. */
public abstract class AbstractSingleChannel<S extends Segment> implements SingleChannel, Closeable {

    private static final Logger LOG = LoggerFactory.getLogger(AbstractSingleChannel.class);

    public static final MetricKey KEY_WRITE_BYTES = new MetricKey("tributary_channel_write_bytes");
    public static final MetricKey KEY_READ_BYTES = new MetricKey("tributary_channel_read_bytes");
    public static final MetricKey KEY_APPEND_COUNTER =
            new MetricKey("tributary_channel_append_counter");
    public static final MetricKey KEY_BUFFER_USAGE =
            new MetricKey("tributary_channel_buffer_usage");
    public static final MetricKey KEY_ACTIVE_SEGMENT =
            new MetricKey("tributary_channel_active_segment");

    public static final MetricKey KEY_BLOCK_CACHE_QUERY_HIT_COUNT =
            new MetricKey("tributary_channel_block_cache_query_hit_count");

    public static final MetricKey KEY_BLOCK_CACHE_QUERY_TOTAL_COUNT =
            new MetricKey("tributary_channel_block_cache_query_total_count");

    private final ReentrantLock lock = new ReentrantLock();
    private final Condition newSegmentCondition = lock.newCondition();
    private final AtomicBoolean closed = new AtomicBoolean();
    private final AtomicLong writeBytes = new AtomicLong();
    private final AtomicLong readBytes = new AtomicLong();
    private final AtomicLong appendCounter = new AtomicLong();
    private final SingleGroupManagerFactory singleGroupManagerFactory;
    private final SingleGroupManager singleGroupManager;
    private final String topic;
    protected final Map<Long, S> cache = new ConcurrentHashMap<>();
    protected final ChannelBlockCache bCache;
    protected volatile S latestSegment;

    protected AbstractSingleChannel(
            String topic,
            int blockCacheCount,
            SingleGroupManagerFactory singleGroupManagerFactory) {
        this.topic = topic;
        this.bCache = blockCacheCount <= 0 ? null : new ChannelBlockCache(blockCacheCount);
        this.singleGroupManagerFactory = singleGroupManagerFactory;
        this.singleGroupManager = singleGroupManagerFactory.create();
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
     * @param byteBuffer byteBuffer
     * @throws IOException IOException
     */
    @Override
    public void append(ByteBuffer byteBuffer) throws IOException, InterruptedException {
        innerAppend(byteBuffer);
        appendCounter.incrementAndGet();
    }

    /**
     * append byte buffer.
     *
     * @param byteBuffer byteBuffer
     * @return AppendResult
     * @throws IOException IOException
     */
    protected AppendResult innerAppend(ByteBuffer byteBuffer) throws IOException {
        final S segment = this.latestSegment;
        final AppendResult result = append2Segment(segment, byteBuffer);
        if (result.appended()) {
            return result;
        }
        final ReentrantLock lock = this.lock;
        lock.lock();
        try {
            if (segment != this.latestSegment) {
                return innerAppend(byteBuffer);
            }
            segment.readonly();
            final long newSegmentId = segment.segmentId() + 1L;
            final S newSegment = createSegment(newSegmentId);
            final AppendResult newResult = append2Segment(newSegment, byteBuffer);
            if (!newResult.appended()) {
                throw new IOException("write new segment failed, segmentId: " + newSegmentId);
            }
            writeBytes.addAndGet(latestSegment.position());
            addSegment(newSegment);
            this.latestSegment = newSegment;
            newSegmentCondition.signalAll();
            return newResult;
        } finally {
            lock.unlock();
        }
    }

    /**
     * append record 2 segment.
     *
     * @param segment segment
     * @return boolean append
     * @throws IOException IOException
     */
    protected AppendResult append2Segment(Segment segment, ByteBuffer byteBuffer)
            throws IOException {
        return segment.append(byteBuffer);
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
    public long lag(Offset offset) {
        if (offset == null) {
            return 0L;
        }
        final long latestSegmentId = latestSegment.segmentId();
        long totalLag = 0;
        for (long segmentId = offset.segmentId; segmentId <= latestSegmentId; segmentId++) {
            final S segment = cache.get(segmentId);
            if (segment == null) {
                continue;
            }
            totalLag += segment.lag(offset);
        }
        return totalLag;
    }

    @Override
    public Map<MetricKey, Double> gaugeFamily() {
        final Map<MetricKey, Double> families = new HashMap<>();
        families.put(KEY_WRITE_BYTES, (double) (writeBytes.get() + latestSegment.position()));
        families.put(KEY_READ_BYTES, (double) readBytes.get());
        families.put(KEY_BUFFER_USAGE, (double) latestSegment.cacheUsed());
        families.put(KEY_ACTIVE_SEGMENT, (double) cache.size());
        if (bCache != null) {
            families.put(KEY_BLOCK_CACHE_QUERY_HIT_COUNT, (double) bCache.matchCount());
            families.put(KEY_BLOCK_CACHE_QUERY_TOTAL_COUNT, (double) bCache.totalCount());
        }
        return families;
    }

    @Override
    public Map<MetricKey, Double> counterFamily() {
        return Collections.singletonMap(KEY_APPEND_COUNTER, (double) appendCounter.get());
    }

    @Override
    public RecordsResultSet poll(Offset offset, long time, TimeUnit unit)
            throws IOException, InterruptedException {
        final RecordsResultSet resultSet = read(offset, time, unit);
        readBytes.addAndGet(resultSet.readBytes());
        return resultSet;
    }

    @Override
    public Offset committedOffset(String groupId) {
        final Offset offset = singleGroupManager.committedOffset(groupId);
        if (!offset.uninitialized()) {
            return offset;
        }
        final Offset minOffset = singleGroupManager.getMinOffset();
        return minOffset.uninitialized() ? new Offset(latestSegment.segmentId()) : minOffset;
    }

    @Override
    public void commit(String groupId, Offset offset) throws IOException {
        if (checkOffsetOver(offset)) {
            return;
        }
        singleGroupManager.commit(groupId, offset);
    }

    @Override
    public void commit(Offset offset) {
        if (checkOffsetOver(offset)) {
            return;
        }
        singleGroupManager.commit(offset);
    }

    @Override
    public Offset getMinOffset() {
        return singleGroupManager.getMinOffset();
    }

    /**
     * check offset over latest segment.
     *
     * @param offset offset
     * @return checkOffsetOver
     */
    private boolean checkOffsetOver(Offset offset) {
        final boolean result = latestSegment.segmentId() < offset.segmentId();
        if (result) {
            LOG.warn("commit group offset {} over latest segment", offset);
        }
        return result;
    }

    /**
     * read file log.
     *
     * @param originalOffset originalOffset
     * @param time time
     * @param unit unit
     * @return LogResult
     * @throws IOException IOException
     * @throws InterruptedException InterruptedException
     */
    private RecordsResultSet read(Offset originalOffset, long time, TimeUnit unit)
            throws IOException, InterruptedException {

        final S latestSegment = this.latestSegment;

        BlockReaderOffset offset = cast(originalOffset);
        S segment = latestSegment.match(offset) ? latestSegment : cache.get(offset.segmentId());
        if (Segment.isClosed(segment)) {
            segment = selectMinReadableSegment();
            segment = Segment.isClosed(segment) ? latestSegment : segment;
            offset = offset.skip2TargetHead(segment.segmentId());
            LOG.warn("{} not found or closed, consume from new offset {}", originalOffset, offset);
        }

        try {
            // try read it first
            final RecordsResultSet resultSet = segment.readBlock(offset, time, unit).toResultSet();
            if (!segment.isReadonly() || !resultSet.isEmpty()) {
                return resultSet;
            }
        } catch (Exception e) {
            // the segment may close by other thread because of capacity check thread
            if (Segment.isClosed(segment)) {
                LOG.info("segment {} is closed, skip to next", segment.segmentId());
                return read(offset.skipNextSegmentHead(), time, unit);
            }
            throw e;
        }

        // search segment is readonly and read offset at end of segment
        final ReentrantLock lock = this.lock;
        lock.lock();
        try {
            // search segment point to latest segment, await new segment append
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
     * select min readable segment.
     *
     * @return segment
     */
    private S selectMinReadableSegment() {
        final Offset minOffset = singleGroupManager.getMinOffset();
        final S segment = cache.get(minOffset.segmentId());
        if (!Segment.isClosed(segment)) {
            return segment;
        }
        final int totalSegmentCount = cache.size();
        final long segmentOffset = latestSegment.segmentId() - totalSegmentCount + 1;
        for (int i = 0; i < totalSegmentCount; i++) {
            final long segmentId = segmentOffset + i;
            if (segmentId < minOffset.segmentId()) {
                continue;
            }
            final S segmentInCache = cache.get(segmentId);
            if (!Segment.isClosed(segmentInCache)) {
                return segmentInCache;
            }
        }
        return null;
    }

    /**
     * read from last segment if null or over lastSegment.
     *
     * @param offset offset
     * @return BlockGroupOffset
     */
    protected BlockReaderOffset cast(Offset offset) {
        final BlockReaderOffset newRecordOffset = BlockReaderOffset.cast(offset);
        if (legalOffset(newRecordOffset)) {
            return newRecordOffset;
        }
        final Offset minOffset = singleGroupManager.getMinOffset();
        return minOffset.uninitialized()
                ? newRecordOffset.skip2TargetHead(latestSegment.segmentId())
                : newRecordOffset.skip2TargetHead(minOffset.segmentId);
    }

    /**
     * check offset is legal.
     *
     * @param offset offset
     * @return legal offset
     */
    private boolean legalOffset(Offset offset) {
        final Offset minOffset = singleGroupManager.getMinOffset();
        return offset.compareTo(minOffset) >= 0
                || offset.compareTo(latestSegment.latestOffset()) <= 0;
    }

    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            singleGroupManagerFactory.destroy(singleGroupManager);
            cache.forEach((k, v) -> IOUtils.closeQuietly(v));
            cache.clear();
            LOG.info("close channel topic = {}", topic);
        }
    }

    @Override
    public void flush() throws IOException {
        latestSegment.flush();
    }

    @Override
    public String topic() {
        return topic;
    }

    @Override
    public Set<String> groups() {
        return singleGroupManager.groups();
    }

    /**
     * active segment count.
     *
     * @return count
     */
    public int activeSegment() {
        return cache.size();
    }

    /** close and cleanup expired segments. */
    public void cleanUpExpiredSegmentsQuietly() {
        final Offset minOffset = singleGroupManager.getMinOffset();
        if (minOffset.uninitialized()) {
            return;
        }
        final long minSegmentId = minOffset.segmentId();
        final List<S> expiredSegments = new ArrayList<>();
        for (Map.Entry<Long, S> entry : cache.entrySet()) {
            final S segment = entry.getValue();
            if (segment.segmentId() < minSegmentId) {
                expiredSegments.add(segment);
            }
        }
        final Segment latestSegment = this.latestSegment;
        for (S segment : expiredSegments) {
            if (segment.segmentId() >= latestSegment.segmentId()) {
                continue;
            }
            final S segmentInCache = cache.remove(segment.segmentId());
            IOUtils.closeQuietly(segmentInCache);
            Segment.recycleQuietly(segmentInCache);
        }
    }

    public void checkCapacityAndCloseEarliestSegments(long capacity) {
        final List<S> segments = new ArrayList<>(cache.values());
        segments.sort(Comparator.reverseOrder()); // sort by segment id desc
        int offset = 0;
        long size = 0;
        for (; offset < segments.size(); offset++) {
            size += segments.get(offset).position();
            if (size > capacity) {
                break;
            }
        }
        if (size <= capacity) {
            return;
        }
        final long latestSegmentId = this.latestSegment.segmentId();
        final long committableId = min(segments.get(offset).segmentId() + 1L, latestSegmentId);
        singleGroupManager.commit(new Offset(committableId));
        for (; offset < segments.size(); offset++) {
            final long segmentId = segments.get(offset).segmentId();
            if (segmentId >= committableId) {
                continue;
            }
            final S segmentInCache = cache.remove(segmentId);
            if (segmentInCache != null) {
                LOG.warn("close and recycle segment {} for capacity", segmentId);
                IOUtils.closeQuietly(segmentInCache);
                Segment.recycleQuietly(segmentInCache);
            }
        }
    }
}
