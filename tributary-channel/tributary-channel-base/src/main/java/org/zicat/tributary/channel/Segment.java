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

import static org.zicat.tributary.channel.SegmentUtil.BLOCK_HEAD_SIZE;
import static org.zicat.tributary.common.IOUtils.copy;
import static org.zicat.tributary.common.IOUtils.reAllocate;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * A segment instance is the represent of parts data in one channel.
 *
 * <p>The Segment is the unit of data expired by channel.
 *
 * <p>The life cycle of a segment instance include {@link Segment#Segment}, {@link
 * Segment#append}/{@link Segment#readBlock}, {@link Segment#readonly()}, {@link Segment#close()},
 * {@link Segment#recycle()}
 *
 * <p>All public methods are @ThreadSafe
 *
 * <p>struct: doc/picture/segment.png
 */
public abstract class Segment implements SegmentStorage, Closeable, Comparable<Segment> {

    private static final String OFFSET_MESSAGE = "next offset {}, limit offset {}";
    private static final Logger LOG = LoggerFactory.getLogger(Segment.class);
    private final long id;
    private final long segmentSize;

    private final ReentrantLock lock = new ReentrantLock();
    private final Condition readable = lock.newCondition();

    private final AtomicBoolean closed = new AtomicBoolean();
    private final AtomicBoolean readonly = new AtomicBoolean();
    protected final AtomicLong position = new AtomicLong();
    protected final BlockWriter writer;
    protected final CompressionType compression;
    protected long cacheUsed = 0;
    private final ChannelBlockCache bCache;
    private final BlockWriter.BlockFlushHandler blockFlushHandler;

    public Segment(
            long id,
            BlockWriter writer,
            CompressionType compression,
            long segmentSize,
            long position,
            ChannelBlockCache bCache) {
        this.id = id;
        this.segmentSize = segmentSize;
        this.writer = writer;
        this.compression = compression;
        this.position.set(position);
        this.bCache = bCache;
        this.blockFlushHandler = createBlockFlushHandler();
    }

    /**
     * create block flush handler.
     *
     * @return BlockFlushHandler
     */
    private BlockWriter.BlockFlushHandler createBlockFlushHandler() {
        return (block) -> {
            final ByteBuffer buff = bCache == null ? null : block.resultBuf().duplicate();
            block.reusedBuf(compression.compression(block.resultBuf(), block.reusedBuf()));
            final int writeCount = block.reusedBuf().remaining();
            writeFull(block.reusedBuf());
            final long preOffset = position.getAndAdd(writeCount);
            if (buff != null) {
                bCache.put(id, preOffset, preOffset + writeCount, copy(buff));
            }
            cacheUsed += writeCount;
            readable.signalAll();
        };
    }

    /**
     * append bytes to log segment.
     *
     * <p>if segment is readonly or segment current size over {@link Segment#segmentSize} , return
     * false.
     *
     * @param data data
     * @param offset offset
     * @param length length
     * @return true if append success
     * @throws IOException IOException if block flush to SegmentStorage
     */
    public boolean append(byte[] data, int offset, int length) throws IOException {

        if (length <= 0) {
            return true;
        }

        final ReentrantLock lock = this.lock;
        lock.lock();
        try {
            if (isReadonly() || position() > segmentSize) {
                return false;
            }

            if (writer.put(data, offset, length)) {
                return true;
            }

            /* writer is full, start to flush channel and clean writer,
             * then try to put data to writer again.
             */
            writer.clear(blockFlushHandler);
            if (writer.put(data, offset, length)) {
                return true;
            }

            /* data length is over writer.size,
             * wrap new writer which match data length and flush channel directly
             */
            BlockWriter.wrap(data, offset, length).clear(blockFlushHandler);
            return true;
        } finally {
            lock.unlock();
        }
    }

    /**
     * get legal offset.
     *
     * @param offset offset
     * @return new offset
     */
    protected abstract long legalOffset(long offset);

    /**
     * blocking read data from SegmentStorage.
     *
     * <p>if Segment is closed OR read position is over {@link Segment#position} and readonly,
     * return empty blockGroupOffset
     *
     * <p>if read position is over {@link Segment#position} and not readonly, block wait for writing
     * thread wake up.
     *
     * @param blockGroupOffset offset in SegmentStorage
     * @param time block time
     * @param unit time unit
     * @return return null if eof or timeout, else return data
     * @throws IOException IOException when read storage
     * @throws InterruptedException InterruptedException
     */
    public BlockGroupOffset readBlock(BlockGroupOffset blockGroupOffset, long time, TimeUnit unit)
            throws IOException, InterruptedException {

        final long readablePosition = position();
        final long offset = legalOffset(blockGroupOffset.offset());
        if (offset < readablePosition) {
            return read(blockGroupOffset, readablePosition);
        }
        final ReentrantLock lock = this.lock;
        lock.lock();
        try {
            if (offset >= position() && !isReadonly()) {
                if (time == 0) {
                    readable.await();
                } else if (!readable.await(time, unit)) {
                    return blockGroupOffset.reset();
                }
            } else if (offset >= position()) {
                return blockGroupOffset.reset();
            }
        } finally {
            lock.unlock();
        }
        return read(blockGroupOffset, position());
    }

    /**
     * read from block offset.
     *
     * @param blockGroupOffset blockGroupOffset
     * @param limitOffset limitOffset
     * @throws IOException IOException
     */
    public BlockGroupOffset read(BlockGroupOffset blockGroupOffset, long limitOffset)
            throws IOException {

        blockGroupOffset = blockGroupOffset.skipOffset(legalOffset(blockGroupOffset.offset()));
        long offset = blockGroupOffset.offset();

        if (offset >= limitOffset) {
            blockGroupOffset.reset();
            return blockGroupOffset;
        }

        final BlockGroupOffset inCache;
        if (bCache != null && (inCache = bCache.find(blockGroupOffset)) != null) {
            return inCache;
        }

        final Block block = blockGroupOffset.block();
        final ByteBuffer headBuf = reAllocate(block.reusedBuf(), BLOCK_HEAD_SIZE);

        readFull(headBuf, offset);
        headBuf.flip();
        offset += headBuf.remaining();

        if (offset >= limitOffset) {
            LOG.warn("read block head over limit, " + OFFSET_MESSAGE, offset, limitOffset);
            return skip2TargetOffset(blockGroupOffset, limitOffset, headBuf);
        }

        final int dataLength = headBuf.getInt();
        if (dataLength <= 0) {
            LOG.warn("data length is less than 0, real value {}", dataLength);
            return skip2TargetOffset(blockGroupOffset, limitOffset, headBuf);
        }

        final long finalOffset = dataLength + offset;
        if (finalOffset > limitOffset) {
            LOG.warn("read block body over limit, " + OFFSET_MESSAGE, finalOffset, limitOffset);
            return skip2TargetOffset(blockGroupOffset, limitOffset, headBuf);
        }

        final ByteBuffer reusedBuf = reAllocate(block.reusedBuf(), dataLength << 1, dataLength);
        readFull(reusedBuf, offset);
        reusedBuf.flip();
        final ByteBuffer resultBuf = compression.decompression(reusedBuf, block.resultBuf());
        final long readBytes = dataLength + BLOCK_HEAD_SIZE;
        final BlockReader bufferReader = new BlockReader(resultBuf, reusedBuf, readBytes);
        return blockGroupOffset.newOffsetReader(finalOffset, bufferReader);
    }

    /**
     * check groupOffset whether in this segment.
     *
     * @param groupOffset groupOffset
     * @return boolean match
     */
    public final boolean match(Offset groupOffset) {
        return groupOffset != null && this.segmentId() == groupOffset.segmentId();
    }

    /**
     * segment id.
     *
     * @return segment id
     */
    public final long segmentId() {
        return id;
    }

    /**
     * get max readable offset.
     *
     * @return offset
     */
    public final Offset latestOffset() {
        return new Offset(segmentId(), position());
    }

    /**
     * flush data.
     *
     * @throws IOException IOException
     */
    public void flush() throws IOException {
        flush(true);
    }

    /**
     * flush page cache data to disk.
     *
     * @param force if forced, block data will flush to page cache first.
     * @throws IOException IOException
     */
    public void flush(boolean force) throws IOException {

        if (isReadonly() || cacheUsed == 0 && !force) {
            return;
        }
        final ReentrantLock lock = this.lock;
        lock.lock();
        try {
            if (isReadonly() || cacheUsed == 0 && !force) {
                return;
            }
            if (!writer.isEmpty() && force) {
                writer.clear(blockFlushHandler);
            }
            persist(force);
            cacheUsed = 0;
        } finally {
            lock.unlock();
        }
    }

    /**
     * set log segment to be not writeable, but read is allowed.
     *
     * @throws IOException IOException
     */
    public void readonly() throws IOException {

        if (isReadonly()) {
            return;
        }
        final ReentrantLock lock = this.lock;
        lock.lock();
        try {
            if (isReadonly()) {
                return;
            }
            flush();
            readonly.set(true);
            readable.signalAll();
        } finally {
            lock.unlock();
        }
    }

    /**
     * whether finish.
     *
     * @return true if segment is not allow to write
     */
    public boolean isReadonly() {
        return readonly.get();
    }

    @Override
    public void close() throws IOException {
        if (closed.compareAndSet(false, true)) {
            readonly();
        }
    }

    @Override
    public int compareTo(Segment o) {
        return Long.compare(this.segmentId(), o.segmentId());
    }

    /**
     * if group offset segment is null or more than current segment than return 0, if group offset
     * segment equals return position - offset else return position.
     *
     * @param groupOffset groupOffset
     * @return long lag
     */
    public final long lag(Offset groupOffset) {
        if (groupOffset == null
                || groupOffset.segmentId() < 0
                || groupOffset.segmentId() > this.segmentId()) {
            return 0L;
        }
        final long offset = legalOffset(groupOffset.offset());
        return match(groupOffset) ? Math.max(0, position() - offset) : position();
    }

    /**
     * get compression type.
     *
     * @return CompressionType
     */
    @Override
    public CompressionType compressionType() {
        return compression;
    }

    /**
     * return page cache used count.
     *
     * @return long
     */
    public final long cacheUsed() {
        return cacheUsed + writer.position();
    }

    /**
     * get readable position.
     *
     * @return long
     */
    public final long position() {
        return position.get();
    }

    /**
     * get write bytes of a segment, not include head.
     *
     * @return write bytes.
     */
    public long writeBytes() {
        return position.get();
    }

    /**
     * blockSize.
     *
     * @return int size
     */
    public final int blockSize() {
        return writer.capacity();
    }

    /**
     * skip to target offset with empty block.
     *
     * @param blockGroupOffset blockGroupOffset
     * @param newOffset newOffset
     * @param reusedBuf reusedBuf
     * @return BlockGroupOffset
     */
    private static BlockGroupOffset skip2TargetOffset(
            BlockGroupOffset blockGroupOffset, long newOffset, ByteBuffer reusedBuf) {
        blockGroupOffset.block().reset().reusedBuf(reusedBuf);
        return blockGroupOffset.skip2TargetOffset(newOffset);
    }
}
