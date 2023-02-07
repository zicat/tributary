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
import org.zicat.tributary.common.IOUtils;
import org.zicat.tributary.common.TributaryIOException;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import static org.zicat.tributary.channel.SegmentUtil.BLOCK_HEAD_SIZE;

/**
 * A segment instance is the represent of id.
 *
 * <p>The life cycle of a segment instance include create(construct function), writeable/readable,
 * readonly(invoke finish method), closed(invoke close method), delete(invoke delete method)
 *
 * <p>All public methods are @ThreadSafe
 *
 * <p>struct: doc/picture/segment.png
 */
public abstract class Segment implements SegmentStorage, Closeable, Comparable<Segment> {

    private static final Logger LOG = LoggerFactory.getLogger(Segment.class);
    private final long id;
    private final long segmentSize;

    private final ReentrantLock lock = new ReentrantLock();
    private final Condition readable = lock.newCondition();
    private final AtomicBoolean finished = new AtomicBoolean(false);
    private final BlockWriter.BlockFlushHandler blockFlushHandler;
    private final AtomicBoolean closed = new AtomicBoolean();
    protected final AtomicLong position = new AtomicLong();
    protected final BlockWriter writer;
    protected final CompressionType compressionType;
    protected long cacheUsed = 0;

    public Segment(long id, BlockWriter writer, CompressionType compressionType, long segmentSize) {
        this.id = id;
        this.segmentSize = segmentSize;
        this.writer = writer;
        this.compressionType = compressionType;
        this.blockFlushHandler =
                (block) -> {
                    block.reusedBuf(
                            compressionType.compression(block.resultBuf(), block.reusedBuf()));
                    final int writeCount = block.reusedBuf().remaining();
                    writeFull(block.reusedBuf());
                    this.position.addAndGet(writeCount);
                    cacheUsed += writeCount;
                    readable.signalAll();
                };
    }

    /**
     * append bytes to log segment.
     *
     * @param data data
     * @param offset offset
     * @param length length
     * @return true if append success
     * @throws IOException IOException
     */
    public boolean append(byte[] data, int offset, int length) throws IOException {

        checkOpen();

        if (length <= 0) {
            return true;
        }

        final ReentrantLock lock = this.lock;
        lock.lock();
        try {
            if (isFinish() || position() > segmentSize) {
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
     * @param blockRecordsOffset offset in SegmentStorage
     * @param time block time
     * @param unit time unit
     * @return return null if eof or timeout, else return data
     * @throws IOException IOException
     * @throws InterruptedException InterruptedException
     */
    public BlockRecordsOffset readBlock(
            BlockRecordsOffset blockRecordsOffset, long time, TimeUnit unit)
            throws IOException, InterruptedException {

        checkOpen();

        if (!matchSegment(blockRecordsOffset)) {
            throw new IllegalStateException(
                    "segment match fail, want "
                            + blockRecordsOffset.segmentId()
                            + " real "
                            + segmentId());
        }

        final long readablePosition = position();
        final long offset = legalOffset(blockRecordsOffset.offset());

        if (offset < readablePosition) {
            return read(blockRecordsOffset, readablePosition);
        }
        final ReentrantLock lock = this.lock;
        lock.lock();
        try {
            if (offset >= position() && !finished.get()) {
                if (time == 0) {
                    readable.await();
                } else if (!readable.await(time, unit)) {
                    return blockRecordsOffset.reset();
                }
            } else if (offset >= position()) {
                return blockRecordsOffset.reset();
            }
        } finally {
            lock.unlock();
        }
        return read(blockRecordsOffset, position());
    }

    /**
     * read from block offset.
     *
     * @param blockRecordsOffset blockRecordsOffset
     * @param limitOffset limitOffset
     * @return BlockRecordsOffset BlockRecordsOffset
     * @throws IOException IOException
     */
    public BlockRecordsOffset read(BlockRecordsOffset blockRecordsOffset, long limitOffset)
            throws IOException {
        long nextOffset = legalOffset(blockRecordsOffset.offset());
        if (nextOffset >= limitOffset) {
            blockRecordsOffset.reset();
            return blockRecordsOffset;
        }
        final Block block = blockRecordsOffset.block();
        final ByteBuffer headBuf = IOUtils.reAllocate(block.reusedBuf(), BLOCK_HEAD_SIZE);

        readFull(headBuf, nextOffset);
        headBuf.flip();
        nextOffset += headBuf.remaining();

        if (nextOffset >= limitOffset) {
            LOG.warn(
                    "read block head over limit, next offset {}, limit offset {}",
                    nextOffset,
                    limitOffset);
            return skip2TargetOffset(blockRecordsOffset, limitOffset, headBuf);
        }

        final int dataLength = headBuf.getInt();
        if (dataLength <= 0) {
            LOG.warn("data length is less than 0, real value {}", dataLength);
            return skip2TargetOffset(blockRecordsOffset, limitOffset, headBuf);
        }

        final long finalNextOffset = dataLength + nextOffset;
        if (finalNextOffset > limitOffset) {
            LOG.warn(
                    "read block body over limit, next offset {}, limit offset {}",
                    finalNextOffset,
                    limitOffset);
            return skip2TargetOffset(blockRecordsOffset, limitOffset, headBuf);
        }

        final ByteBuffer bodyBuf =
                IOUtils.reAllocate(block.reusedBuf(), dataLength << 1, dataLength);
        readFull(bodyBuf, nextOffset);
        bodyBuf.flip();

        final BlockReader bufferReader =
                new BlockReader(
                        compressionType.decompression(bodyBuf, block.resultBuf()),
                        bodyBuf,
                        dataLength + BLOCK_HEAD_SIZE);
        return new BlockRecordsOffset(
                blockRecordsOffset.segmentId(), finalNextOffset, bufferReader);
    }

    /**
     * check segment whether open.
     *
     * @throws IOException IOException
     */
    private void checkOpen() throws IOException {
        if (closed.get()) {
            throw new TributaryIOException("segments storage is close, segment id = " + id);
        }
    }

    /**
     * check recordsOffset whether in this segment.
     *
     * @param recordsOffset recordsOffset
     * @return boolean match
     */
    public final boolean matchSegment(RecordsOffset recordsOffset) {
        return recordsOffset != null && this.segmentId() == recordsOffset.segmentId();
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
     * @param force if force, block data will flush to page cache first.
     * @throws IOException IOException
     */
    public void flush(boolean force) throws IOException {

        if (isFinish() || cacheUsed == 0 && !force) {
            return;
        }
        final ReentrantLock lock = this.lock;
        lock.lock();
        try {
            if (isFinish() || cacheUsed == 0 && !force) {
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
    public synchronized void finish() throws IOException {

        if (isFinish()) {
            return;
        }
        final ReentrantLock lock = this.lock;
        lock.lock();
        try {
            if (isFinish()) {
                return;
            }
            flush();
            finished.set(true);
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
    public boolean isFinish() {
        return finished.get();
    }

    @Override
    public synchronized void close() throws IOException {
        if (closed.compareAndSet(false, true)) {
            try {
                finish();
            } finally {
                closeCallback();
            }
        }
    }

    /** close callback. */
    public abstract void closeCallback() throws IOException;

    @Override
    public int compareTo(Segment o) {
        return Long.compare(this.segmentId(), o.segmentId());
    }

    /**
     * estimate lag, the lag not contains data in block.
     *
     * @param recordsOffset recordsOffset
     * @return long lag
     */
    public final long lag(RecordsOffset recordsOffset) {

        final long offset =
                recordsOffset == null ? legalOffset(0) : legalOffset(recordsOffset.offset());
        if (recordsOffset == null
                || recordsOffset.segmentId() == -1
                || matchSegment(recordsOffset)) {
            final long lag = position() - offset;
            return lag < 0 ? 0 : lag;
        } else {
            final long segmentIdDelta = this.segmentId() - recordsOffset.segmentId();
            if (segmentIdDelta < 0) {
                return 0;
            }
            final long segmentLag = (segmentIdDelta - 1) * segmentSize;
            // may segment size adjust, this lag is only an estimation
            final long lagInSegment = position() + (segmentSize - offset);
            return segmentLag + lagInSegment;
        }
    }

    /**
     * get compression type.
     *
     * @return CompressionType
     */
    @Override
    public CompressionType compressionType() {
        return compressionType;
    }

    /**
     * return page cache used count.
     *
     * @return long
     */
    public final long cacheUsed() {
        return cacheUsed;
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
     * @param blockRecordsOffset blockRecordsOffset
     * @param newOffset newOffset
     * @param reusedBuf reusedBuf
     * @return BlockRecordsOffset
     */
    public static BlockRecordsOffset skip2TargetOffset(
            BlockRecordsOffset blockRecordsOffset, long newOffset, ByteBuffer reusedBuf) {
        blockRecordsOffset.block().reset().reusedBuf(reusedBuf);
        return blockRecordsOffset.skip2TargetOffset(newOffset);
    }
}
