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
import static org.zicat.tributary.common.BytesUtils.toBytes;
import static org.zicat.tributary.common.IOUtils.reAllocate;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CountDownLatch;
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
    protected volatile CountDownLatchWitException inStorageLatch =
            new CountDownLatchWitException(1);
    protected final CompressionType compression;
    protected long cacheUsed = 0;
    protected long preFreshTime = System.currentTimeMillis();
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
            try {
                writeFull(block.reusedBuf());
            } catch (IOException e) {
                inStorageLatch.setException(e);
                throw e;
            } finally {
                inStorageLatch.countDown();
                inStorageLatch = new CountDownLatchWitException(1);
            }
            final long preOffset = position.getAndAdd(writeCount);
            if (buff != null) {
                bCache.put(id, preOffset, preOffset + writeCount, toBytes(buff));
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
     * @return AppendResult
     * @throws IOException IOException if block flush to SegmentStorage
     */
    public AppendResult append(byte[] data, int offset, int length) throws IOException {
        if (length <= 0) {
            return HAS_IN_STORAGE;
        }
        return append(ByteBuffer.wrap(data, offset, length));
    }

    public AppendResult append(String value) throws IOException {
        if (value == null || value.isEmpty()) {
            return HAS_IN_STORAGE;
        }
        final byte[] bs = value.getBytes(StandardCharsets.UTF_8);
        return append(bs, 0, bs.length);
    }

    /**
     * append bytes to log segment.
     *
     * <p>if segment is readonly or segment current size over {@link Segment#segmentSize} , return
     * false.
     *
     * @param byteBuffer byteBuffer
     * @return AppendResult
     * @throws IOException IOException if block flush to SegmentStorage
     */
    public AppendResult append(ByteBuffer byteBuffer) throws IOException {

        if (byteBuffer == null || byteBuffer.remaining() == 0) {
            return HAS_IN_STORAGE;
        }

        final ReentrantLock lock = this.lock;
        lock.lock();
        try {
            if (isReadonly() || position() > segmentSize) {
                return APPEND_FAIL;
            }

            if (writer.put(byteBuffer)) {
                return inBlock(inStorageLatch);
            }

            /* writer is full, start to flush channel and clean writer,
             * then try to put data to writer again.
             */
            writer.clear(blockFlushHandler);
            if (writer.put(byteBuffer)) {
                return inBlock(inStorageLatch);
            }

            /* data length is over writer.size,
             * wrap new writer which match data length and flush channel directly
             */
            BlockWriter.wrap(byteBuffer).clear(blockFlushHandler);
            return HAS_IN_STORAGE;
        } finally {
            lock.unlock();
        }
    }

    /**
     * only in block.
     *
     * @param inStorageLatch inStorageLatch
     * @return AppendResult
     */
    public static AppendResult inBlock(CountDownLatchWitException inStorageLatch) {
        return new AppendResult() {
            @Override
            public boolean appended() {
                return true;
            }

            @Override
            public boolean await2Storage(long timeout, TimeUnit unit)
                    throws InterruptedException, IOException {
                final boolean result = inStorageLatch.await(timeout, unit);
                if (inStorageLatch.exception() != null) {
                    throw inStorageLatch.exception();
                }
                return result;
            }
        };
    }

    private static final AppendResult HAS_IN_STORAGE =
            new AppendResult() {
                @Override
                public boolean appended() {
                    return true;
                }

                @Override
                public boolean await2Storage(long timeout, TimeUnit unit) {
                    return true;
                }
            };

    private static final AppendResult APPEND_FAIL =
            new AppendResult() {
                @Override
                public boolean appended() {
                    return false;
                }

                @Override
                public boolean await2Storage(long timeout, TimeUnit unit) {
                    throw new IllegalStateException("append fail, never call this method");
                }
            };

    /** AppendResult. */
    public interface AppendResult {

        /**
         * check if data append success.
         *
         * @return false if append data fail
         */
        boolean appended();

        /** await data 2 storage. */
        boolean await2Storage(long timeout, TimeUnit unit) throws InterruptedException, IOException;
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
     * @param blockReaderOffset offset in SegmentStorage
     * @param time block time
     * @param unit time unit
     * @return return null if eof or timeout, else return data
     * @throws IOException IOException when read storage
     * @throws InterruptedException InterruptedException
     */
    public BlockReaderOffset readBlock(
            BlockReaderOffset blockReaderOffset, long time, TimeUnit unit)
            throws IOException, InterruptedException {

        final long readablePosition = position();
        final long offset = legalOffset(blockReaderOffset.offset());
        if (offset < readablePosition) {
            return read(blockReaderOffset, readablePosition);
        }
        final ReentrantLock lock = this.lock;
        lock.lock();
        try {
            if (offset >= position() && !isReadonly()) {
                if (time == 0) {
                    readable.await();
                } else if (!readable.await(time, unit)) {
                    return blockReaderOffset.reset();
                }
            } else if (offset >= position()) {
                return blockReaderOffset.reset();
            }
        } finally {
            lock.unlock();
        }
        return read(blockReaderOffset, position());
    }

    /**
     * read from block offset.
     *
     * @param blockReaderOffset blockGroupOffset
     * @param limitOffset limitOffset
     * @throws IOException IOException
     */
    public BlockReaderOffset read(BlockReaderOffset blockReaderOffset, long limitOffset)
            throws IOException {

        blockReaderOffset = blockReaderOffset.skipOffset(legalOffset(blockReaderOffset.offset()));
        long offset = blockReaderOffset.offset();

        if (offset >= limitOffset) {
            blockReaderOffset.reset();
            return blockReaderOffset;
        }

        final BlockReaderOffset inCache;
        if (bCache != null && (inCache = bCache.find(blockReaderOffset)) != null) {
            return inCache;
        }

        final Block block = blockReaderOffset.blockReader();
        final ByteBuffer headBuf = reAllocate(block.reusedBuf(), BLOCK_HEAD_SIZE);

        readFull(headBuf, offset);
        headBuf.flip();
        offset += headBuf.remaining();

        if (offset >= limitOffset) {
            LOG.warn("read block head over limit, " + OFFSET_MESSAGE, offset, limitOffset);
            return skip2TargetOffset(blockReaderOffset, limitOffset, headBuf);
        }

        final int dataLength = headBuf.getInt();
        if (dataLength <= 0) {
            LOG.warn("data length is less than 0, real value {}", dataLength);
            return skip2TargetOffset(blockReaderOffset, limitOffset, headBuf);
        }

        final long finalOffset = dataLength + offset;
        if (finalOffset > limitOffset) {
            LOG.warn("read block body over limit, " + OFFSET_MESSAGE, finalOffset, limitOffset);
            return skip2TargetOffset(blockReaderOffset, limitOffset, headBuf);
        }

        final ByteBuffer reusedBuf = reAllocate(block.reusedBuf(), dataLength << 1, dataLength);
        readFull(reusedBuf, offset);
        reusedBuf.flip();
        final ByteBuffer resultBuf = compression.decompression(reusedBuf, block.resultBuf());
        final long readBytes = dataLength + BLOCK_HEAD_SIZE;
        final BlockReader blockReader = new BlockReader(resultBuf, reusedBuf, readBytes);
        return blockReaderOffset.newOffsetReader(finalOffset, blockReader);
    }

    /**
     * check offset whether in this segment.
     *
     * @param offset offset
     * @return boolean match
     */
    public final boolean match(Offset offset) {
        return offset != null && this.segmentId() == offset.segmentId();
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
            preFreshTime = System.currentTimeMillis();
        } finally {
            lock.unlock();
        }
    }

    /**
     * get flush idle milli.
     *
     * @return idle time
     */
    public long flushIdleMillis() {
        return System.currentTimeMillis() - preFreshTime;
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
            try {
                flush();
            } finally {
                readonly.set(true);
                readable.signalAll();
            }
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
    public void recycle() {
        if (!closed.get()) {
            throw new IllegalStateException("segment is not closed before recycle");
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
     * @param offset offset
     * @return long lag
     */
    public final long lag(Offset offset) {
        if (offset == null || offset.segmentId() < 0 || offset.segmentId() > this.segmentId()) {
            return 0L;
        }
        return match(offset) ? Math.max(0, position() - legalOffset(offset.offset())) : position();
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
     * @param blockReaderOffset blockGroupOffset
     * @param newOffset newOffset
     * @param reusedBuf reusedBuf
     * @return BlockGroupOffset
     */
    private static BlockReaderOffset skip2TargetOffset(
            BlockReaderOffset blockReaderOffset, long newOffset, ByteBuffer reusedBuf) {
        blockReaderOffset.blockReader().reset().reusedBuf(reusedBuf);
        return blockReaderOffset.skip2TargetOffset(newOffset);
    }

    /** CountDownLatchWitException. */
    public static class CountDownLatchWitException extends CountDownLatch {
        private volatile IOException exception;

        public CountDownLatchWitException(int count) {
            super(count);
        }

        public void setException(IOException e) {
            this.exception = e;
        }

        public IOException exception() {
            return exception;
        }
    }
}
