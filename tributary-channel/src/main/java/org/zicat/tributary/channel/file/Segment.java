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

import org.zicat.tributary.channel.BlockRecordsOffset;
import org.zicat.tributary.channel.BlockWriter;
import org.zicat.tributary.channel.CompressionType;
import org.zicat.tributary.channel.RecordsOffset;
import org.zicat.tributary.channel.utils.IOUtils;
import org.zicat.tributary.channel.utils.TributaryChannelException;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import static org.zicat.tributary.channel.file.FileBlockReaderUtil.readChannel;
import static org.zicat.tributary.channel.file.SegmentUtil.SEGMENT_HEAD_SIZE;
import static org.zicat.tributary.channel.file.SegmentUtil.legalOffset;

/**
 * A LogSegment instance is the represent of a file with file id.
 *
 * <p>The life cycle of a LogSegment instance include create(construct function),
 * writeable/readable, readonly(invoke finish method), closed(invoke close method), delete(invoke
 * delete method)
 *
 * <p>All public methods are @ThreadSafe
 *
 * <p>struct: doc/picture/segment.png
 */
public final class Segment implements Closeable, Comparable<Segment> {

    private final long id;
    private final File file;
    private final FileChannel fileChannel;
    private final long segmentSize;
    private final CompressionType compressionType;

    private final ReentrantLock lock = new ReentrantLock();
    private final Condition readable = lock.newCondition();
    private final AtomicLong readablePosition = new AtomicLong();
    private final AtomicBoolean finished = new AtomicBoolean(false);
    private final BlockWriter writer;
    private final BlockWriter.ClearHandler clearHandler;
    private long pageCacheUsed = 0;

    public Segment(
            long id,
            File file,
            FileChannel fileChannel,
            BlockWriter writer,
            long segmentSize,
            CompressionType compressionType,
            long position) {
        if (segmentSize - SEGMENT_HEAD_SIZE < writer.capacity()) {
            throw new IllegalArgumentException(
                    "segment size must over block size, segment size = "
                            + segmentSize
                            + ",block size = "
                            + writer.capacity());
        }
        this.id = id;
        this.file = file;
        this.fileChannel = fileChannel;
        this.segmentSize = segmentSize;
        this.compressionType = compressionType;
        this.writer = writer;
        this.readablePosition.set(position);
        this.clearHandler =
                (block) -> {
                    final int readableCount =
                            IOUtils.writeFull(fileChannel, compressionType.compression(block));
                    readablePosition.addAndGet(readableCount);
                    pageCacheUsed += readableCount;
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
            if (isFinish() || readablePosition() > segmentSize) {
                return false;
            }

            if (writer.put(data, offset, length)) {
                return true;
            }

            /* writer is full, start to flush channel and clean writer,
             * then try to put data to writer again.
             */
            writer.clear(clearHandler);
            if (writer.put(data, offset, length)) {
                return true;
            }

            /* data length is over writer.size,
             * wrap new writer which match data length and flush channel directly
             */
            BlockWriter.wrap(data, offset, length).clear(clearHandler);
            return true;
        } finally {
            lock.unlock();
        }
    }

    /**
     * blocking read data from file channel.
     *
     * @param blockRecordsOffset offset in file
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
                            + fileId());
        }

        final long readablePosition = readablePosition();
        final long offset = legalOffset(blockRecordsOffset.offset());

        if (offset < readablePosition) {
            return readChannel(blockRecordsOffset, fileChannel, compressionType, readablePosition);
        }
        final ReentrantLock lock = this.lock;
        lock.lock();
        try {
            if (offset >= readablePosition() && !finished.get()) {
                if (time == 0) {
                    readable.await();
                } else if (!readable.await(time, unit)) {
                    return blockRecordsOffset.reset();
                }
            } else if (offset >= readablePosition()) {
                return blockRecordsOffset.reset();
            }
        } finally {
            lock.unlock();
        }
        return readChannel(blockRecordsOffset, fileChannel, compressionType, readablePosition());
    }

    /**
     * check file channel whether open.
     *
     * @throws IOException IOException
     */
    private void checkOpen() throws IOException {
        if (!fileChannel.isOpen()) {
            throw new TributaryChannelException("file is close, file path = " + filePath());
        }
    }

    /**
     * check recordsOffset whether in this segment.
     *
     * @param recordsOffset recordsOffset
     * @return boolean match
     */
    public final boolean matchSegment(RecordsOffset recordsOffset) {
        return recordsOffset != null && this.fileId() == recordsOffset.segmentId();
    }

    /**
     * return page cache used count.
     *
     * @return long
     */
    public final long pageCacheUsed() {
        return pageCacheUsed;
    }

    /**
     * file id.
     *
     * @return fileId
     */
    public final long fileId() {
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

        if (isFinish() || pageCacheUsed == 0 && !force) {
            return;
        }
        final ReentrantLock lock = this.lock;
        lock.lock();
        try {
            if (isFinish() || pageCacheUsed == 0 && !force) {
                return;
            }
            if (!writer.isEmpty() && force) {
                writer.clear(clearHandler);
            }
            fileChannel.force(force);
            pageCacheUsed = 0;
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

    /** delete segment. */
    public synchronized boolean delete() {
        IOUtils.closeQuietly(this);
        return file.delete();
    }

    @Override
    public synchronized void close() throws IOException {
        if (fileChannel.isOpen()) {
            try {
                finish();
            } finally {
                IOUtils.closeQuietly(fileChannel);
            }
        }
    }

    @Override
    public int compareTo(Segment o) {
        return Long.compare(this.fileId(), o.fileId());
    }

    /**
     * estimate lag, the lag not contains data in block.
     *
     * @param recordsOffset recordsOffset
     * @return long lag
     */
    public final long lag(RecordsOffset recordsOffset) {

        final long offset =
                recordsOffset == null || recordsOffset.offset() < SEGMENT_HEAD_SIZE
                        ? SEGMENT_HEAD_SIZE
                        : recordsOffset.offset();
        if (recordsOffset == null
                || recordsOffset.segmentId() == -1
                || matchSegment(recordsOffset)) {
            final long lag = readablePosition() - offset;
            return lag < 0 ? 0 : lag;
        } else {
            final long fileIdDelta = this.fileId() - recordsOffset.segmentId();
            if (fileIdDelta < 0) {
                return 0;
            }
            final long segmentLag = (fileIdDelta - 1) * segmentSize;
            // may segment size adjust, this lag is only an estimation
            final long lagInSegment = readablePosition() + (segmentSize - offset);
            return segmentLag + lagInSegment;
        }
    }

    /**
     * get readable position.
     *
     * @return long
     */
    public final long readablePosition() {
        return readablePosition.get();
    }

    /**
     * file path.
     *
     * @return string
     */
    public final String filePath() {
        return file.getPath();
    }

    /**
     * get compression type.
     *
     * @return CompressionType
     */
    public final CompressionType compressionType() {
        return compressionType;
    }

    /**
     * blockSize.
     *
     * @return int size
     */
    public final int blockSize() {
        return writer.capacity();
    }
}
