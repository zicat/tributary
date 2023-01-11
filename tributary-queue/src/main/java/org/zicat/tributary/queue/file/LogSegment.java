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

import org.zicat.tributary.queue.BufferRecordsOffset;
import org.zicat.tributary.queue.BufferWriter;
import org.zicat.tributary.queue.CompressionType;
import org.zicat.tributary.queue.RecordsOffset;
import org.zicat.tributary.queue.utils.IOUtils;
import org.zicat.tributary.queue.utils.TributaryQueueException;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import static org.zicat.tributary.queue.file.LogSegmentUtil.SEGMENT_HEAD_SIZE;

/**
 * LogSegment.
 *
 * <p>All public methods are @ThreadSafe
 *
 * <p>struct: doc/picture/segment.png
 */
public final class LogSegment implements Closeable, Comparable<LogSegment> {

    private final long id;
    private final File file;
    private final FileChannel fileChannel;
    private final long segmentSize;
    private final CompressionType compressionType;

    private final ReentrantLock lock = new ReentrantLock();
    private final Condition readable = lock.newCondition();
    private final AtomicLong readablePosition = new AtomicLong();
    private final AtomicBoolean finished = new AtomicBoolean(false);
    private final BufferWriter writer;
    private final BufferWriter.ClearHandler clearHandler;
    private long pageCacheUsed = 0;

    public LogSegment(
            long id,
            File file,
            FileChannel fileChannel,
            BufferWriter writer,
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
                (byteBuffer, reusedBuffer) -> {
                    final ByteBuffer compressedBuffer =
                            compressionType.compression(byteBuffer, reusedBuffer);
                    final int readableCount = IOUtils.writeFull(fileChannel, compressedBuffer);
                    readablePosition.addAndGet(readableCount);
                    pageCacheUsed += readableCount;
                    readable.signalAll();
                    return compressedBuffer;
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
            BufferWriter.wrap(data, offset, length).clear(clearHandler);
            return true;
        } finally {
            lock.unlock();
        }
    }

    /**
     * blocking read data from file channel.
     *
     * @param bufferRecordsOffset offset in file
     * @param time block time
     * @param unit time unit
     * @return return null if eof or timeout, else return data
     * @throws IOException IOException
     * @throws InterruptedException InterruptedException
     */
    public BufferRecordsOffset readBlock(
            BufferRecordsOffset bufferRecordsOffset, long time, TimeUnit unit)
            throws IOException, InterruptedException {

        checkOpen();

        if (!matchSegment(bufferRecordsOffset)) {
            throw new IllegalStateException(
                    "segment match fail, want "
                            + bufferRecordsOffset.segmentId()
                            + " real "
                            + fileId());
        }

        final FileBufferReader fileBufferReader = FileBufferReader.from(bufferRecordsOffset);
        final long readablePosition = readablePosition();
        if (!fileBufferReader.reach(readablePosition)) {
            return fileBufferReader.readChannel(fileChannel, compressionType, readablePosition);
        }
        final ReentrantLock lock = this.lock;
        lock.lock();
        try {
            if (fileBufferReader.reach(readablePosition()) && !finished.get()) {
                if (time == 0) {
                    readable.await();
                } else if (!readable.await(time, unit)) {
                    return bufferRecordsOffset.reset();
                }
            } else if (fileBufferReader.reach(readablePosition())) {
                return bufferRecordsOffset.reset();
            }
        } finally {
            lock.unlock();
        }
        return fileBufferReader.readChannel(fileChannel, compressionType, readablePosition());
    }

    /**
     * check file channel whether open.
     *
     * @throws IOException IOException
     */
    private void checkOpen() throws IOException {
        if (!fileChannel.isOpen()) {
            throw new TributaryQueueException("file is close, file path = " + filePath());
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
     * @param force if force, buffer data will flush to page cache first.
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
     * set log segment as not writeable, but read is allow.
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
    public int compareTo(LogSegment o) {
        return Long.compare(this.fileId(), o.fileId());
    }

    /**
     * estimate lag, the lag not contains data in buffer.
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
     * reusableBuffer.
     *
     * @return reusableBuffer
     */
    public final int blockSize() {
        return writer.capacity();
    }
}
