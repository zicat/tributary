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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zicat.tributary.queue.BufferRecordsOffset;
import org.zicat.tributary.queue.CompressionType;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import static org.zicat.tributary.queue.file.LogSegmentUtil.BLOCK_HEAD_SIZE;
import static org.zicat.tributary.queue.file.LogSegmentUtil.SEGMENT_HEAD_SIZE;
import static org.zicat.tributary.queue.utils.IOUtils.reAllocate;
import static org.zicat.tributary.queue.utils.IOUtils.readFully;

/**
 * A tool which fill block from file channel to {@link org.zicat.tributary.queue.RecordsResultSet} .
 *
 * <p>Read logic should be adjusted when {@link org.zicat.tributary.queue.BufferWriter} be adjusted.
 */
public final class FileBufferReader {

    private static final Logger LOG = LoggerFactory.getLogger(FileBufferReader.class);
    private final BufferRecordsOffset buffer;
    private final long legalOffset;

    private FileBufferReader(BufferRecordsOffset buffer) {
        this.buffer = buffer;
        this.legalOffset =
                buffer.offset() < SEGMENT_HEAD_SIZE ? SEGMENT_HEAD_SIZE : buffer.offset();
    }

    /**
     * cast bufferReader as FileBufferReader.
     *
     * @param bufferRecordsOffset bufferReader
     * @return FileBufferReader
     */
    public static FileBufferReader from(BufferRecordsOffset bufferRecordsOffset) {
        return new FileBufferReader(bufferRecordsOffset);
    }

    /**
     * check readable on position.
     *
     * @param position position
     * @return boolean
     */
    public final boolean reach(long position) {
        return legalOffset >= position;
    }

    /**
     * create empty BufferRecordsOffset.
     *
     * @param newOffset newOffset
     * @param newHeadBuf newHeadBuf
     * @return BufferRecordsOffset
     */
    private BufferRecordsOffset empty(long newOffset, ByteBuffer newHeadBuf) {
        return new BufferRecordsOffset(
                buffer.segmentId(), newOffset, newHeadBuf, buffer.bodyBuf(), buffer.resultBuf(), 0);
    }

    /**
     * read from file channel with compression type.
     *
     * @param fileChannel fileChannel
     * @param compressionType compressionType
     * @return new block file offset.
     * @throws IOException IOException
     */
    public final BufferRecordsOffset readChannel(
            FileChannel fileChannel, CompressionType compressionType, long limitOffset)
            throws IOException {

        long nextOffset = legalOffset;
        final ByteBuffer headBuf = reAllocate(buffer.headBuf(), BLOCK_HEAD_SIZE);
        readFully(fileChannel, headBuf, nextOffset).flip();
        nextOffset += headBuf.remaining();

        if (nextOffset >= limitOffset) {
            LOG.warn(
                    "read block head over limit, next offset {}, limit offset {}",
                    nextOffset,
                    limitOffset);
            return empty(limitOffset, headBuf);
        }

        final int limit = headBuf.getInt();
        if (limit <= 0) {
            LOG.warn("data length is less than 0, real value {}", limit);
            return empty(limitOffset, headBuf);
        }
        if (limit + nextOffset > limitOffset) {
            LOG.warn(
                    "read block body over limit, next offset {}, limit offset {}",
                    limit + nextOffset,
                    limitOffset);
            return empty(limitOffset, headBuf);
        }

        final ByteBuffer bodyBuf = reAllocate(buffer.bodyBuf(), limit << 1, limit);
        readFully(fileChannel, bodyBuf, nextOffset).flip();
        nextOffset += bodyBuf.remaining();
        final ByteBuffer resultBuf = compressionType.decompression(bodyBuf, buffer.resultBuf());
        return new BufferRecordsOffset(
                buffer.segmentId(),
                nextOffset,
                headBuf,
                bodyBuf,
                resultBuf,
                limit + BLOCK_HEAD_SIZE);
    }
}
