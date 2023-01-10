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
import org.zicat.tributary.queue.BufferRecordsResultSet;
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
    private final BufferRecordsResultSet resultSet;
    private final long readableOffset;

    private FileBufferReader(BufferRecordsResultSet resultSet) {
        this.resultSet = resultSet;
        this.readableOffset =
                resultSet.offset() < SEGMENT_HEAD_SIZE ? SEGMENT_HEAD_SIZE : resultSet.offset();
    }

    /**
     * cast bufferReader as FileBufferReader.
     *
     * @param bufferRecordsResultSet bufferReader
     * @return FileBufferReader
     */
    public static FileBufferReader from(BufferRecordsResultSet bufferRecordsResultSet) {
        return new FileBufferReader(bufferRecordsResultSet);
    }

    /**
     * check readable on position.
     *
     * @param maxPosition maxPosition
     * @return boolean
     */
    public final boolean readable(long maxPosition) {
        return readableOffset < maxPosition;
    }

    /**
     * read from file channel with compression type.
     *
     * @param fileChannel fileChannel
     * @param compressionType compressionType
     * @return new block file offset.
     * @throws IOException IOException
     */
    public final BufferRecordsResultSet readChannel(
            FileChannel fileChannel, CompressionType compressionType, long limitOffset)
            throws IOException {

        long nextOffset = readableOffset;
        final ByteBuffer headBuf = reAllocate(resultSet.headBuf(), BLOCK_HEAD_SIZE);
        readFully(fileChannel, headBuf, nextOffset).flip();
        nextOffset += headBuf.remaining();

        if (nextOffset >= limitOffset) {
            LOG.warn(
                    "read block head over limit, next offset {}, limit offset {}",
                    nextOffset,
                    limitOffset);
            return createSkippedRecordsResultSet(limitOffset, headBuf);
        }

        final int limit = headBuf.getInt();
        if (limit <= 0) {
            LOG.warn("data length is less than 0, real value {}", limit);
            return createSkippedRecordsResultSet(limitOffset, headBuf);
        }
        if (limit + nextOffset > limitOffset) {
            LOG.warn(
                    "read block body over limit, next offset {}, limit offset {}",
                    limit + nextOffset,
                    limitOffset);
            return createSkippedRecordsResultSet(limitOffset, headBuf);
        }

        final ByteBuffer bodyBuf = reAllocate(resultSet.bodyBuf(), limit << 1, limit);
        readFully(fileChannel, bodyBuf, nextOffset).flip();
        nextOffset += bodyBuf.remaining();

        final ByteBuffer resultBuf = compressionType.decompression(bodyBuf, resultSet.resultBuf());
        return new BufferRecordsResultSet(
                resultSet.segmentId(),
                nextOffset,
                headBuf,
                bodyBuf,
                resultBuf,
                limit + BLOCK_HEAD_SIZE);
    }

    /**
     * create skipped records result set with empty data.
     *
     * @param newOffset newOffset
     * @param newHeadBuf newHeadBuf
     * @return BufferRecordsResultSet
     */
    private BufferRecordsResultSet createSkippedRecordsResultSet(
            long newOffset, ByteBuffer newHeadBuf) {
        return new BufferRecordsResultSet(
                resultSet.segmentId(),
                newOffset,
                newHeadBuf,
                resultSet.bodyBuf(),
                resultSet.resultBuf(),
                0);
    }
}
