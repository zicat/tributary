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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zicat.tributary.channel.CompressionType;
import org.zicat.tributary.channel.RecordsResultSet;
import org.zicat.tributary.common.IOUtils;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import static org.zicat.tributary.channel.file.SegmentUtil.BLOCK_HEAD_SIZE;
import static org.zicat.tributary.channel.file.SegmentUtil.legalOffset;

/**
 * A tool which fill block from file channel to {@link RecordsResultSet} by
 * readChannel @NotThreadSafe
 *
 * <p>Read logic should be adjusted when {@link BlockWriter} be adjusted.
 */
public final class FileBlockReaderUtil {

    private static final Logger LOG = LoggerFactory.getLogger(FileBlockReaderUtil.class);

    /**
     * skip to target offset with empty block.
     *
     * @param blockRecordsOffset blockRecordsOffset
     * @param newOffset newOffset
     * @param reusedBuf reusedBuf
     * @return BlockRecordsOffset
     */
    private static BlockRecordsOffset skip2TargetOffset(
            BlockRecordsOffset blockRecordsOffset, long newOffset, ByteBuffer reusedBuf) {
        blockRecordsOffset.block().reset().reusedBuf(reusedBuf);
        return blockRecordsOffset.skip2TargetOffset(newOffset);
    }

    /**
     * read from file channel with compression type.
     *
     * <p>note: limitOffset must be the start offset of one block, else will cause read
     * unpredictable data, the method will return empty ResultSet and offset point to {@param
     * limitOffset}
     *
     * @param blockRecordsOffset blockRecordsOffset
     * @param fileChannel fileChannel
     * @param compressionType compressionType
     * @param limitOffset limitOffset
     * @return new block file offset.
     * @throws IOException IOException
     */
    public static BlockRecordsOffset readChannel(
            BlockRecordsOffset blockRecordsOffset,
            FileChannel fileChannel,
            CompressionType compressionType,
            long limitOffset)
            throws IOException {

        long nextOffset = legalOffset(blockRecordsOffset.offset());
        if (nextOffset >= limitOffset) {
            blockRecordsOffset.reset();
            return blockRecordsOffset;
        }
        final Block block = blockRecordsOffset.block();
        final ByteBuffer headBuf = IOUtils.reAllocate(block.reusedBuf(), BLOCK_HEAD_SIZE);
        IOUtils.readFully(fileChannel, headBuf, nextOffset).flip();
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
        IOUtils.readFully(fileChannel, bodyBuf, nextOffset).flip();

        final BlockReader bufferReader =
                new BlockReader(
                        compressionType.decompression(bodyBuf, block.resultBuf()),
                        bodyBuf,
                        dataLength + BLOCK_HEAD_SIZE);
        return new BlockRecordsOffset(
                blockRecordsOffset.segmentId(), finalNextOffset, bufferReader);
    }
}
