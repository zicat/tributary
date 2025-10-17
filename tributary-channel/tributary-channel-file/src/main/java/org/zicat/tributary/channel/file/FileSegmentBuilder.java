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
import org.zicat.tributary.channel.BlockWriter;
import org.zicat.tributary.channel.ChannelBlockCache;
import static org.zicat.tributary.channel.ChannelConfigOption.OPTION_COMPRESSION;
import static org.zicat.tributary.channel.ChannelConfigOption.OPTION_SEGMENT_SIZE;
import org.zicat.tributary.channel.CompressionType;
import static org.zicat.tributary.channel.file.FileSegmentUtil.FILE_SEGMENT_HEAD_SIZE;
import static org.zicat.tributary.channel.file.FileSegmentUtil.getNameById;
import org.zicat.tributary.common.util.IOUtils;
import org.zicat.tributary.common.exception.TributaryRuntimeException;

import java.io.File;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/** FileSegmentBuilder. */
public class FileSegmentBuilder {

    private static final Logger LOG = LoggerFactory.getLogger(FileSegmentBuilder.class);
    private Long fileId;
    private long segmentSize = OPTION_SEGMENT_SIZE.defaultValue().getBytes();
    private File dir;
    private String filePrefix = null;
    private CompressionType compressionType = OPTION_COMPRESSION.defaultValue();

    private ChannelBlockCache bCache;

    /**
     * set file id.
     *
     * @param fileId fileId
     * @return Builder
     */
    public FileSegmentBuilder fileId(long fileId) {
        this.fileId = fileId;
        return this;
    }

    /**
     * set compression type.
     *
     * @param compressionType compressionType
     * @return Builder
     */
    public FileSegmentBuilder compressionType(CompressionType compressionType) {
        if (compressionType != null) {
            this.compressionType = compressionType;
        }
        return this;
    }

    /**
     * set file prefix.
     *
     * @param filePrefix filePrefix
     * @return Builder
     */
    public FileSegmentBuilder filePrefix(String filePrefix) {
        this.filePrefix = filePrefix;
        return this;
    }

    /**
     * set segment size.
     *
     * @param segmentSize segmentSize
     * @return Builder
     */
    public FileSegmentBuilder segmentSize(Long segmentSize) {
        if (segmentSize != null) {
            this.segmentSize = segmentSize;
        }
        return this;
    }

    /**
     * set dir.
     *
     * @param dir dir
     * @return Builder
     */
    public FileSegmentBuilder dir(File dir) {
        this.dir = dir;
        return this;
    }

    public FileSegmentBuilder blockCache(ChannelBlockCache bCache) {
        this.bCache = bCache;
        return this;
    }

    /**
     * build log segment.
     *
     * @return FileSegment
     */
    public FileSegment build(BlockWriter blockWriter) {
        if (fileId == null) {
            throw new NullPointerException("segment file id is null");
        }
        if (dir == null) {
            throw new NullPointerException("segment dir is null");
        }
        final File file = new File(dir, getNameById(filePrefix, fileId));
        int blockSize = blockWriter.capacity();
        RandomAccessFile randomAccessFile = null;
        FileChannel fileChannel = null;
        try {
            randomAccessFile = new RandomAccessFile(file, "rw");
            fileChannel = randomAccessFile.getChannel();
            long position = fileChannel.size();
            final ByteBuffer byteBuffer = ByteBuffer.allocate(FILE_SEGMENT_HEAD_SIZE);
            CompressionType realCompressType = compressionType;
            // read block size from segment head first
            if (position == 0) {
                byteBuffer.putInt(blockSize).put(compressionType.id());
                while (byteBuffer.hasRemaining()) {
                    byteBuffer.put((byte) 1);
                }
                byteBuffer.flip();
                position += IOUtils.writeFull(fileChannel, byteBuffer);
            } else {
                IOUtils.readFully(fileChannel, byteBuffer, 0).flip();
                blockSize = byteBuffer.getInt();
                realCompressType = CompressionType.getById(byteBuffer.get());
            }
            fileChannel.position(position);
            return new FileSegment(
                    fileId,
                    blockWriter.reAllocate(blockSize),
                    realCompressType,
                    segmentSize,
                    position,
                    file,
                    fileChannel,
                    bCache);
        } catch (Exception e) {
            IOUtils.closeQuietly(fileChannel);
            IOUtils.closeQuietly(randomAccessFile);
            if (file.exists() && !file.delete()) {
                LOG.warn("delete file fail, file id {}", file.getPath());
            }
            throw new TributaryRuntimeException("create segment fail, path " + file, e);
        }
    }
}
