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
import org.zicat.tributary.channel.CompressionType;
import org.zicat.tributary.channel.utils.IOUtils;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import static org.zicat.tributary.channel.file.SegmentUtil.SEGMENT_HEAD_SIZE;
import static org.zicat.tributary.channel.file.SegmentUtil.getNameById;
import static org.zicat.tributary.channel.utils.IOUtils.readFully;
import static org.zicat.tributary.channel.utils.IOUtils.writeFull;

/** LogSegmentBuilder for {@link Segment}. */
public class SegmentBuilder {

    private static final Logger LOG = LoggerFactory.getLogger(SegmentBuilder.class);

    private Long fileId;
    private long segmentSize = 2L * 1024L * 1024L * 1024L;
    private File dir;
    private String filePrefix = null;
    private CompressionType compressionType = CompressionType.NONE;

    /**
     * set file id.
     *
     * @param fileId fileId
     * @return LogSegmentBuilder
     */
    public SegmentBuilder fileId(long fileId) {
        this.fileId = fileId;
        return this;
    }

    /**
     * set compression type.
     *
     * @param compressionType compressionType
     * @return LogSegmentBuilder
     */
    public SegmentBuilder compressionType(CompressionType compressionType) {
        if (compressionType != null) {
            this.compressionType = compressionType;
        }
        return this;
    }

    /**
     * set file prefix.
     *
     * @param filePrefix filePrefix
     * @return LogSegmentBuilder
     */
    public SegmentBuilder filePrefix(String filePrefix) {
        this.filePrefix = filePrefix;
        return this;
    }

    /**
     * set segment size.
     *
     * @param segmentSize segmentSize
     * @return LogSegmentBuilder
     */
    public SegmentBuilder segmentSize(Long segmentSize) {
        if (segmentSize != null) {
            this.segmentSize = segmentSize;
        }
        return this;
    }

    /**
     * set dir.
     *
     * @param dir dir
     * @return LogSegmentBuilder
     */
    public SegmentBuilder dir(File dir) {
        this.dir = dir;
        return this;
    }

    /**
     * build log segment.
     *
     * @return LogSegment
     */
    public Segment build(BlockWriter blockWriter) {
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
            final ByteBuffer byteBuffer = ByteBuffer.allocate(SEGMENT_HEAD_SIZE);
            CompressionType realCompressType = compressionType;
            // read block size from segment head first
            if (position == 0) {
                byteBuffer.putInt(blockSize).put(compressionType.id());
                while (byteBuffer.hasRemaining()) {
                    byteBuffer.put((byte) 1);
                }
                byteBuffer.flip();
                position += writeFull(fileChannel, byteBuffer);
            } else {
                readFully(fileChannel, byteBuffer, 0).flip();
                blockSize = byteBuffer.getInt();
                realCompressType = CompressionType.getById(byteBuffer.get());
            }
            return create(file, fileChannel, blockWriter, blockSize, realCompressType, position);
        } catch (Exception e) {
            IOUtils.closeQuietly(fileChannel);
            IOUtils.closeQuietly(randomAccessFile);
            if (file.exists() && !file.delete()) {
                LOG.warn("delete file fail, file id {}", file.getPath());
            }
            throw new RuntimeException("create log segment error, file path " + file.getPath(), e);
        }
    }

    /**
     * create log segment.
     *
     * @param file file
     * @param channel channel
     * @param writer writer
     * @param blockSize blockSize
     * @param compressionType compressionType
     * @param position position
     * @return LogSegment
     */
    private Segment create(
            File file,
            FileChannel channel,
            BlockWriter writer,
            int blockSize,
            CompressionType compressionType,
            long position)
            throws IOException {
        channel.position(position);
        LOG.info(
                "create new segment fileId:{}, segmentSize:{}, blockSize:{}",
                file,
                segmentSize,
                blockSize);
        return new Segment(
                fileId,
                file,
                channel,
                writer.reAllocate(blockSize),
                segmentSize,
                compressionType,
                position);
    }
}
