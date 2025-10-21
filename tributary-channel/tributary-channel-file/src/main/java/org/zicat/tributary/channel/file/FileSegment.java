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
import org.zicat.tributary.channel.CompressionType;
import org.zicat.tributary.channel.Segment;
import org.zicat.tributary.common.util.IOUtils;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.atomic.AtomicBoolean;

/** FileSegment storage data to file. */
public class FileSegment extends Segment {

    private static final Logger LOG = LoggerFactory.getLogger(FileSegment.class);
    private final File file;
    private final FileChannel fileChannel;
    private final AtomicBoolean closed = new AtomicBoolean();
    private final AtomicBoolean recycled = new AtomicBoolean();

    protected FileSegment(
            long id,
            BlockWriter writer,
            CompressionType compressionType,
            long segmentSize,
            long position,
            long await2StorageTimeout,
            File file,
            FileChannel fileChannel,
            ChannelBlockCache bCache) {
        super(id, writer, compressionType, segmentSize, position, await2StorageTimeout, bCache);
        this.file = file;
        this.fileChannel = fileChannel;
    }

    @Override
    public void writeFull(ByteBuffer byteBuffer) throws IOException {
        IOUtils.writeFull(fileChannel, byteBuffer);
    }

    @Override
    public void readFull(ByteBuffer byteBuffer, long offset) throws IOException {
        IOUtils.readFully(fileChannel, byteBuffer, offset);
    }

    @Override
    public void persist(boolean force) throws IOException {
        fileChannel.force(force);
    }

    @Override
    public void recycle() {
        super.recycle();
        if (recycled.compareAndSet(false, true)) {
            LOG.info("deleted {} {}", file.getPath(), file.delete() ? "success" : "fail");
        }
    }

    @Override
    protected long legalOffset(long offset) {
        return FileSegmentUtil.legalFileOffset(offset);
    }

    @Override
    public void close() throws IOException {
        try {
            super.close();
        } finally {
            if (closed.compareAndSet(false, true)) {
                IOUtils.closeQuietly(fileChannel);
            }
        }
    }
}
