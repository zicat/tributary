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

package org.zicat.tributary.channel.memory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zicat.tributary.channel.BlockWriter;
import org.zicat.tributary.channel.ChannelBlockCache;
import org.zicat.tributary.channel.CompressionType;
import org.zicat.tributary.channel.Segment;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/** MemorySegment. */
public class MemorySegment extends Segment {

    private static final Logger LOG = LoggerFactory.getLogger(MemorySegment.class);
    private final List<ByteBuffer> chunkChain = new ArrayList<>();
    private volatile ByteBuffer currentChunk;
    private final int chunkSize;

    public MemorySegment(
            long id,
            BlockWriter writer,
            CompressionType compressionType,
            long segmentSize,
            ChannelBlockCache bCache) {
        super(id, writer, compressionType, segmentSize, 0, bCache);
        this.chunkSize = (int) DirectMemoryPool.chunkSize().getBytes();
        newChunk();
    }

    @Override
    public long legalOffset(long offset) {
        return offset;
    }

    /** new chunk. */
    private void newChunk() {
        final ByteBuffer chunk = DirectMemoryPool.borrowChunk();
        chunkChain.add(chunk);
        currentChunk = chunk;
    }

    @Override
    public void writeFull(ByteBuffer byteBuffer) {
        do {
            final int currentLimit = byteBuffer.position() + currentChunk.remaining();
            // current chunk have sufficient capacity.
            if (byteBuffer.limit() <= currentLimit) {
                currentChunk.put(byteBuffer);
                return;
            }
            final int originLimit = byteBuffer.limit();
            byteBuffer.limit(currentLimit);
            try {
                currentChunk.put(byteBuffer);
                newChunk();
            } finally {
                byteBuffer.limit(originLimit);
            }
        } while (byteBuffer.hasRemaining());
    }

    @Override
    public void readFull(ByteBuffer buffer, long offset) {
        while (buffer.hasRemaining()) {
            offset += read(buffer, offset);
        }
    }

    /**
     * only read from one chunk, return real read count.
     *
     * @param buffer buffer
     * @param offset offset
     * @return real count
     */
    private int read(ByteBuffer buffer, long offset) {
        final ByteBuffer readChunk = getReadChunk(offset);
        final int newLimit = readChunk.position() + buffer.remaining();
        if (newLimit <= readChunk.limit()) {
            readChunk.limit(newLimit);
        }
        final int result = readChunk.remaining();
        buffer.put(readChunk);
        return result;
    }

    /**
     * get read chunk by offset.
     *
     * @param offset offset
     * @return read chunk
     */
    private ByteBuffer getReadChunk(long offset) {
        final ByteBuffer chunk = chunkChain.get((int) (offset / chunkSize));
        final int position = (int) (offset % chunkSize);
        final ByteBuffer readChunk = chunk.duplicate();
        readChunk.position(position);
        return readChunk;
    }

    @Override
    public void persist(boolean force) {}

    @Override
    public void recycle() {
        try {
            super.recycle();
        } finally {
            for (ByteBuffer byteBuffer : chunkChain) {
                DirectMemoryPool.returnChunk(byteBuffer);
            }
            chunkChain.clear();
            currentChunk = null;
            LOG.info("recycled memory segment: {}", segmentId());
        }
    }
}
