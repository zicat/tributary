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

package org.zicat.tributary.queue;

import java.nio.ByteBuffer;

import static org.zicat.tributary.queue.utils.VIntUtil.readVInt;

/**
 * BufferRecordsResultSet.
 *
 * <p>BufferRecordsResultSet point to next records offset, and get all buffer records with iterator.
 */
public final class BufferRecordsResultSet extends RecordsOffset implements RecordsResultSet {

    protected final ByteBuffer headBuf;
    protected final ByteBuffer bodyBuf;
    protected final ByteBuffer resultBuf;
    protected final long readBytes;
    protected final boolean isEmpty;

    volatile byte[] nextData;

    BufferRecordsResultSet(RecordsOffset recordsOffset) {
        this(recordsOffset.segmentId(), recordsOffset.offset());
    }

    BufferRecordsResultSet(long segmentId, long offset) {
        this(segmentId, offset, null, 0);
    }

    BufferRecordsResultSet(long segmentId, long offset, ByteBuffer resultBuf, long readBytes) {
        this(segmentId, offset, null, null, resultBuf, readBytes);
    }

    public BufferRecordsResultSet(
            long segmentId,
            long offset,
            ByteBuffer headBuf,
            ByteBuffer bodyBuf,
            ByteBuffer resultBuf,
            long readBytes) {
        super(segmentId, offset);
        this.headBuf = headBuf;
        this.bodyBuf = bodyBuf;
        this.resultBuf = resultBuf;
        this.readBytes = readBytes;
        next();
        isEmpty = !hasNext();
    }

    /**
     * read next value.
     *
     * @return byte[]
     */
    private byte[] readNext() {
        if (resultBuf == null || !resultBuf.hasRemaining()) {
            return null;
        }
        int length = readVInt(resultBuf);
        byte[] bs = new byte[length];
        resultBuf.get(bs);
        return bs;
    }

    /**
     * create empty log result set.
     *
     * @return BufferRecordsResultSet
     */
    public final BufferRecordsResultSet empty() {
        if (resultBuf != null && resultBuf.hasRemaining()) {
            resultBuf.clear().flip();
        }
        return new BufferRecordsResultSet(segmentId, offset, headBuf, bodyBuf, resultBuf, 0);
    }

    /**
     * cast RecordsOffset as BufferReader.
     *
     * @param recordsOffset recordsOffset
     * @return recordsOffset
     */
    public static BufferRecordsResultSet cast(RecordsOffset recordsOffset) {
        if (recordsOffset instanceof BufferRecordsResultSet) {
            return (BufferRecordsResultSet) recordsOffset;
        }
        return new BufferRecordsResultSet(recordsOffset);
    }

    /**
     * create buffer by file id.
     *
     * @param segmentId segmentId
     * @return BufferReader
     */
    public static BufferRecordsResultSet cast(long segmentId) {
        return new BufferRecordsResultSet(segmentId, 0);
    }

    @Override
    public BufferRecordsResultSet skipNextSegmentHead() {
        return skip2TargetHead(segmentId() + 1);
    }

    @Override
    public BufferRecordsResultSet skip2TargetHead(long segmentId) {
        return skip2Target(segmentId, 0);
    }

    @Override
    public BufferRecordsResultSet skip2Target(RecordsOffset recordsOffset) {
        return skip2Target(recordsOffset.segmentId(), recordsOffset.offset());
    }

    @Override
    public BufferRecordsResultSet skip2Target(long segmentId, long offset) {
        return new BufferRecordsResultSet(segmentId, offset, headBuf, bodyBuf, resultBuf, 0);
    }

    @Override
    public RecordsOffset nexRecordsOffset() {
        return this;
    }

    @Override
    public final boolean hasNext() {
        return nextData != null;
    }

    @Override
    public final byte[] next() {
        final byte[] result = this.nextData;
        this.nextData = readNext();
        return result;
    }

    @Override
    public final long readBytes() {
        return readBytes;
    }

    @Override
    public final boolean isEmpty() {
        return isEmpty;
    }

    /**
     * get headBuf.
     *
     * @return headBuf
     */
    public final ByteBuffer headBuf() {
        return headBuf;
    }

    /**
     * get bodyBuf.
     *
     * @return bodyBuf
     */
    public final ByteBuffer bodyBuf() {
        return bodyBuf;
    }

    /**
     * get resultBuf.
     *
     * @return resultBuf
     */
    public final ByteBuffer resultBuf() {
        return resultBuf;
    }
}
