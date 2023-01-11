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

/** BufferRecordsOffset. */
public class BufferRecordsOffset extends RecordsOffset {

    protected final ByteBuffer headBuf;
    protected final ByteBuffer bodyBuf;
    protected final ByteBuffer resultBuf;
    protected final long readBytes;

    public BufferRecordsOffset(
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
    }

    private BufferRecordsOffset(long segmentId, long offset) {
        this(segmentId, offset, null, null, null, 0);
    }

    /**
     * create result set.
     *
     * @return RecordsResultSet
     */
    public RecordsResultSet toResultSet() {
        final BufferRecordsOffset nextRecordsOffset = this;
        return new RecordsResultSetImpl(nextRecordsOffset);
    }

    /**
     * read next value.
     *
     * @return byte[]
     */
    public byte[] readNext() {
        if (resultBuf == null || !resultBuf.hasRemaining()) {
            return null;
        }
        final int length = readVInt(resultBuf);
        final byte[] bs = new byte[length];
        resultBuf.get(bs);
        return bs;
    }

    /**
     * create buffer by segment id.
     *
     * @param segmentId segmentId
     * @return BufferReader
     */
    public static BufferRecordsOffset cast(long segmentId) {
        return cast(segmentId, 0);
    }

    /**
     * create buffer by segment id and offset.
     *
     * @param segmentId segmentId
     * @param offset offset
     * @return BufferReader
     */
    public static BufferRecordsOffset cast(long segmentId, long offset) {
        return new BufferRecordsOffset(segmentId, offset);
    }

    /**
     * cast RecordsOffset as BufferRecordsOffset.
     *
     * @param recordsOffset recordsOffset
     * @return BufferRecordsOffset
     */
    public static BufferRecordsOffset cast(RecordsOffset recordsOffset) {
        if (recordsOffset instanceof BufferRecordsOffset) {
            return (BufferRecordsOffset) recordsOffset;
        }
        return new BufferRecordsOffset(recordsOffset.segmentId, recordsOffset.offset);
    }

    /**
     * reset buffer and return new BufferRecord.
     *
     * @return BufferRecordsOffset
     */
    public final BufferRecordsOffset reset() {
        if (resultBuf != null && resultBuf.hasRemaining()) {
            resultBuf.clear().flip();
        }
        return new BufferRecordsOffset(segmentId, offset, headBuf, bodyBuf, resultBuf, 0);
    }

    @Override
    public BufferRecordsOffset skipNextSegmentHead() {
        return skip2TargetHead(segmentId() + 1);
    }

    @Override
    public BufferRecordsOffset skip2TargetHead(long segmentId) {
        return skip2Target(segmentId, 0);
    }

    @Override
    public BufferRecordsOffset skip2Target(RecordsOffset recordsOffset) {
        return skip2Target(recordsOffset.segmentId(), recordsOffset.offset());
    }

    @Override
    public BufferRecordsOffset skip2Target(long segmentId, long offset) {
        return new BufferRecordsOffset(segmentId, offset, headBuf, bodyBuf, resultBuf, 0);
    }

    /**
     * get head buf.
     *
     * @return ByteBuffer
     */
    public final ByteBuffer headBuf() {
        return headBuf;
    }

    /**
     * get body buf.
     *
     * @return ByteBuffer
     */
    public final ByteBuffer bodyBuf() {
        return bodyBuf;
    }

    /**
     * get result buf.
     *
     * @return ByteBuffer
     */
    public final ByteBuffer resultBuf() {
        return resultBuf;
    }

    /**
     * get read bytes.
     *
     * @return bytes
     */
    public final long readBytes() {
        return readBytes;
    }
}
