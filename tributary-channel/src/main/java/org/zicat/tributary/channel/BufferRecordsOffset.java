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

package org.zicat.tributary.channel;

/** BufferRecordsOffset. */
public class BufferRecordsOffset extends RecordsOffset {

    protected final BufferReader bufferReader;

    public BufferRecordsOffset(long segmentId, long offset, BufferReader bufferReader) {
        super(segmentId, offset);
        this.bufferReader = bufferReader == null ? new BufferReader(null, null, 0) : bufferReader;
    }

    private BufferRecordsOffset(long segmentId, long offset) {
        this(segmentId, offset, null);
    }

    /**
     * create result set.
     *
     * @return RecordsResultSet
     */
    public RecordsResultSet toResultSet() {
        return new RecordsResultSetImpl();
    }

    /**
     * get buffer.
     *
     * @return Buffer
     */
    public Buffer buffer() {
        return bufferReader;
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
     * reset buffer.
     *
     * @return BufferRecordsOffset
     */
    public final BufferRecordsOffset reset() {
        bufferReader.reset();
        return this;
    }

    @Override
    public BufferRecordsOffset skip2TargetOffset(long newOffset) {
        return skip2Target(segmentId(), newOffset);
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
        return new BufferRecordsOffset(segmentId, offset, bufferReader);
    }

    /** RecordsResultSetImpl. */
    private class RecordsResultSetImpl implements RecordsResultSet {

        private byte[] nextData;

        RecordsResultSetImpl() {
            next();
        }

        @Override
        public final RecordsOffset nexRecordsOffset() {
            return BufferRecordsOffset.this;
        }

        @Override
        public final boolean hasNext() {
            return nextData != null;
        }

        @Override
        public final byte[] next() {
            final byte[] result = this.nextData;
            this.nextData = BufferRecordsOffset.this.bufferReader.readNext();
            return result;
        }

        @Override
        public final long readBytes() {
            return BufferRecordsOffset.this.bufferReader.readBytes();
        }
    }
}
