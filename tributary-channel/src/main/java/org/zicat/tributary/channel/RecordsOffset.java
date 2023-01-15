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

import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Objects;

/**
 * RecordsOffset.
 *
 * <p>Each block in {@link Channel} has a unique RecordsOffset.
 *
 * <p>RecordsOffset can be stored and fetch by {@link Channel} using group id.
 *
 * <p>RecordsOffset is composed of segmentId(long) and offset(long). It is important to control the
 * size of segment. If the size is too small will cause {@link FileChannel} put records performance
 * poor.If the size is to large will cause {@link FileChannel} clear expired segment not timely.
 */
public class RecordsOffset implements Comparable<RecordsOffset> {

    protected final long segmentId;
    protected final long offset;

    public RecordsOffset(long segmentId, long offset) {
        this.segmentId = segmentId;
        this.offset = offset;
    }

    /**
     * get segment id.
     *
     * @return long segment id
     */
    public long segmentId() {
        return segmentId;
    }

    /**
     * get offset.
     *
     * @return long offset
     */
    public long offset() {
        return offset;
    }

    /**
     * put segment id offset to byte buffer.
     *
     * @param byteBuffer byte buffer
     */
    public void fillBuffer(ByteBuffer byteBuffer) {
        byteBuffer.putLong(segmentId);
        byteBuffer.putLong(offset);
        byteBuffer.flip();
    }

    /**
     * create record offset from byte buffer.
     *
     * @param byteBuffer byte buffer
     * @return RecordOffset
     */
    public static RecordsOffset parserByteBuffer(ByteBuffer byteBuffer) {
        return new RecordsOffset(byteBuffer.getLong(), byteBuffer.getLong());
    }

    /**
     * skip to the beginning of next segment .
     *
     * @return RecordOffset
     */
    public RecordsOffset skipNextSegmentHead() {
        return skip2TargetHead(segmentId() + 1);
    }

    /**
     * skip to target record offset.
     *
     * @param segmentId segmentId
     * @return RecordOffset
     */
    public RecordsOffset skip2TargetHead(long segmentId) {
        return skip2Target(segmentId, 0);
    }

    /**
     * skip to target.
     *
     * @param recordsOffset recordOffset
     * @return new recordOffset
     */
    public RecordsOffset skip2Target(RecordsOffset recordsOffset) {
        return skip2Target(recordsOffset.segmentId(), recordsOffset.offset());
    }

    /**
     * skip to target.
     *
     * @param segmentId segmentId
     * @param offset offset
     * @return RecordOffset
     */
    public RecordsOffset skip2Target(long segmentId, long offset) {
        return new RecordsOffset(segmentId, offset);
    }

    @Override
    public int compareTo(RecordsOffset o) {
        if (this.segmentId == o.segmentId) {
            return Long.compare(this.offset, o.offset);
        } else {
            return Long.compare(this.segmentId, o.segmentId);
        }
    }

    /**
     * compare min offset.
     *
     * @param offset1 offset1
     * @param offset2 offset2
     * @return min
     */
    public static RecordsOffset min(RecordsOffset offset1, RecordsOffset offset2) {
        if (offset1 == null || offset2 == null) {
            return null;
        }
        return offset1.compareTo(offset2) > 0 ? offset2 : offset1;
    }

    /**
     * compare max offset.
     *
     * @param offset1 offset1
     * @param offset2 offset2
     * @return min
     */
    public static RecordsOffset max(RecordsOffset offset1, RecordsOffset offset2) {
        if (offset1 == null) {
            return offset2;
        }
        if (offset2 == null) {
            return offset1;
        }
        return offset1.compareTo(offset2) > 0 ? offset1 : offset2;
    }

    @Override
    public String toString() {
        return "[" + "segment=" + segmentId + ", offset=" + offset + ']';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final RecordsOffset that = (RecordsOffset) o;
        return segmentId == that.segmentId && offset == that.offset;
    }

    @Override
    public int hashCode() {
        return Objects.hash(segmentId, offset);
    }

    /**
     * create first record offset.
     *
     * @return RecordOffset
     */
    public static RecordsOffset startRecordOffset() {
        return new RecordsOffset(0, 0);
    }
}
