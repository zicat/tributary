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
import java.nio.charset.StandardCharsets;
import java.util.Objects;

/**
 * GroupOffset.
 *
 * <p>Each block in {@link Channel} has a unique GroupOffset.
 *
 * <p>GroupOffset can be stored and fetch by {@link Channel} using group id.
 *
 * <p>GroupOffset is composed of segmentId(long) and offset(long) and group id(string). It is
 * important to control the size of segment. If the size is too small will cause {@link FileChannel}
 * put records performance poor.If the size is to large will cause {@link FileChannel} clear expired
 * segment not timely.
 */
public class GroupOffset extends Offset {

    protected final String groupId;
    protected final byte[] groupBs;
    private static final int RECORD_LENGTH = 20;

    public GroupOffset(long segmentId, long offset, String groupId) {

        super(segmentId, offset);
        if (groupId == null) {
            throw new IllegalArgumentException("group id not null");
        }
        this.groupId = groupId;
        this.groupBs = groupId.getBytes(StandardCharsets.UTF_8);
    }

    /**
     * group id.
     *
     * @return string group id
     */
    public String groupId() {
        return groupId;
    }

    public int size() {
        return RECORD_LENGTH + groupBs.length;
    }

    /**
     * put segment id offset to byte buffer.
     *
     * @param byteBuffer byte buffer
     */
    public void fillBuffer(ByteBuffer byteBuffer) {
        byteBuffer.putLong(segmentId);
        byteBuffer.putLong(offset);
        byteBuffer.putInt(groupBs.length);
        byteBuffer.put(groupBs);
    }

    /**
     * create record offset from byte buffer.
     *
     * @param byteBuffer byte buffer
     * @return RecordOffset
     */
    public static GroupOffset parserByteBuffer(ByteBuffer byteBuffer) {
        final long segmentId = byteBuffer.getLong();
        final long offset = byteBuffer.getLong();
        final byte[] groupByte = new byte[byteBuffer.getInt()];
        byteBuffer.get(groupByte);
        return new GroupOffset(segmentId, offset, new String(groupByte, StandardCharsets.UTF_8));
    }

    /**
     * skip to the beginning of next segment .
     *
     * @return RecordOffset
     */
    public GroupOffset skipNextSegmentHead() {
        return skip2TargetHead(segmentId() + 1);
    }

    /**
     * skip to target record offset.
     *
     * @param segmentId segmentId
     * @return RecordOffset
     */
    public GroupOffset skip2TargetHead(long segmentId) {
        return skip2Target(segmentId, 0, groupId);
    }

    /**
     * skip to target.
     *
     * @param groupOffset groupOffset
     * @return new recordOffset
     */
    public GroupOffset skip2Target(GroupOffset groupOffset) {
        return skip2Target(groupOffset.segmentId(), groupOffset.offset(), groupOffset.groupId());
    }

    /**
     * skip to target.
     *
     * @param segmentId segmentId
     * @param offset offset
     * @param groupId groupId
     * @return RecordOffset
     */
    public GroupOffset skip2Target(long segmentId, long offset, String groupId) {
        return new GroupOffset(segmentId, offset, groupId);
    }

    /**
     * skip to target offset.
     *
     * @param newOffset newOffset
     * @return GroupOffset
     */
    public GroupOffset skip2TargetOffset(long newOffset) {
        return new GroupOffset(segmentId, newOffset, groupId);
    }

    @Override
    public String toString() {
        return "[" + "segment=" + segmentId + ", offset=" + offset + ", groupId=" + groupId + ']';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof GroupOffset)) {
            return false;
        }
        final GroupOffset that = (GroupOffset) o;
        return segmentId == that.segmentId
                && offset == that.offset
                && Objects.equals(groupId, that.groupId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(segmentId, offset, groupId);
    }

    /**
     * cast offset to group offset.
     *
     * @param offset offset
     * @param groupId groupId
     * @return GroupOffset
     */
    public static GroupOffset cast(Offset offset, String groupId) {
        if (offset instanceof GroupOffset) {
            final GroupOffset groupOffset = (GroupOffset) offset;
            if (!groupOffset.groupId.equals(groupId)) {
                throw new IllegalArgumentException(
                        "group id match fail, expected "
                                + groupOffset.groupId
                                + ", real "
                                + groupId);
            }
            return groupOffset;
        }
        return new GroupOffset(offset.segmentId, offset.offset, groupId);
    }
}
