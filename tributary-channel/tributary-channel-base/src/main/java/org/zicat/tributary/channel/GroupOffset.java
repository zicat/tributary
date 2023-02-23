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

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.nio.channels.FileChannel;
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
    private static final ObjectMapper MAPPER = new ObjectMapper();
    @JsonProperty protected final String groupId;

    public GroupOffset(
            @JsonProperty("segmentId") long segmentId,
            @JsonProperty("offset") long offset,
            @JsonProperty("groupId") String groupId) {
        super(segmentId, offset);
        if (groupId == null) {
            throw new IllegalArgumentException("group id not null");
        }
        this.groupId = groupId;
    }

    /**
     * group id.
     *
     * @return string group id
     */
    public String groupId() {
        return groupId;
    }

    /**
     * parser from json.
     *
     * @param json json
     * @return GroupOffset
     * @throws JsonProcessingException JsonProcessingException
     */
    public static GroupOffset fromJson(String json) throws JsonProcessingException {
        return MAPPER.readValue(json, GroupOffset.class);
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

    /**
     * cast as json.
     *
     * @return json string
     * @throws JsonProcessingException JsonProcessingException
     */
    public String toJson() throws JsonProcessingException {
        return MAPPER.writeValueAsString(this);
    }
}
