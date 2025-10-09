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

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

/** Offset. */
public class Offset implements Comparable<Offset> {

    public static final Offset UNINITIALIZED_OFFSET = new Offset(-1, 0);
    public static final Offset ZERO = new Offset(0, 0);

    @JsonProperty protected final long segmentId;
    @JsonProperty protected final long offset;

    private Offset() {
        this(0, 0L);
    }

    public Offset(long segmentId, long offset) {
        this.segmentId = segmentId;
        this.offset = offset;
    }

    public Offset(long segmentId) {
        this(segmentId, 0L);
    }

    /**
     * get segment id.
     *
     * @return long segment id
     */
    @JsonIgnore
    public long segmentId() {
        return segmentId;
    }

    /**
     * get offset.
     *
     * @return long offset
     */
    @JsonIgnore
    public long offset() {
        return offset;
    }

    @Override
    public int compareTo(Offset o) {
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
    @JsonIgnore
    public static <T extends Offset> T min(T offset1, T offset2) {
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
    @JsonIgnore
    public static <T extends Offset> T max(T offset1, T offset2) {
        if (offset1 == null) {
            return offset2;
        }
        if (offset2 == null) {
            return offset1;
        }
        return offset1.compareTo(offset2) > 0 ? offset1 : offset2;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof Offset)) {
            return false;
        }
        final Offset that = (Offset) o;
        return segmentId == that.segmentId && offset == that.offset;
    }

    @Override
    public int hashCode() {
        return Objects.hash(segmentId, offset);
    }

    @JsonIgnore
    public Offset skip2TargetOffset(long newOffset) {
        return skip2Target(segmentId, newOffset);
    }

    @JsonIgnore
    public Offset skipNextSegmentHead() {
        return skip2TargetHead(segmentId() + 1);
    }

    @JsonIgnore
    public Offset skip2TargetHead(long segmentId) {
        return skip2Target(segmentId, 0);
    }

    @JsonIgnore
    public Offset skip2Target(Offset offset) {
        return skip2Target(offset.segmentId(), offset.offset());
    }

    @JsonIgnore
    public Offset skip2Target(long segmentId, long offset) {
        if (segmentId == this.segmentId && offset == this.offset) {
            return this;
        }
        return new Offset(segmentId, offset);
    }

    @JsonIgnore
    public Offset skipOffset(long offset) {
        return skip2Target(segmentId, offset);
    }

    @JsonIgnore
    public boolean zero() {
        return compareTo(ZERO) == 0;
    }

    @JsonIgnore
    public boolean uninitialized() {
        return compareTo(UNINITIALIZED_OFFSET) == 0;
    }

    @Override
    public String toString() {
        return "Offset{" + "segmentId=" + segmentId + ", offset=" + offset + '}';
    }
}
