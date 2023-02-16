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

import java.util.Objects;

/** Offset. */
public class Offset implements Comparable<Offset> {

    protected final long segmentId;
    protected final long offset;

    public Offset(long segmentId, long offset) {
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
}
