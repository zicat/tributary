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

/** LogSegmentUtil. */
public class SegmentUtil {

    public static final int BLOCK_HEAD_SIZE = 4;

    /**
     * find max LogSegment.
     *
     * @param s1 s1
     * @param s2 s2
     * @return LogSegment.
     */
    public static <S extends Segment> S max(S s1, S s2) {
        if (s1 == null) {
            return s2;
        }
        if (s2 == null) {
            return s1;
        }
        return s1.compareTo(s2) > 0 ? s1 : s2;
    }

    /**
     * find min log segment.
     *
     * @param s1 s1
     * @param s2 s2
     * @return LogSegment
     */
    public static <S extends Segment> S min(S s1, S s2) {
        final Segment max = max(s1, s2);
        return max == s1 ? s2 : s1;
    }
}
