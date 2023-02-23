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

package org.zicat.tributary.channel.test;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.junit.Assert;
import org.junit.Test;
import org.zicat.tributary.channel.GroupOffset;

/** GroupOffsetTest. */
public class GroupOffsetTest {

    @Test
    public void testMin() {

        final GroupOffset f1 = new GroupOffset(1, 1, "g1");
        final GroupOffset f2 = new GroupOffset(2, 0, "g1");
        final GroupOffset f3 = new GroupOffset(2, 2, "g1");
        Assert.assertEquals(f1, GroupOffset.min(f1, f2));
        Assert.assertEquals(f2, GroupOffset.max(f1, f2));
        Assert.assertEquals(f2, GroupOffset.min(f3, f2));
        Assert.assertEquals(f3, GroupOffset.max(f3, f2));
    }

    @Test
    public void testByteBufferConvert() throws JsonProcessingException {
        final GroupOffset groupOffset = new GroupOffset(2, 2, "g1");
        final String json = groupOffset.toJson();
        final GroupOffset convertGroupOffset = GroupOffset.fromJson(json);
        Assert.assertEquals(groupOffset, convertGroupOffset);
    }

    @Test
    public void testSkip() {
        final GroupOffset groupOffset = new GroupOffset(2, 2, "g1");
        Assert.assertEquals(
                new GroupOffset(groupOffset.segmentId() + 1, 0, "g1"),
                groupOffset.skipNextSegmentHead());
        Assert.assertEquals(new GroupOffset(4, 0, "g1"), groupOffset.skip2TargetHead(4));
    }

    @Test
    public void testHashEquals() {
        final GroupOffset groupOffset = new GroupOffset(2, 2, "g1");
        final GroupOffset groupOffset2 = new GroupOffset(2, 2, "g1");
        Assert.assertEquals(groupOffset.hashCode(), groupOffset2.hashCode());
        Assert.assertEquals(groupOffset, groupOffset2);
    }
}
