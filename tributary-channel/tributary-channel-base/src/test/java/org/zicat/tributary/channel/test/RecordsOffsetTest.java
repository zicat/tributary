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

import org.junit.Assert;
import org.junit.Test;
import org.zicat.tributary.channel.RecordsOffset;

import java.nio.ByteBuffer;

/** RecordsOffsetTest. */
public class RecordsOffsetTest {

    @Test
    public void testMin() {

        final RecordsOffset f1 = new RecordsOffset(1, 1);
        final RecordsOffset f2 = new RecordsOffset(2, 0);
        final RecordsOffset f3 = new RecordsOffset(2, 2);
        Assert.assertEquals(f1, RecordsOffset.min(f1, f2));
        Assert.assertEquals(f2, RecordsOffset.max(f1, f2));
        Assert.assertEquals(f2, RecordsOffset.min(f3, f2));
        Assert.assertEquals(f3, RecordsOffset.max(f3, f2));
    }

    @Test
    public void testByteBufferConvert() {
        final RecordsOffset recordsOffset = new RecordsOffset(2, 2);
        final ByteBuffer byteBuffer = ByteBuffer.allocate(100);
        recordsOffset.fillBuffer(byteBuffer);
        final RecordsOffset convertRecordsOffset = RecordsOffset.parserByteBuffer(byteBuffer);
        Assert.assertEquals(recordsOffset, convertRecordsOffset);
    }

    @Test
    public void testSkip() {
        final RecordsOffset recordsOffset = new RecordsOffset(2, 2);
        Assert.assertEquals(
                new RecordsOffset(recordsOffset.segmentId() + 1, 0),
                recordsOffset.skipNextSegmentHead());
        Assert.assertEquals(new RecordsOffset(4, 0), recordsOffset.skip2TargetHead(4));
    }

    @Test
    public void testHashEquals() {
        final RecordsOffset recordsOffset = new RecordsOffset(2, 2);
        final RecordsOffset recordsOffset2 = new RecordsOffset(2, 2);
        Assert.assertEquals(recordsOffset.hashCode(), recordsOffset2.hashCode());
        Assert.assertEquals(recordsOffset, recordsOffset2);
    }
}
