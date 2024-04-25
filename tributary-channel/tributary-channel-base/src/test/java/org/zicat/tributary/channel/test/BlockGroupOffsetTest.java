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

import static org.zicat.tributary.common.VIntUtil.putVInt;
import static org.zicat.tributary.common.VIntUtil.vIntLength;

import org.junit.Assert;
import org.junit.Test;
import org.zicat.tributary.channel.BlockGroupOffset;
import org.zicat.tributary.channel.BlockReader;
import org.zicat.tributary.channel.RecordsResultSet;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/** BlockGroupOffsetTest. */
public class BlockGroupOffsetTest {

    @Test
    public void testToResultSet() {
        final List<byte[]> testData = new ArrayList<>();
        testData.add(new byte[] {1, 2, 3, 4, 5, 6, 7, 8, 9, 10});
        testData.add(new byte[] {1, 2, 3, 4, 5});
        testData.add(new byte[] {1, 2, 3});
        final ByteBuffer resultBuf = toBuffer(testData);
        final BlockGroupOffset groupOffset = new BlockGroupOffsetMock(resultBuf, "g1");
        final RecordsResultSet resultSet = groupOffset.toResultSet();
        Assert.assertTrue(resultSet.hasNext());
        Assert.assertArrayEquals(testData.get(0), resultSet.next());
        Assert.assertTrue(resultSet.hasNext());
        Assert.assertArrayEquals(testData.get(1), resultSet.next());
        Assert.assertTrue(resultSet.hasNext());
        Assert.assertArrayEquals(testData.get(2), resultSet.next());
        Assert.assertSame(groupOffset, resultSet.nexGroupOffset());
        Assert.assertFalse(resultSet.isEmpty());
    }

    @Test
    public void testReset() {
        final List<byte[]> testData = new ArrayList<>();
        testData.add(new byte[] {1, 2, 3, 4, 5, 6, 7, 8, 9, 10});
        final ByteBuffer resultBuf = toBuffer(testData);
        final BlockGroupOffset groupOffset = new BlockGroupOffsetMock(resultBuf, "g1");
        Assert.assertTrue(groupOffset.toResultSet().hasNext());
        Assert.assertFalse(groupOffset.reset().toResultSet().hasNext());
        Assert.assertTrue(groupOffset.reset().toResultSet().isEmpty());
    }

    @Test
    public void testSkip2Target() {
        final List<byte[]> testData = new ArrayList<>();
        testData.add(new byte[] {1, 2, 3, 4, 5, 6, 7, 8, 9, 10});
        final ByteBuffer resultBuf = toBuffer(testData);
        final BlockGroupOffset groupOffset = new BlockGroupOffsetMock(resultBuf, "g1");
        final BlockGroupOffset groupOffset2 = groupOffset.skip2Target(2, 0, "g1");
        Assert.assertSame(groupOffset.block().resultBuf(), groupOffset2.block().resultBuf());
        Assert.assertSame(groupOffset.block().resultBuf(), groupOffset2.block().resultBuf());

        Assert.assertTrue(groupOffset.toResultSet().hasNext());
        Assert.assertFalse(groupOffset2.toResultSet().hasNext());
    }

    /** BlockGroupOffsetMock. */
    public static class BlockGroupOffsetMock extends BlockGroupOffset {

        public BlockGroupOffsetMock(ByteBuffer resultBuf, String groupId) {
            super(1, 0, groupId, new BlockReader(resultBuf, null, null, resultBuf.remaining()));
        }
    }

    /**
     * create buffer.
     *
     * @param testData testData
     * @return ByteBuffer
     */
    private static ByteBuffer toBuffer(List<byte[]> testData) {
        int length = 0;
        for (byte[] bs : testData) {
            length += vIntLength(bs.length);
        }
        final ByteBuffer resultBuf = ByteBuffer.allocate(length);
        for (byte[] bs : testData) {
            putVInt(resultBuf, bs.length);
            resultBuf.put(bs);
        }
        resultBuf.flip();
        return resultBuf;
    }
}
