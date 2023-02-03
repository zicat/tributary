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

package org.zicat.tributary.channle.file.test;

import org.junit.Assert;
import org.junit.Test;
import org.zicat.tributary.channel.RecordsResultSet;
import org.zicat.tributary.channel.file.BlockReader;
import org.zicat.tributary.channel.file.BlockRecordsOffset;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import static org.zicat.tributary.common.VIntUtil.putVInt;
import static org.zicat.tributary.common.VIntUtil.vIntLength;

/** BufferRecordsOffsetTest. */
public class BlockRecordsOffsetTest {

    @Test
    public void testToResultSet() {
        final List<byte[]> testData = new ArrayList<>();
        testData.add(new byte[] {1, 2, 3, 4, 5, 6, 7, 8, 9, 10});
        testData.add(new byte[] {1, 2, 3, 4, 5});
        testData.add(new byte[] {1, 2, 3});
        final ByteBuffer resultBuf = toBuffer(testData);
        final BlockRecordsOffset recordsOffset = new BlockRecordsOffsetMock(resultBuf);
        final RecordsResultSet resultSet = recordsOffset.toResultSet();
        Assert.assertTrue(resultSet.hasNext());
        Assert.assertArrayEquals(testData.get(0), resultSet.next());
        Assert.assertTrue(resultSet.hasNext());
        Assert.assertArrayEquals(testData.get(1), resultSet.next());
        Assert.assertTrue(resultSet.hasNext());
        Assert.assertArrayEquals(testData.get(2), resultSet.next());
        Assert.assertSame(recordsOffset, resultSet.nexRecordsOffset());
        Assert.assertFalse(resultSet.isEmpty());
    }

    @Test
    public void testReset() {
        final List<byte[]> testData = new ArrayList<>();
        testData.add(new byte[] {1, 2, 3, 4, 5, 6, 7, 8, 9, 10});
        final ByteBuffer resultBuf = toBuffer(testData);
        final BlockRecordsOffset recordsOffset = new BlockRecordsOffsetMock(resultBuf);
        Assert.assertTrue(recordsOffset.toResultSet().hasNext());
        Assert.assertFalse(recordsOffset.reset().toResultSet().hasNext());
        Assert.assertTrue(recordsOffset.reset().toResultSet().isEmpty());
    }

    @Test
    public void testSkip2Target() {
        final List<byte[]> testData = new ArrayList<>();
        testData.add(new byte[] {1, 2, 3, 4, 5, 6, 7, 8, 9, 10});
        final ByteBuffer resultBuf = toBuffer(testData);
        final BlockRecordsOffset recordsOffset = new BlockRecordsOffsetMock(resultBuf);
        final BlockRecordsOffset recordsOffset2 = recordsOffset.skip2Target(2, 0);
        Assert.assertSame(recordsOffset.block().resultBuf(), recordsOffset2.block().resultBuf());
        Assert.assertSame(recordsOffset.block().resultBuf(), recordsOffset2.block().resultBuf());

        Assert.assertTrue(recordsOffset.toResultSet().hasNext());
        Assert.assertFalse(recordsOffset2.toResultSet().hasNext());
    }

    /** BufferRecordsOffsetMock. */
    public static class BlockRecordsOffsetMock extends BlockRecordsOffset {

        public BlockRecordsOffsetMock(ByteBuffer resultBuf) {
            super(1, 0, new BlockReader(resultBuf, null, resultBuf.remaining()));
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
