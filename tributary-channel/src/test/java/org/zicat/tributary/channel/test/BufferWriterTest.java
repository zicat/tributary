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
import org.zicat.tributary.channel.BufferWriter;
import org.zicat.tributary.channel.RecordsResultSet;
import org.zicat.tributary.channel.utils.IOUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/** BufferWriterTest. */
public class BufferWriterTest {

    @Test
    public void testReAllocate() {
        final byte[] testData = new byte[] {1, 2, 3, 4, 5, 6, 7, 8, 9};
        final BufferWriter bufferWriter = new BufferWriter(10);
        bufferWriter.put(testData);
        Assert.assertEquals(0, bufferWriter.remaining());

        final BufferWriter bufferWriter2 = bufferWriter.reAllocate(9);
        final BufferWriter bufferWriter3 = bufferWriter.reAllocate(12);
        Assert.assertNotSame(bufferWriter, bufferWriter2);
        Assert.assertNotSame(bufferWriter, bufferWriter3);

        Assert.assertSame(bufferWriter.writeBuf(), bufferWriter2.writeBuf());
        Assert.assertSame(bufferWriter.reusedByteBuf(), bufferWriter2.reusedByteBuf());
        Assert.assertNotSame(bufferWriter.writeBuf(), bufferWriter3.writeBuf());
        Assert.assertNotSame(bufferWriter.reusedByteBuf(), bufferWriter3.reusedByteBuf());

        Assert.assertEquals(9, bufferWriter.remaining());
        Assert.assertEquals(9, bufferWriter2.remaining());
        Assert.assertEquals(12, bufferWriter3.remaining());
    }

    @Test
    public void testWrap() throws IOException {
        final byte[] testData = new byte[] {1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
        final BufferWriter bufferWriter = BufferWriter.wrap(testData);
        Assert.assertEquals(0, bufferWriter.remaining());
        Assert.assertFalse(bufferWriter.put(new byte[] {1}));
        bufferWriter.clear(
                (byteBuffer, reusedBuffer) -> {
                    reusedBuffer = IOUtils.reAllocate(reusedBuffer, byteBuffer.remaining());
                    while (byteBuffer.hasRemaining()) {
                        reusedBuffer.put(byteBuffer.get());
                    }
                    reusedBuffer.flip();
                    final RecordsResultSet rs =
                            new BufferRecordsOffsetTest.BufferRecordsOffsetMock(reusedBuffer)
                                    .toResultSet();
                    Assert.assertTrue(rs.hasNext());
                    Assert.assertArrayEquals(testData, rs.next());
                    Assert.assertFalse(rs.hasNext());
                    return reusedBuffer;
                });
    }

    @Test
    public void testPutAndClear() throws IOException {
        final BufferWriter bufferWriter = new BufferWriter(20);
        Assert.assertEquals(20, bufferWriter.capacity());
        Assert.assertEquals(20, bufferWriter.remaining());
        Assert.assertTrue(bufferWriter.isEmpty());
        final List<byte[]> testData = new ArrayList<>();
        testData.add(new byte[] {1, 2, 3, 4, 5, 6, 7, 8, 9, 10});
        testData.add(new byte[] {1, 2, 3, 4, 5});
        testData.add(new byte[] {1, 2, 3});
        Assert.assertTrue(bufferWriter.put(testData.get(0)));
        Assert.assertFalse(bufferWriter.isEmpty());
        Assert.assertNotEquals(0, bufferWriter.remaining());
        Assert.assertTrue(bufferWriter.put(testData.get(1)));
        Assert.assertFalse(bufferWriter.put(testData.get(2)));
        Assert.assertEquals(3, bufferWriter.remaining());
        bufferWriter.clear(
                (byteBuffer, reusedBuffer) -> {
                    reusedBuffer = IOUtils.reAllocate(reusedBuffer, byteBuffer.remaining());
                    while (byteBuffer.hasRemaining()) {
                        reusedBuffer.put(byteBuffer.get());
                    }
                    reusedBuffer.flip();
                    final RecordsResultSet rs =
                            new BufferRecordsOffsetTest.BufferRecordsOffsetMock(reusedBuffer)
                                    .toResultSet();
                    Assert.assertTrue(rs.hasNext());
                    Assert.assertArrayEquals(testData.get(0), rs.next());
                    Assert.assertTrue(rs.hasNext());
                    Assert.assertArrayEquals(testData.get(1), rs.next());
                    Assert.assertFalse(rs.hasNext());
                    return reusedBuffer;
                });
    }
}
