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
import org.zicat.tributary.channel.BlockWriter;
import org.zicat.tributary.channel.RecordsResultSet;
import org.zicat.tributary.common.IOUtils;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/** BufferWriterTest. */
public class BlockWriterTest {

    @Test
    public void testReAllocate() {
        final byte[] testData = new byte[] {1, 2, 3, 4, 5, 6, 7, 8, 9};
        final BlockWriter bufferWriter = new BlockWriter(10);
        bufferWriter.put(testData);
        Assert.assertEquals(0, bufferWriter.remaining());

        final BlockWriter bufferWriter2 = bufferWriter.reAllocate(9);
        final BlockWriter bufferWriter3 = bufferWriter.reAllocate(12);
        Assert.assertNotSame(bufferWriter, bufferWriter2);
        Assert.assertNotSame(bufferWriter, bufferWriter3);

        Assert.assertSame(bufferWriter.resultBuf(), bufferWriter2.resultBuf());
        Assert.assertSame(bufferWriter.reusedBuf(), bufferWriter2.reusedBuf());
        Assert.assertNotSame(bufferWriter.resultBuf(), bufferWriter3.resultBuf());
        Assert.assertNotSame(bufferWriter.reusedBuf(), bufferWriter3.reusedBuf());

        Assert.assertEquals(9, bufferWriter.remaining());
        Assert.assertEquals(9, bufferWriter2.remaining());
        Assert.assertEquals(12, bufferWriter3.remaining());
    }

    @Test
    public void testWrap() throws IOException {
        final byte[] testData = new byte[] {1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
        final BlockWriter bufferWriter = BlockWriter.wrap(testData);
        Assert.assertEquals(0, bufferWriter.remaining());
        Assert.assertFalse(bufferWriter.put(new byte[] {1}));
        bufferWriter.clear(
                (buffer) -> {
                    ByteBuffer reusedBuffer =
                            IOUtils.reAllocate(buffer.reusedBuf(), buffer.resultBuf().remaining());
                    while (buffer.resultBuf().hasRemaining()) {
                        reusedBuffer.put(buffer.resultBuf().get());
                    }
                    reusedBuffer.flip();
                    final RecordsResultSet rs =
                            new BlockRecordsOffsetTest.BlockRecordsOffsetMock(reusedBuffer, "g1")
                                    .toResultSet();
                    Assert.assertTrue(rs.hasNext());
                    Assert.assertArrayEquals(testData, rs.next());
                    Assert.assertFalse(rs.hasNext());
                    buffer.reusedBuf(reusedBuffer);
                });
    }

    @Test
    public void testPutAndClear() throws IOException {
        final BlockWriter bufferWriter = new BlockWriter(20);
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
                (buffer) -> {
                    ByteBuffer reusedBuffer =
                            IOUtils.reAllocate(buffer.reusedBuf(), buffer.resultBuf().remaining());
                    while (buffer.resultBuf().hasRemaining()) {
                        reusedBuffer.put(buffer.resultBuf().get());
                    }
                    reusedBuffer.flip();
                    final RecordsResultSet rs =
                            new BlockRecordsOffsetTest.BlockRecordsOffsetMock(reusedBuffer, "g1")
                                    .toResultSet();
                    Assert.assertTrue(rs.hasNext());
                    Assert.assertArrayEquals(testData.get(0), rs.next());
                    Assert.assertTrue(rs.hasNext());
                    Assert.assertArrayEquals(testData.get(1), rs.next());
                    Assert.assertFalse(rs.hasNext());
                    buffer.reusedBuf(reusedBuffer);
                });
    }
}
