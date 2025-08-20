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

package org.zicat.tributary.channel.memory.test;

import static org.zicat.tributary.channel.test.StringTestUtils.createStringByLength;

import org.junit.Assert;
import org.junit.Test;
import org.zicat.tributary.channel.*;
import org.zicat.tributary.channel.memory.MemorySegment;
import org.zicat.tributary.common.IOUtils;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/** MemorySegmentTest. */
public class MemorySegmentTest {

    @Test
    public void testLegalOffset() throws IOException {
        try (final MemorySegment memorySegment =
                new MemorySegment(
                        1,
                        new BlockWriter(128),
                        CompressionType.NONE,
                        10240 * 4,
                        new ChannelBlockCache(1))) {
            Assert.assertEquals(1, memorySegment.legalOffset(1));
        }
    }

    @Test
    public void testReadWrite() {
        MemorySegment memorySegment = null;
        try {
            memorySegment =
                    new MemorySegment(
                            1,
                            new BlockWriter(128),
                            CompressionType.NONE,
                            10240 * 4,
                            new ChannelBlockCache(1));
            final Random random = new Random(1023312);
            final List<byte[]> result = new ArrayList<>();
            for (int i = 0; i < 10000; i++) {
                final int count = random.nextInt(60) + 60;
                final byte[] bs = new byte[count];
                for (int j = 0; j < count; j++) {
                    bs[j] = (byte) random.nextInt(256);
                }
                result.add(bs);
                memorySegment.writeFull(ByteBuffer.wrap(bs));
            }

            int offset = 0;
            for (byte[] bs : result) {
                final ByteBuffer byteBuffer = ByteBuffer.allocate(bs.length);
                memorySegment.readFull(byteBuffer, offset);
                Assert.assertArrayEquals(bs, byteBuffer.array());
                offset += bs.length;
            }
        } finally {
            IOUtils.closeQuietly(memorySegment);
        }
        memorySegment.recycle();
    }

    @Test
    public void testMultiThread() throws InterruptedException, IOException {

        final List<String> result = new ArrayList<>();
        for (int i = 6; i < 200; i++) {
            result.add(createStringByLength(i));
        }
        final Iterator<String> expectedIt = result.iterator();

        try (final MemorySegment segment =
                new MemorySegment(
                        1,
                        new BlockWriter(128),
                        CompressionType.NONE,
                        10240 * 4,
                        new ChannelBlockCache(1))) {
            Thread writerThread =
                    new Thread(
                            () -> {
                                try {
                                    Assert.assertTrue(segment.append("").appended());
                                    for (String s : result) {
                                        segment.append(s);
                                    }
                                } catch (IOException ioException) {
                                    throw new RuntimeException(ioException);
                                }
                            });
            Thread readThread =
                    new Thread(
                            () -> {
                                BlockReaderOffset offset = BlockReaderOffset.cast(Offset.ZERO);
                                while (expectedIt.hasNext()) {
                                    RecordsResultSet resultSet;
                                    try {
                                        resultSet =
                                                segment.readBlock(offset, 1, TimeUnit.MILLISECONDS)
                                                        .toResultSet();
                                        Assert.assertTrue(resultSet.hasNext());
                                        while (resultSet.hasNext()) {
                                            byte[] bs = resultSet.next();
                                            Assert.assertEquals(
                                                    expectedIt.next(),
                                                    new String(bs, StandardCharsets.UTF_8));
                                        }
                                        offset = BlockReaderOffset.cast(resultSet.nexOffset());
                                    } catch (Exception e) {
                                        throw new RuntimeException(e);
                                    }
                                }
                            });
            writerThread.start();
            readThread.start();
            writerThread.join();
            segment.flush();
            readThread.join();
        }
    }
}
