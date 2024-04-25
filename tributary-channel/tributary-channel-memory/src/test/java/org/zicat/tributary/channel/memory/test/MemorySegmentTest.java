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

import org.junit.Assert;
import org.junit.Test;
import org.zicat.tributary.channel.BlockWriter;
import org.zicat.tributary.channel.ChannelBlockCache;
import org.zicat.tributary.channel.CompressionType;
import org.zicat.tributary.channel.memory.MemorySegment;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/** MemorySegmentTest. */
public class MemorySegmentTest {

    @Test
    public void testReadWrite() throws IOException {
        try (final MemorySegment memorySegment =
                new MemorySegment(
                        1,
                        new BlockWriter(128),
                        CompressionType.NONE,
                        10240 * 4,
                        new ChannelBlockCache(1))) {
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
                byteBuffer.flip();
                final byte[] readBs = new byte[byteBuffer.remaining()];
                byteBuffer.get(readBs);
                Assert.assertArrayEquals(bs, readBs);
                offset += readBs.length;
            }
            memorySegment.readonly();
        }
    }
}
