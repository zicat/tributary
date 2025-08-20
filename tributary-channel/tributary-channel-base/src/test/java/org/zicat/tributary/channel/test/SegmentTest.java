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
import org.zicat.tributary.channel.*;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/** SegmentTest. */
public class SegmentTest {

    @Test
    public void test() throws IOException, InterruptedException {
        final BlockWriter writer = new BlockWriter(10);
        final long segmentSize = 100;
        final long id = 2;
        final CompressionType compressionType = CompressionType.NONE;
        final Buffer buffer = new Buffer();
        try (final Segment segment =
                new Segment(
                        id, writer, compressionType, segmentSize, 0L, new ChannelBlockCache(1)) {
                    @Override
                    protected long legalOffset(long offset) {
                        return offset;
                    }

                    @Override
                    public void writeFull(ByteBuffer byteBuffer) {
                        buffer.writeFull(byteBuffer);
                    }

                    @Override
                    public void readFull(ByteBuffer byteBuffer, long offset) {
                        buffer.readFull(byteBuffer, offset);
                    }

                    @Override
                    public void persist(boolean force) {}

                    @Override
                    public void recycle() {
                        buffer.clear();
                    }
                }) {
            test(segment);
        }
    }

    @Test
    public void testWithCountBlockCache() throws IOException, InterruptedException {
        final BlockWriter writer = new BlockWriter(10);
        final long segmentSize = 100;
        final long id = 2;
        final CompressionType compressionType = CompressionType.NONE;
        final Buffer buffer = new Buffer();
        try (final Segment segment =
                new Segment(id, writer, compressionType, segmentSize, 0L, null) {
                    @Override
                    protected long legalOffset(long offset) {
                        return offset;
                    }

                    @Override
                    public void writeFull(ByteBuffer byteBuffer) {
                        buffer.writeFull(byteBuffer);
                    }

                    @Override
                    public void readFull(ByteBuffer byteBuffer, long offset) {
                        buffer.readFull(byteBuffer, offset);
                    }

                    @Override
                    public void persist(boolean force) {}

                    @Override
                    public void recycle() {
                        buffer.clear();
                    }
                }) {
            test(segment);
        }
    }

    private void test(Segment segment) throws IOException, InterruptedException {

        final byte[] data = new byte[] {(byte) 1, (byte) 2, (byte) 3, (byte) 4, (byte) 2};
        for (int i = 0; i < 11; i++) {
            // (5 + 4) * 11 = 99, 5 is data size, 4 is data length
            Assert.assertTrue(segment.append(data, 0, data.length).appended());
        }

        Assert.assertTrue(segment.append(data, 0, data.length).appended());
        Assert.assertFalse(segment.append(data, 0, data.length).appended());
        segment.flush();
        Assert.assertEquals(120, segment.lag(Offset.ZERO));
        Assert.assertEquals(50, segment.lag(new Offset(2L, 70L)));
        Assert.assertEquals(0, segment.lag(new Offset(3L, 70L)));

        BlockReaderOffset newRecordOffset =
                BlockReaderOffset.cast(new Offset(segment.segmentId(), 0L));

        int count = 0;
        while (true) {
            RecordsResultSet recordsResultSet =
                    segment.readBlock(newRecordOffset, 1, TimeUnit.MILLISECONDS).toResultSet();
            if (recordsResultSet.isEmpty()) {
                break;
            }
            while (recordsResultSet.hasNext()) {
                count++;
                Assert.assertArrayEquals(data, recordsResultSet.next());
            }
            newRecordOffset = BlockReaderOffset.cast(recordsResultSet.nexOffset());
        }
        Assert.assertEquals(12, count);
    }

    private static class Buffer {

        private final List<byte[]> chains = new ArrayList<>();

        public void writeFull(ByteBuffer byteBuffer) {
            if (byteBuffer.remaining() == 0) {
                return;
            }
            byte[] data = new byte[byteBuffer.remaining()];
            byteBuffer.get(data);
            chains.add(data);
        }

        public void clear() {
            chains.clear();
        }

        public void readFull(ByteBuffer byteBuffer, long offset) {
            int index = 0;
            long offsetOffset = 0;
            for (; index < chains.size(); index++) {
                byte[] data = chains.get(index);
                if (offsetOffset + data.length < offset) {
                    offsetOffset += data.length;
                    continue;
                }
                break;
            }
            if (index >= chains.size()) {
                throw new IndexOutOfBoundsException();
            }
            byte[] dataOffset = chains.get(index);
            int deltaOffset = (int) (offset - offsetOffset);
            while (byteBuffer.hasRemaining()) {
                byteBuffer.put(
                        dataOffset,
                        deltaOffset,
                        Math.min(byteBuffer.remaining(), dataOffset.length - deltaOffset));
                if (byteBuffer.hasRemaining()) {
                    index++;
                    dataOffset = chains.get(index);
                    deltaOffset = 0;
                }
            }
        }
    }
}
