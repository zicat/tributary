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
import org.zicat.tributary.channel.BlockReaderOffset;
import org.zicat.tributary.channel.ChannelBlockCache;
import org.zicat.tributary.common.util.VIntUtil;

import java.nio.ByteBuffer;

import static org.zicat.tributary.channel.BlockReader.emptyBlockReader;

/** ChannelBlockCacheTest. */
public class ChannelBlockCacheTest {

    @Test
    public void test() {
        final byte[] data1 = createData("aa".getBytes());
        final byte[] data2 = createData("aaa".getBytes());
        final byte[] data3 = createData("aaaa".getBytes());
        final byte[] data4 = createData("aaaaa".getBytes());
        final ChannelBlockCache blockCache = new ChannelBlockCache(3);
        blockCache.put(1, 1, 1 + data1.length, data1);
        blockCache.put(1, 3, 3 + data2.length, data2);
        blockCache.put(2, 1, 1 + data3.length, data3);

        final BlockReaderOffset blockReaderOffset1 =
                blockCache.find(new BlockReaderOffset(1, 1, emptyBlockReader()));
        Assert.assertEquals(1, blockReaderOffset1.segmentId());
        Assert.assertEquals(1 + data1.length, blockReaderOffset1.offset());
        Assert.assertEquals(data1.length, blockReaderOffset1.blockReader().readBytes());
        Assert.assertEquals("aa", new String(blockReaderOffset1.blockReader().readNext()));

        try {
            blockCache.put(0, 0, data1.length, data1);
            Assert.fail();
        } catch (Exception e) {
            Assert.assertTrue(e instanceof IllegalArgumentException);
        }

        BlockReaderOffset blockReaderOffset3 =
                blockCache.find(new BlockReaderOffset(2, 1, emptyBlockReader()));
        Assert.assertEquals(2, blockReaderOffset3.segmentId());
        Assert.assertEquals(1 + data3.length, blockReaderOffset3.offset());
        Assert.assertEquals(data3.length, blockReaderOffset3.blockReader().readBytes());
        Assert.assertEquals("aaaa", new String(blockReaderOffset3.blockReader().readNext()));

        Assert.assertNull(blockCache.find(new BlockReaderOffset(2, 2, emptyBlockReader())));

        blockCache.put(2, 5, 5 + data4.length, data4);
        // expired
        Assert.assertNull(blockCache.find(new BlockReaderOffset(1, 1, emptyBlockReader())));
        blockReaderOffset3 = blockCache.find(new BlockReaderOffset(2, 1, emptyBlockReader()));
        Assert.assertEquals(2, blockReaderOffset3.segmentId());
        Assert.assertEquals(1 + data3.length, blockReaderOffset3.offset());
        Assert.assertEquals(data3.length, blockReaderOffset3.blockReader().readBytes());
        Assert.assertEquals("aaaa", new String(blockReaderOffset3.blockReader().readNext()));

        final BlockReaderOffset blockReaderOffset4 =
                blockCache.find(new BlockReaderOffset(2, 5, emptyBlockReader()));
        Assert.assertEquals(2, blockReaderOffset4.segmentId());
        Assert.assertEquals(5 + data4.length, blockReaderOffset4.offset());
        Assert.assertEquals(data4.length, blockReaderOffset4.blockReader().readBytes());
        Assert.assertEquals("aaaaa", new String(blockReaderOffset4.blockReader().readNext()));
    }

    private byte[] createData(byte[] data) {
        ByteBuffer byteBuffer = ByteBuffer.allocate(VIntUtil.vIntEncodeLength(data.length));
        VIntUtil.putVInt(byteBuffer, data.length);
        byteBuffer.put(data);
        return byteBuffer.array();
    }
}
