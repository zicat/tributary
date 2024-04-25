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

package org.zicat.tributary.channel;

import static org.zicat.tributary.common.VIntUtil.readVInt;

import java.nio.ByteBuffer;

/** BlockReader. */
public class BlockReader extends Block {

    private ByteBuffer cacheBuf;
    private long readBytes;

    public BlockReader(
            ByteBuffer resultBuf, ByteBuffer reusedBuf, ByteBuffer cacheBuf, long readBytes) {
        super(resultBuf, reusedBuf);
        this.cacheBuf = cacheBuf;
        this.readBytes = readBytes;
    }

    public BlockReader(ByteBuffer resultBuf, ByteBuffer reusedBuf, long readBytes) {
        this(resultBuf, reusedBuf, null, readBytes);
    }

    /**
     * create block reader by cache.
     *
     * @param cache cache
     * @param readBytes readBytes
     * @return BlockReader
     */
    public BlockReader cacheBlockReader(byte[] cache, long readBytes) {
        ByteBuffer cacheBuf = ByteBuffer.wrap(cache);
        return new BlockReader(resultBuf, reusedBuf, cacheBuf, readBytes);
    }

    public long readBytes() {
        return readBytes;
    }

    /**
     * read next value.
     *
     * @return byte[]
     */
    public byte[] readNext() {
        final ByteBuffer resultBuf = cacheBuf != null ? cacheBuf : resultBuf();
        if (resultBuf == null || !resultBuf.hasRemaining()) {
            return null;
        }
        final int length = readVInt(resultBuf);
        final byte[] bs = new byte[length];
        resultBuf.get(bs);
        return bs;
    }

    @Override
    public BlockReader reset() {
        super.reset();
        readBytes = 0;
        cacheBuf = null;
        return this;
    }
}
