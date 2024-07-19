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

import org.zicat.tributary.common.IOUtils;

import java.nio.ByteBuffer;

/** BlockReader. */
public class BlockReader extends Block {

    private long readBytes;

    public BlockReader(ByteBuffer resultBuf, ByteBuffer reusedBuf, long readBytes) {
        super(resultBuf, reusedBuf);
        this.readBytes = readBytes;
    }

    /**
     * create block reader by cache.
     *
     * @param data data
     * @param readBytes readBytes
     * @return BlockReader
     */
    public BlockReader wrap(byte[] data, long readBytes) {
        final ByteBuffer byteBuffer = IOUtils.reAllocate(this.resultBuf, data.length);
        byteBuffer.put(data).flip();
        return new BlockReader(byteBuffer, reusedBuf, readBytes);
    }

    /**
     * get read bytes. note: if data is compression in storage, this value is smaller than real data
     * size of method {@link Block#readNext()}
     *
     * @return size
     */
    public long readBytes() {
        return readBytes;
    }

    @Override
    public BlockReader reset() {
        super.reset();
        readBytes = 0;
        return this;
    }

    /**
     * empty block reader.
     *
     * @return block reader
     */
    public static BlockReader emptyBlockReader() {
        return new BlockReader(null, null, 0);
    }
}
