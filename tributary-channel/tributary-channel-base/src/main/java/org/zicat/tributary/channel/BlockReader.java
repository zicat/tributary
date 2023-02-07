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

import java.nio.ByteBuffer;

import static org.zicat.tributary.common.VIntUtil.readVInt;

/** BlockReader. */
public class BlockReader extends Block {

    private long readBytes;

    public BlockReader(ByteBuffer resultBuf, ByteBuffer reusedBuf, long readBytes) {
        super(resultBuf, reusedBuf);
        this.readBytes = readBytes;
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
        final ByteBuffer resultBuf = resultBuf();
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
        return this;
    }
}
