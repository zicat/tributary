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

import java.io.IOException;
import java.nio.ByteBuffer;

import static org.zicat.tributary.common.util.VIntUtil.*;

/** Block. */
public class Block {

    protected final ByteBuffer resultBuf;
    protected ByteBuffer reusedBuf;

    public Block(ByteBuffer resultBuf, ByteBuffer reusedBuf) {
        this.resultBuf = resultBuf;
        this.reusedBuf = reusedBuf;
    }

    /**
     * reset buffer.
     *
     * @return this
     */
    public Block reset() {
        if (resultBuf != null && resultBuf.hasRemaining()) {
            resultBuf.clear();
            resultBuf.flip();
        }
        return this;
    }

    /**
     * get result buf.
     *
     * @return ByteBuffer
     */
    public ByteBuffer resultBuf() {
        return resultBuf;
    }

    /**
     * get reused buf.
     *
     * @return ByteBuffer
     */
    public ByteBuffer reusedBuf() {
        return reusedBuf;
    }

    /**
     * set reused buf.
     *
     * @param reusedBuf reusedBuf
     */
    public void reusedBuf(ByteBuffer reusedBuf) {
        this.reusedBuf = reusedBuf;
    }

    /**
     * remaining.
     *
     * @return int
     */
    public final int remaining() {
        return resultBuf.remaining();
    }

    /**
     * position.
     *
     * @return position
     */
    public final int position() {
        return resultBuf.position();
    }

    /**
     * check is empty.
     *
     * @return boolean.
     */
    public final boolean isEmpty() {
        return position() == 0;
    }

    /**
     * read next value.
     *
     * @return byte[]
     */
    public byte[] readNext() {
        if (resultBuf == null || !resultBuf.hasRemaining()) {
            return null;
        }
        final int length = readVInt(resultBuf);
        final byte[] bs = new byte[length];
        resultBuf.get(bs);
        return bs;
    }

    /**
     * append data to block set.
     *
     * @param data data
     * @param offset offset
     * @param length length
     * @return boolean put success
     */
    public boolean put(byte[] data, int offset, int length) {
        return put(ByteBuffer.wrap(data, offset, length));
    }

    /**
     * append data to block set.
     *
     * @param data data
     * @return boolean put success
     */
    public boolean put(byte[] data) {
        return put(ByteBuffer.wrap(data));
    }

    /**
     * append data. to block set.
     *
     * @param byteBuffer byteBuffer
     * @return boolean put success
     */
    public boolean put(ByteBuffer byteBuffer) {
        if (remaining() < vIntEncodeLength(byteBuffer.remaining())) {
            return false;
        }
        putVInt(resultBuf, byteBuffer.remaining());
        resultBuf.put(byteBuffer);
        return true;
    }

    /**
     * clear block writer.
     *
     * @param blockFlushHandler blockFlushHandler
     */
    public void clear(BlockFlushHandler blockFlushHandler) throws IOException {
        if (isEmpty()) {
            return;
        }
        resultBuf.flip();
        blockFlushHandler.callback(this);
        resultBuf.clear();
    }

    /** ClearHandler. */
    public interface BlockFlushHandler {

        /**
         * callback.
         *
         * @param block block
         * @throws IOException IOException
         */
        void callback(Block block) throws IOException;
    }
}
