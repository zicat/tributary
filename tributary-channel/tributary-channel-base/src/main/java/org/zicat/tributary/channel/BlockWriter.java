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

import java.io.IOException;
import java.nio.ByteBuffer;

import static org.zicat.tributary.common.VIntUtil.putVInt;
import static org.zicat.tributary.common.VIntUtil.vIntLength;

/**
 * BlockWriter.
 *
 * <p>struct: Array[data length(VINT) + data body(byte arrays)]
 */
public final class BlockWriter extends Block {

    final int capacity;

    private BlockWriter(int capacity, ByteBuffer resultBuf, ByteBuffer reusedBuf) {
        super(resultBuf, reusedBuf);
        this.capacity = capacity;
    }

    public BlockWriter(int capacity) {
        this(capacity, ByteBuffer.allocateDirect(capacity), ByteBuffer.allocateDirect(capacity));
    }

    /**
     * wrap from byte array.
     *
     * @param data byte array
     * @param offset offset
     * @param length length
     * @return BlockWriter
     */
    public static BlockWriter wrap(byte[] data, int offset, int length) {
        final BlockWriter blockWriter = new BlockWriter(vIntLength(length));
        blockWriter.put(data, offset, length);
        return blockWriter;
    }

    /**
     * wrap from byte array.
     *
     * @param data byte array
     * @return BlockWriter
     */
    public static BlockWriter wrap(byte[] data) {
        return wrap(data, 0, data.length);
    }

    /**
     * append data to block set.
     *
     * @param data data
     * @param offset offset
     * @param length length
     * @return boolean put success
     */
    public final boolean put(byte[] data, int offset, int length) {
        if (remaining() < vIntLength(length)) {
            return false;
        }
        putVInt(resultBuf, length);
        resultBuf.put(data, offset, length);
        return true;
    }

    /**
     * append data to block set.
     *
     * @param data data
     * @return boolean put success
     */
    public final boolean put(byte[] data) {
        return put(data, 0, data.length);
    }

    /**
     * clear block writer.
     *
     * @param blockFlushHandler blockFlushHandler
     */
    public final void clear(BlockFlushHandler blockFlushHandler) throws IOException {
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

    /**
     * reallocate reusable BlockWriter.
     *
     * @param size size
     * @return BlockWriter
     */
    public BlockWriter reAllocate(int size) {
        return new BlockWriter(
                size, IOUtils.reAllocate(resultBuf, size), IOUtils.reAllocate(reusedBuf, size));
    }

    /**
     * get capacity.
     *
     * @return capacity
     */
    public final int capacity() {
        return capacity;
    }
}
