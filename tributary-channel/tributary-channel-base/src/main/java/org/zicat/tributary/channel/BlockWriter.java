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

import org.zicat.tributary.common.util.IOUtils;

import java.nio.ByteBuffer;

import static org.zicat.tributary.common.util.VIntUtil.vIntEncodeLength;

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
        return wrap(ByteBuffer.wrap(data, offset, length));
    }

    /**
     * wrap from byte array.
     *
     * @param byteBuffer byteBuffer
     * @return BlockWriter
     */
    public static BlockWriter wrap(ByteBuffer byteBuffer) {
        final BlockWriter blockWriter = new BlockWriter(vIntEncodeLength(byteBuffer.remaining()));
        blockWriter.put(byteBuffer);
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
    public int capacity() {
        return capacity;
    }
}
