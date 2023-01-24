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

import org.zicat.tributary.channel.utils.IOUtils;

import java.io.IOException;
import java.nio.ByteBuffer;

import static org.zicat.tributary.channel.utils.VIntUtil.putVInt;
import static org.zicat.tributary.channel.utils.VIntUtil.vIntLength;

/**
 * BufferWriter.
 *
 * <p>struct: Array[data length(VINT) + data body(byte arrays)]
 */
public final class BufferWriter {

    final ByteBuffer writeBuf;
    final int capacity;
    ByteBuffer reusedByteBuf;

    private BufferWriter(int capacity, ByteBuffer writeBuf, ByteBuffer reusedByteBuf) {
        this.capacity = capacity;
        this.writeBuf = writeBuf;
        this.reusedByteBuf = reusedByteBuf;
    }

    public BufferWriter(int capacity) {
        this.capacity = capacity;
        this.writeBuf = ByteBuffer.allocateDirect(capacity);
        this.reusedByteBuf = ByteBuffer.allocateDirect(capacity);
    }

    /**
     * wrap from byte array.
     *
     * @param data byte array
     * @param offset offset
     * @param length length
     * @return ReusableBuffer
     */
    public static BufferWriter wrap(byte[] data, int offset, int length) {
        final BufferWriter bufferWriter = new BufferWriter(vIntLength(length));
        bufferWriter.put(data, offset, length);
        return bufferWriter;
    }

    /**
     * wrap from byte array.
     *
     * @param data byte array
     * @return ReusableBuffer
     */
    public static BufferWriter wrap(byte[] data) {
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
        putVInt(writeBuf, length);
        writeBuf.put(data, offset, length);
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
     * clear buffer writer.
     *
     * @param clearHandler consumerHandler
     */
    public final void clear(ClearHandler clearHandler) throws IOException {
        if (isEmpty()) {
            return;
        }
        writeBuf.flip();
        reusedByteBuf = clearHandler.clearCallback(writeBuf, reusedByteBuf);
        writeBuf.clear();
    }

    /** ClearHandler. */
    public interface ClearHandler {

        /**
         * clearCallback.
         *
         * @param byteBuffer byteBuffer
         * @param reusedBuffer reusedBuffer
         * @return reusedBuffer.
         * @throws IOException IOException
         */
        ByteBuffer clearCallback(ByteBuffer byteBuffer, ByteBuffer reusedBuffer) throws IOException;
    }

    /**
     * reallocate reusable buffer.
     *
     * @param size size
     * @return BlockSet
     */
    public BufferWriter reAllocate(int size) {
        return new BufferWriter(
                size, IOUtils.reAllocate(writeBuf, size), IOUtils.reAllocate(reusedByteBuf, size));
    }

    /**
     * get capacity.
     *
     * @return capacity
     */
    public final int capacity() {
        return capacity;
    }

    /**
     * remaining.
     *
     * @return int
     */
    public final int remaining() {
        return writeBuf.remaining();
    }

    /**
     * position.
     *
     * @return position
     */
    public final int position() {
        return writeBuf.position();
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
     * write buf. VisibleForTesting
     *
     * @return ByteBuffer
     */
    public final ByteBuffer writeBuf() {
        return writeBuf;
    }

    /**
     * reusedByteBuf. VisibleForTesting
     *
     * @return ByteBuffer
     */
    public final ByteBuffer reusedByteBuf() {
        return reusedByteBuf;
    }
}
