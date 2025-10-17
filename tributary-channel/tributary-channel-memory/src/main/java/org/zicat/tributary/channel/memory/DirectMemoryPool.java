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

package org.zicat.tributary.channel.memory;

import org.zicat.tributary.common.config.MemorySize;

import java.nio.ByteBuffer;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

/** DirectMemoryPool. */
public class DirectMemoryPool {

    private static final MemorySize CHUNK = new MemorySize(1024 * 1024);
    private static final MemorySize DEFAULT_DIRECT_CAPACITY =
            new MemorySize(sun.misc.VM.maxDirectMemory());
    private static final BlockingQueue<ByteBuffer> POOL =
            new ArrayBlockingQueue<>(
                    (int) ((DEFAULT_DIRECT_CAPACITY.getBytes() * 0.3) / CHUNK.getBytes()));
    private static final AtomicInteger CREATED_COUNT = new AtomicInteger(0);

    /** get chunk. */
    public static ByteBuffer borrowChunk() {
        final ByteBuffer byteBuffer = POOL.poll();
        if (byteBuffer != null) {
            byteBuffer.clear();
            return byteBuffer;
        }
        CREATED_COUNT.incrementAndGet();
        return ByteBuffer.allocateDirect((int) CHUNK.getBytes());
    }

    /**
     * capacity.
     *
     * @return capacity
     */
    public static MemorySize capacity() {
        return new MemorySize(
                Math.max(
                        CREATED_COUNT.get() * CHUNK.getBytes(),
                        DEFAULT_DIRECT_CAPACITY.getBytes()));
    }

    /**
     * chunk size.
     *
     * @return chunk size
     */
    public static MemorySize chunkSize() {
        return CHUNK;
    }

    /**
     * memory usage.
     *
     * @return memory usage
     */
    public static MemorySize usage() {
        return new MemorySize(Math.max(0, CREATED_COUNT.get() - POOL.size()) * CHUNK.getBytes());
    }

    /**
     * return chunk.
     *
     * @param byteBuffer byteBuffer
     */
    public static void returnChunk(ByteBuffer byteBuffer) {
        if (byteBuffer == null) {
            return;
        }
        if (!POOL.offer(byteBuffer)) {
            CREATED_COUNT.decrementAndGet();
        }
    }
}
