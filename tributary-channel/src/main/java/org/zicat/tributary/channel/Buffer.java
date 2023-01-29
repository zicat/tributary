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

/** Buffer. */
public class Buffer {

    protected final ByteBuffer resultBuf;
    protected ByteBuffer reusedBuf;

    public Buffer() {
        this(null, null);
    }

    public Buffer(ByteBuffer resultBuf, ByteBuffer reusedBuf) {
        this.resultBuf = resultBuf;
        this.reusedBuf = reusedBuf;
    }

    /**
     * reset buffer.
     *
     * @return this
     */
    public Buffer reset() {
        if (resultBuf != null && resultBuf.hasRemaining()) {
            resultBuf.clear().flip();
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
}
