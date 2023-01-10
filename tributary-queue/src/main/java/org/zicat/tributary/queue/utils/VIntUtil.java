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

package org.zicat.tributary.queue.utils;

import java.nio.ByteBuffer;

/** VIntUtil. */
public class VIntUtil {

    public static final int VINT_1_BYTE_LIMIT = (1 << 7);
    public static final int VINT_2_BYTE_LIMIT = (1 << 14);
    public static final int VINT_3_BYTE_LIMIT = (1 << 21);
    public static final int VINT_4_BYTE_LIMIT = (1 << 28);

    /**
     * get the real length with vint encode.
     *
     * @param length length
     * @return length
     */
    public static int vIntLength(int length) {
        if (length < 0) {
            throw new IllegalArgumentException("length < 0, param length = " + length);
        }
        if (length < VINT_1_BYTE_LIMIT) {
            return 1 + length;
        } else if (length < VINT_2_BYTE_LIMIT) {
            return 2 + length;
        } else if (length < VINT_3_BYTE_LIMIT) {
            return 3 + length;
        } else if (length < VINT_4_BYTE_LIMIT) {
            return 4 + length;
        }
        return 5 + length;
    }

    /**
     * put vint to buffer.
     *
     * @param byteBuffer byteBuffer
     * @param value value
     */
    public static void putVInt(ByteBuffer byteBuffer, int value) {
        int dataSize = value;
        while ((dataSize & ~0x7F) != 0) {
            byteBuffer.put((byte) ((dataSize & 0x7f) | 0x80));
            dataSize >>>= 7;
        }
        byteBuffer.put((byte) dataSize);
    }

    /**
     * read vint encode value.
     *
     * @param byteBuffer byteBuffer
     * @return return
     */
    public static int readVInt(ByteBuffer byteBuffer) {
        byte b = byteBuffer.get();
        int length = b & 0x7F;
        for (int shift = 7; (b & 0x80) != 0; shift += 7) {
            b = byteBuffer.get();
            length |= (b & 0x7F) << shift;
        }
        return length;
    }
}
