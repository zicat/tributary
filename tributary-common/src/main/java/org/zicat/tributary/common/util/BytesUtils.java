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

package org.zicat.tributary.common.util;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

/** BytesUtils. */
public class BytesUtils {

    /**
     * int 2 bytes.
     *
     * @param value value
     * @return bytes
     */
    public static byte[] toBytes(int value) {
        return new byte[] {
            (byte) (value >>> 24), (byte) (value >>> 16), (byte) (value >>> 8), (byte) value
        };
    }

    /**
     * to int.
     *
     * @param bytes bytes
     * @return int
     */
    public static int toInt(byte[] bytes) {
        if (bytes.length != 4) {
            throw new IllegalArgumentException("Array length must be 4");
        }
        return (bytes[0] & 0xFF) << 24
                | (bytes[1] & 0xFF) << 16
                | (bytes[2] & 0xFF) << 8
                | (bytes[3] & 0xFF);
    }

    /**
     * to string with utf-8.
     *
     * @param byteBuffer byteBuffer
     * @return String
     */
    public static String toString(ByteBuffer byteBuffer) {
        return toString(byteBuffer, byteBuffer.remaining());
    }

    /**
     * to string with utf-8.
     *
     * @param byteBuffer byteBuffer
     * @param size size
     * @return string
     */
    public static String toString(ByteBuffer byteBuffer, int size) {
        return new String(toBytes(byteBuffer, size), StandardCharsets.UTF_8);
    }

    /**
     * to bytes.
     *
     * @param byteBuffer byteBuffer
     * @return bytes
     */
    public static byte[] toBytes(ByteBuffer byteBuffer) {
        return toBytes(byteBuffer, byteBuffer.remaining());
    }

    /**
     * to bytes.
     *
     * @param byteBuffer byteBuffer
     * @param size size
     * @return byte array
     */
    public static byte[] toBytes(ByteBuffer byteBuffer, int size) {
        if (size > byteBuffer.remaining()) {
            throw new IllegalArgumentException("size must be less than remaining bytes");
        }
        final byte[] bytes = new byte[size];
        byteBuffer.get(bytes);
        return bytes;
    }

    /**
     * get vint bytes.
     *
     * @param byteBuffer byteBuffer
     * @return byte array
     */
    public static byte[] toVIntBytes(ByteBuffer byteBuffer) {
        return toBytes(byteBuffer, VIntUtil.readVInt(byteBuffer));
    }

    /**
     * get vint string.
     *
     * @param byteBuffer byteBuffer
     * @return string
     */
    public static String toVIntString(ByteBuffer byteBuffer) {
        return toString(byteBuffer, VIntUtil.readVInt(byteBuffer));
    }
}
