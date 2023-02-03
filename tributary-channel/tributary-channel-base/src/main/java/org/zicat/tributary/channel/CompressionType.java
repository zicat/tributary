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

import static org.zicat.tributary.common.IOUtils.*;

/** CompressionType. */
public enum CompressionType {
    NONE((byte) 1, "none"),
    ZSTD((byte) 2, "zstd"),
    SNAPPY((byte) 3, "snappy");

    private final byte id;
    private final String name;

    CompressionType(byte id, String name) {
        this.id = id;
        this.name = name;
    }

    public byte id() {
        return id;
    }

    /**
     * get type by name.
     *
     * @param name name
     * @return CompressionType
     */
    public static CompressionType getByName(String name) {
        if (name == null) {
            return CompressionType.NONE;
        }
        name = name.trim().toLowerCase();
        if (NONE.name.equals(name)) {
            return NONE;
        } else if (ZSTD.name.equals(name)) {
            return ZSTD;
        } else if (SNAPPY.name.equals(name)) {
            return SNAPPY;
        } else {
            throw new IllegalArgumentException("compression name not found, name " + name);
        }
    }

    /**
     * get type by id.
     *
     * @param b id
     * @return CompressionType
     */
    public static CompressionType getById(byte b) {
        if (b == 1) {
            return NONE;
        } else if (b == 2) {
            return ZSTD;
        } else if (b == 3) {
            return SNAPPY;
        } else {
            throw new IllegalArgumentException("compression type not found, id " + b);
        }
    }

    /**
     * compression byte buffer. only support DirectByteBuffer
     *
     * @param uncompressed byteBuffer
     * @param reusedBuf reusedBuf
     * @return byteBuffer length + compression data
     */
    public ByteBuffer compression(ByteBuffer uncompressed, ByteBuffer reusedBuf)
            throws IOException {
        if (this == ZSTD) {
            return compressionZSTD(uncompressed, reusedBuf);
        } else if (this == NONE) {
            return compressionNone(uncompressed, reusedBuf);
        } else if (this == SNAPPY) {
            return compressionSnappy(uncompressed, reusedBuf);
        } else {
            throw new IllegalArgumentException("compression type not found, id " + id());
        }
    }

    /**
     * decompression byte buffer. only support DirectByteBuffer
     *
     * @param byteBuffer byteBuffer
     * @return byteBuffer
     */
    public ByteBuffer decompression(ByteBuffer byteBuffer, ByteBuffer reusedByteBuffer)
            throws IOException {
        if (this == ZSTD) {
            return decompressionZSTD(byteBuffer, reusedByteBuffer);
        } else if (this == NONE) {
            return decompressionNone(byteBuffer, reusedByteBuffer);
        } else if (this == SNAPPY) {
            return decompressionSnappy(byteBuffer, reusedByteBuffer);
        } else {
            throw new IllegalArgumentException("compression type not found, id " + id());
        }
    }
}
