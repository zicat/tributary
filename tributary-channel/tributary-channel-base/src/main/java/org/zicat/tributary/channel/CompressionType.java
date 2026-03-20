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

import static org.zicat.tributary.common.util.IOUtils.*;

/** CompressionType. */
public enum CompressionType {
    NONE((byte) 1, "none") {
        @Override
        public ByteBuffer compression(ByteBuffer byteBuffer, ByteBuffer reusedBuf) {
            return compressionNone(byteBuffer, reusedBuf);
        }

        @Override
        public ByteBuffer decompression(ByteBuffer byteBuffer, ByteBuffer reusedBuffer) {
            return decompressionNone(byteBuffer, reusedBuffer);
        }
    },
    ZSTD((byte) 2, "zstd") {
        @Override
        public ByteBuffer compression(ByteBuffer byteBuffer, ByteBuffer reusedBuf) {
            return compressionZSTD(byteBuffer, reusedBuf);
        }

        @Override
        public ByteBuffer decompression(ByteBuffer byteBuffer, ByteBuffer reusedBuffer) {
            return decompressionZSTD(byteBuffer, reusedBuffer);
        }
    },
    SNAPPY((byte) 3, "snappy") {
        @Override
        public ByteBuffer compression(ByteBuffer byteBuffer, ByteBuffer reusedBuf)
                throws IOException {
            return compressionSnappy(byteBuffer, reusedBuf);
        }

        @Override
        public ByteBuffer decompression(ByteBuffer byteBuffer, ByteBuffer reusedBuffer)
                throws IOException {
            return decompressionSnappy(byteBuffer, reusedBuffer);
        }
    },
    LZ4((byte) 4, "lz4") {
        @Override
        public ByteBuffer compression(ByteBuffer uncompressed, ByteBuffer reusedBuf)
                throws IOException {
            return compressionLZ4(uncompressed, reusedBuf);
        }

        @Override
        public ByteBuffer decompression(ByteBuffer byteBuffer, ByteBuffer reusedBuffer)
                throws IOException {
            return decompressionLZ4(byteBuffer, reusedBuffer);
        }
    };

    private final byte id;
    private final String name;

    CompressionType(byte id, String name) {
        this.id = id;
        this.name = name;
    }

    public byte id() {
        return id;
    }

    @Override
    public String toString() {
        return name;
    }

    /**
     * compression byte buffer. only support DirectByteBuffer
     *
     * @param byteBuffer byteBuffer
     * @param reusedBuffer reusedBuffer
     * @return byteBuffer length + compression data
     */
    public abstract ByteBuffer compression(ByteBuffer byteBuffer, ByteBuffer reusedBuffer)
            throws IOException;

    /**
     * decompression byte buffer. only support DirectByteBuffer
     *
     * @param byteBuffer byteBuffer
     * @param reusedBuffer reusedBuffer
     * @return byteBuffer
     */
    public abstract ByteBuffer decompression(ByteBuffer byteBuffer, ByteBuffer reusedBuffer)
            throws IOException;

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
        for (CompressionType compressionType : CompressionType.values()) {
            if (compressionType.name.equals(name)) {
                return compressionType;
            }
        }
        throw new IllegalArgumentException("compression name not found, name " + name);
    }

    /**
     * get type by id.
     *
     * @param b id
     * @return CompressionType
     */
    public static CompressionType getById(byte b) {
        for (CompressionType compressionType : CompressionType.values()) {
            if (compressionType.id == b) {
                return compressionType;
            }
        }
        throw new IllegalArgumentException("compression type not found, id " + b);
    }
}
