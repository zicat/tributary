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

package org.zicat.tributary.common.records;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import org.zicat.tributary.common.util.VIntUtil;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import static org.zicat.tributary.common.util.BytesUtils.toVIntBytes;
import static org.zicat.tributary.common.util.BytesUtils.toVIntString;
import static org.zicat.tributary.common.util.VIntUtil.*;

/** Record. */
@JsonTypeInfo(defaultImpl = JsonRecord.class, use = JsonTypeInfo.Id.CLASS)
public interface Record {

    /**
     * Get the headers of record.
     *
     * @return map
     */
    Map<String, byte[]> headers();

    /**
     * Get the key of record.
     *
     * @return byte array
     */
    byte[] key();

    /**
     * Get the value of record.
     *
     * @return byte value
     */
    byte[] value();

    /**
     * to byte buffer.
     *
     * @return byte buffer.
     */
    default ByteBuffer toByteBuffer() {

        final List<ByteBuffer> headerBuffers = headBuffers(headers());
        int size = vIntLength(headerBuffers.size());
        for (ByteBuffer buffer : headerBuffers) {
            size += buffer.remaining();
        }
        final byte[] key = key();
        final int keySize = key.length;
        final byte[] value = value();
        final int valueSize = value.length;
        size += vIntEncodeLength(keySize);
        size += vIntEncodeLength(valueSize);

        final ByteBuffer result = ByteBuffer.allocate(size);
        putVInt(result, headerBuffers.size());
        for (ByteBuffer headerBuffer : headerBuffers) {
            result.put(headerBuffer);
        }
        putVInt(result, keySize);
        result.put(key);
        putVInt(result, valueSize);
        result.put(value);
        result.flip();
        return result;
    }

    /**
     * head buffers.
     *
     * @param headers headers
     * @return byte list
     */
    static List<ByteBuffer> headBuffers(Map<String, byte[]> headers) {
        if (headers == null || headers.isEmpty()) {
            return new ArrayList<>();
        }
        final List<ByteBuffer> result = new ArrayList<>(headers.size());
        for (Entry<String, byte[]> entry : headers.entrySet()) {
            final byte[] key = entry.getKey().getBytes(StandardCharsets.UTF_8);
            final byte[] value = entry.getValue();
            final ByteBuffer headBuffer =
                    ByteBuffer.allocate(
                            vIntEncodeLength(key.length) + vIntEncodeLength(value.length));
            putVInt(headBuffer, key.length);
            headBuffer.put(key);
            putVInt(headBuffer, value.length);
            headBuffer.put(value);
            headBuffer.flip();
            result.add(headBuffer);
        }
        return result;
    }

    /**
     * parse record by byte buffer.
     *
     * @param byteBuffer byteBuffer
     * @return Record
     */
    static Record parse(ByteBuffer byteBuffer) {
        final Map<String, byte[]> headers = parseHeaders(byteBuffer);
        final byte[] key = toVIntBytes(byteBuffer);
        final byte[] value = toVIntBytes(byteBuffer);
        return new DefaultRecord(headers, key, value);
    }

    /**
     * parse headers.
     *
     * @param byteBuffer byteBuffer
     * @return map
     */
    static Map<String, byte[]> parseHeaders(ByteBuffer byteBuffer) {
        final int headSize = VIntUtil.readVInt(byteBuffer);
        final Map<String, byte[]> headers = new HashMap<>();
        for (int i = 0; i < headSize; i++) {
            headers.put(toVIntString(byteBuffer), toVIntBytes(byteBuffer));
        }
        return headers;
    }
}
