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

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.zicat.tributary.common.util.BytesUtils.toVIntString;
import static org.zicat.tributary.common.util.VIntUtil.*;
import static org.zicat.tributary.common.records.Record.headBuffers;
import static org.zicat.tributary.common.records.Record.parseHeaders;

/** Records. */
public interface Records extends Iterable<Record> {

    /**
     * Get the topic of record.
     *
     * @return topic
     */
    String topic();

    /**
     * get headers of records.
     *
     * @return headers
     */
    Map<String, byte[]> headers();

    /**
     * record count.
     *
     * @return int
     */
    int count();

    default ByteBuffer toByteBuffer() {

        final byte[] topic = topic().getBytes(StandardCharsets.UTF_8);
        final List<ByteBuffer> headerBuffers = headBuffers(headers());
        final List<ByteBuffer> recordBuffers = recordBuffer();

        int size = vIntEncodeLength(topic.length);

        size += vIntLength(headerBuffers.size());
        for (ByteBuffer headBuffer : headerBuffers) {
            size += headBuffer.remaining();
        }

        size += vIntLength(recordBuffers.size());
        for (ByteBuffer recordBuffer : recordBuffers) {
            size += recordBuffer.remaining();
        }

        final ByteBuffer result = ByteBuffer.allocate(size);
        putVInt(result, topic.length);
        result.put(topic);

        putVInt(result, headerBuffers.size());
        for (ByteBuffer headerBuffer : headerBuffers) {
            result.put(headerBuffer);
        }

        putVInt(result, recordBuffers.size());
        for (ByteBuffer recordBuffer : recordBuffers) {
            result.put(recordBuffer);
        }
        result.flip();
        return result;
    }

    /**
     * convert record to buffer.
     *
     * @return list byteBuffer.
     */
    default List<ByteBuffer> recordBuffer() {
        final List<ByteBuffer> recordBuffers = new ArrayList<>(count());
        for (Record value : this) {
            final ByteBuffer record = value.toByteBuffer();
            recordBuffers.add(record);
        }
        return recordBuffers;
    }

    /**
     * parse bytebuffer to Records.
     *
     * @param byteBuffer byteBuffer
     * @return Records
     */
    static Records parse(ByteBuffer byteBuffer) {
        final String topic = toVIntString(byteBuffer);
        final Map<String, byte[]> headers = parseHeaders(byteBuffer);
        final int recordCount = readVInt(byteBuffer);
        final List<Record> records = new ArrayList<>(recordCount);
        for (int i = 0; i < recordCount; i++) {
            records.add(Record.parse(byteBuffer));
        }
        return new DefaultRecords(topic, headers, records);
    }

    /**
     * parse byte buffer to Records.
     *
     * @param bytes bytes
     * @return Records
     */
    static Records parse(byte[] bytes) {
        return parse(ByteBuffer.wrap(bytes));
    }
}
