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

package org.zicat.tributary.common;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.record.MemoryRecords;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.zicat.tributary.common.VIntUtil.*;

/** KafkaRecordSerDeSer. */
public class KafkaRecordSerDeSer {

    /**
     * to byte buffer.
     *
     * @param topicPartition topicPartition
     * @param memoryRecords memoryRecords
     * @return ByteBuffer
     */
    public ByteBuffer toByteBuffer(
            TopicPartition topicPartition, List<Header> extraHeaders, MemoryRecords memoryRecords) {
        final byte[] topics = topicPartition.topic().getBytes(StandardCharsets.UTF_8);
        final int partition = topicPartition.partition();
        int size =
                vIntEncodeLength(topics.length)
                        + vIntLength(partition)
                        + vIntEncodeLength(memoryRecords.sizeInBytes());

        final List<ByteBuffer> headBuffers = headBuffers(extraHeaders);
        size += vIntLength(headBuffers.size());
        for (ByteBuffer headBuffer : headBuffers) {
            size += headBuffer.remaining();
        }

        final ByteBuffer byteBuffer = ByteBuffer.allocate(size);
        // write topic
        putVInt(byteBuffer, topics.length);
        byteBuffer.put(topics);
        // write partition
        putVInt(byteBuffer, partition);
        // write memory records
        putVInt(byteBuffer, memoryRecords.sizeInBytes());
        byteBuffer.put(memoryRecords.buffer());

        // write header.
        putVInt(byteBuffer, headBuffers.size());
        for (ByteBuffer header : headBuffers) {
            byteBuffer.put(header);
        }
        // flip
        byteBuffer.flip();
        return byteBuffer;
    }

    /**
     * headBuffers.
     *
     * @param headers headers
     * @return list buffer.
     */
    private static List<ByteBuffer> headBuffers(List<Header> headers) {
        if (headers == null || headers.isEmpty()) {
            return Collections.emptyList();
        }
        final List<ByteBuffer> result = new ArrayList<>(headers.size());
        for (Header header : headers) {
            result.add(headBuf(header));
        }
        return result;
    }

    /**
     * header buf.
     *
     * @param header header
     * @return ByteBuffer
     */
    private static ByteBuffer headBuf(Header header) {
        final byte[] key = header.key().getBytes(StandardCharsets.UTF_8);
        final byte[] value = header.value();
        final ByteBuffer headBuf =
                ByteBuffer.allocate(vIntEncodeLength(key.length) + vIntEncodeLength(value.length));
        putVInt(headBuf, key.length);
        headBuf.put(key);
        putVInt(headBuf, value.length);
        headBuf.put(value);
        headBuf.flip();
        return headBuf;
    }

    /**
     * from bytes.
     *
     * @param bytes bytes
     * @return KafkaTopicPartitionRecords
     */
    public KafkaTopicPartitionRecords fromBytes(byte[] bytes) {
        return fromBytes(ByteBuffer.wrap(bytes));
    }

    /**
     * from bytes.
     *
     * @param byteBuffer byteBuffer
     * @return KafkaTopicPartitionRecords
     */
    public KafkaTopicPartitionRecords fromBytes(ByteBuffer byteBuffer) {
        final int topicSize = readVInt(byteBuffer);
        final byte[] topics = new byte[topicSize];
        byteBuffer.get(topics);
        final int partition = readVInt(byteBuffer);
        final int memoryRecordsSize = readVInt(byteBuffer);
        final byte[] memoryRecords = new byte[memoryRecordsSize];
        byteBuffer.get(memoryRecords);
        // read headers
        final int headerCount = readVInt(byteBuffer);
        final List<Header> headers = new ArrayList<>(headerCount);
        for (int i = 0; i < headerCount; i++) {
            final int keyLength = readVInt(byteBuffer);
            final byte[] key = new byte[keyLength];
            byteBuffer.get(key);
            final int valueLength = readVInt(byteBuffer);
            final byte[] value = new byte[valueLength];
            byteBuffer.get(value);
            headers.add(new RecordHeader(new String(key, StandardCharsets.UTF_8), value));
        }
        return new KafkaTopicPartitionRecords(
                topics,
                partition,
                headers,
                MemoryRecords.readableRecords(ByteBuffer.wrap(memoryRecords)));
    }

    /** KafkaTopicPartitionRecords. */
    public static class KafkaTopicPartitionRecords {
        private final byte[] topic;
        private final int partition;
        private final List<Header> headers;
        private final MemoryRecords memoryRecords;

        public KafkaTopicPartitionRecords(
                byte[] topic, int partition, List<Header> headers, MemoryRecords memoryRecords) {
            this.topic = topic;
            this.partition = partition;
            this.headers = headers;
            this.memoryRecords = memoryRecords;
        }

        public byte[] getTopic() {
            return topic;
        }

        public int getPartition() {
            return partition;
        }

        public List<Header> getHeaders() {
            return headers;
        }

        public MemoryRecords getMemoryRecords() {
            return memoryRecords;
        }
    }
}
