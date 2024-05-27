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

package org.zicat.tributary.common.test;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.record.MemoryRecords;
import org.junit.Assert;
import org.junit.Test;
import org.zicat.tributary.common.KafkaRecordSerDeSer;
import org.zicat.tributary.common.KafkaRecordSerDeSer.KafkaTopicPartitionRecords;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;

/** KafkaRecordSerDeSerTest. */
public class KafkaRecordSerDeSerTest {

    @Test
    public void test() {
        KafkaRecordSerDeSer serDeSer = new KafkaRecordSerDeSer();
        final TopicPartition topicPartition = new TopicPartition("111", 1);
        final MemoryRecords memoryRecords = createMemoryRecords(100);
        final ByteBuffer byteBuffer = serDeSer.toByteBuffer(topicPartition, null, memoryRecords);
        // check whether is fulfilled
        Assert.assertEquals(byteBuffer.remaining(), byteBuffer.capacity());
        final KafkaTopicPartitionRecords topicPartitionRecords = serDeSer.fromBytes(byteBuffer);
        Assert.assertEquals(
                topicPartition.topic(),
                new String(topicPartitionRecords.getTopic(), StandardCharsets.UTF_8));
        Assert.assertEquals(topicPartition.partition(), topicPartitionRecords.getPartition());
        Assert.assertEquals(memoryRecords, topicPartitionRecords.getMemoryRecords());

        final List<Header> headers =
                Arrays.asList(
                        new RecordHeader("h1", "v1".getBytes()),
                        new RecordHeader("h2", "v2".getBytes()));
        final MemoryRecords memoryRecords2 = createMemoryRecords(200);
        final ByteBuffer byteBuffer2 =
                serDeSer.toByteBuffer(topicPartition, headers, memoryRecords2);
        // check whether is fulfilled
        Assert.assertEquals(byteBuffer2.remaining(), byteBuffer2.capacity());
        final KafkaTopicPartitionRecords topicPartitionRecords2 = serDeSer.fromBytes(byteBuffer2);
        Assert.assertEquals(memoryRecords2, topicPartitionRecords2.getMemoryRecords());
        Assert.assertEquals(headers, topicPartitionRecords2.getHeaders());
    }

    /**
     * create memory records.
     *
     * @param size size
     * @return MemoryRecords
     */
    private static MemoryRecords createMemoryRecords(int size) {
        final ByteBuffer byteBuffer = ByteBuffer.allocate(size);
        for (int i = 0; i < size; i++) {
            byteBuffer.put((byte) i);
        }
        byteBuffer.flip();
        return MemoryRecords.readableRecords(byteBuffer);
    }
}
