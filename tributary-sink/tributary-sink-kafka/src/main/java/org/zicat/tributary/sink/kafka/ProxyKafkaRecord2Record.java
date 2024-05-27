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

package org.zicat.tributary.sink.kafka;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.record.MutableRecordBatch;
import org.apache.kafka.common.record.Record;
import org.zicat.tributary.common.KafkaRecordSerDeSer;
import org.zicat.tributary.common.KafkaRecordSerDeSer.KafkaTopicPartitionRecords;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import static org.zicat.tributary.common.BytesUtils.toBytes;

/** ProxyKafkaRecord2Record. */
public class ProxyKafkaRecord2Record implements Byte2Record {
    private static final KafkaRecordSerDeSer DE_SER = new KafkaRecordSerDeSer();
    public static final String HEAD_KEY_SENT_TS = "sent_ts";
    public static final String HEAD_KEY_ORIGIN_TOPIC = "origin_topic";
    private static final String TOPIC_TEMPLATE = "${topic}";

    private final String defaultTopic;

    public ProxyKafkaRecord2Record(String defaultTopic) {
        this.defaultTopic = defaultTopic;
    }

    @Override
    public List<ProducerRecord<byte[], byte[]>> convert(byte[] value) {
        final List<ProducerRecord<byte[], byte[]>> result = new ArrayList<>();
        final KafkaTopicPartitionRecords records = DE_SER.fromBytes(value);
        final byte[] recordTopic = records.getTopic();
        final String targetTopic = targetTopic(records.getTopic());
        final Iterator<MutableRecordBatch> it = records.getMemoryRecords().batchIterator();
        while (it.hasNext()) {
            final MutableRecordBatch batch = it.next();
            for (Record record : batch) {
                final byte[] key = record.hasKey() ? toBytes(record.key()) : null;
                final byte[] values = record.hasValue() ? toBytes(record.value()) : null;
                final List<Header> headers = new ArrayList<>(records.getHeaders());
                headers.addAll(extraHeaders(recordTopic));
                headers.addAll(Arrays.asList(record.headers()));
                final ProducerRecord<byte[], byte[]> producerRecord =
                        new ProducerRecord<>(targetTopic, null, key, values, headers);
                result.add(producerRecord);
            }
        }
        return result;
    }

    /**
     * get target topic.
     *
     * @param recordTopic recordTopic
     * @return topic
     */
    private String targetTopic(byte[] recordTopic) {
        final String sourceTopic = new String(recordTopic, StandardCharsets.UTF_8);
        if (defaultTopic == null) {
            return sourceTopic;
        }
        return defaultTopic.replace(TOPIC_TEMPLATE, sourceTopic);
    }

    /**
     * extra headers.
     *
     * @param recordTopic recordTopic
     * @return headers
     */
    private List<Header> extraHeaders(byte[] recordTopic) {
        final List<Header> headers = new ArrayList<>();
        final int sentTs = (int) (System.currentTimeMillis() / 1000L);
        headers.add(new RecordHeader(HEAD_KEY_SENT_TS, toBytes(sentTs)));
        headers.add(new RecordHeader(HEAD_KEY_ORIGIN_TOPIC, recordTopic));
        return headers;
    }
}
