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

package org.zicat.tributary.sink.kafka.test;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.SimpleRecord;
import org.junit.Assert;
import org.junit.Test;
import org.zicat.tributary.common.BytesUtils;
import org.zicat.tributary.common.KafkaRecordSerDeSer;
import org.zicat.tributary.sink.kafka.ProxyKafkaRecord2Record;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.zicat.tributary.sink.kafka.ProxyKafkaRecord2Record.HEAD_KEY_ORIGIN_TOPIC;
import static org.zicat.tributary.sink.kafka.ProxyKafkaRecord2Record.HEAD_KEY_SENT_TS;

/** ProxyKafkaRecord2RecordTest. */
public class ProxyKafkaRecord2RecordTest {

    private static final KafkaRecordSerDeSer DE_SER = new KafkaRecordSerDeSer();

    @Test
    public void test() {
        final long start = System.currentTimeMillis();
        final ProxyKafkaRecord2Record record2Record = new ProxyKafkaRecord2Record(null);

        final TopicPartition tp = new TopicPartition("t1", 1);
        final List<Header> headers =
                Arrays.asList(
                        new RecordHeader("th1", "tv1".getBytes()),
                        new RecordHeader("th2", "tv2".getBytes()));
        final List<Header> record1Headers =
                Arrays.asList(
                        new RecordHeader("h1", "v1".getBytes()),
                        new RecordHeader("m1", "n1".getBytes()));
        final List<Header> record2Headers =
                Arrays.asList(
                        new RecordHeader("h2", "v2".getBytes()),
                        new RecordHeader("m2", "n2".getBytes()));
        final MemoryRecords memoryRecords =
                MemoryRecords.withRecords(
                        CompressionType.SNAPPY,
                        new SimpleRecord(
                                start,
                                "k1".getBytes(),
                                "v1".getBytes(),
                                record1Headers.toArray(new Header[] {})),
                        new SimpleRecord(
                                start,
                                "k2".getBytes(),
                                "v2".getBytes(),
                                record2Headers.toArray(new Header[] {})));

        final ByteBuffer buffer = DE_SER.toByteBuffer(tp, headers, memoryRecords);
        final List<ProducerRecord<byte[], byte[]>> records =
                record2Record.convert(BytesUtils.toBytes(buffer));

        final long end = System.currentTimeMillis();

        Assert.assertEquals(2, records.size());
        for (int i = 0; i < records.size(); i++) {
            final ProducerRecord<byte[], byte[]> record = records.get(i);
            Assert.assertEquals(tp.topic(), record.topic());
            Assert.assertNull(record.partition());

            Assert.assertArrayEquals(("k" + (i + 1)).getBytes(), record.key());
            Assert.assertArrayEquals(("v" + (i + 1)).getBytes(), record.value());

            List<Header> recordHeaders = Arrays.asList(record.headers().toArray());
            Assert.assertEquals(6, recordHeaders.size());

            for (Header header : headers) {
                Assert.assertTrue(recordHeaders.contains(header));
            }

            if ("k1".equals(new String(record.key()))) {
                for (Header header : record1Headers) {
                    Assert.assertTrue(recordHeaders.contains(header));
                }
            } else {
                for (Header header : record2Headers) {
                    Assert.assertTrue(recordHeaders.contains(header));
                }
            }

            final List<Header> topicHeaders =
                    recordHeaders.stream()
                            .filter(header -> header.key().equals(HEAD_KEY_ORIGIN_TOPIC))
                            .collect(Collectors.toList());
            Assert.assertEquals(1, topicHeaders.size());
            Assert.assertEquals(tp.topic(), new String(topicHeaders.get(0).value()));

            final List<Header> sentTsHeader =
                    recordHeaders.stream()
                            .filter(header -> header.key().equals(HEAD_KEY_SENT_TS))
                            .collect(Collectors.toList());
            Assert.assertEquals(1, sentTsHeader.size());
            Assert.assertTrue(BytesUtils.toInt(sentTsHeader.get(0).value()) >= (start / 1000));
            Assert.assertTrue(BytesUtils.toInt(sentTsHeader.get(0).value()) <= (end / 1000));
        }
    }
}
