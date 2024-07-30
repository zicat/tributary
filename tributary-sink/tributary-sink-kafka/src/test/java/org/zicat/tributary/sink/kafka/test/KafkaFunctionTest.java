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

import io.prometheus.client.Counter;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.zicat.tributary.channel.Offset;
import org.zicat.tributary.common.IOUtils;
import org.zicat.tributary.common.records.*;
import org.zicat.tributary.sink.function.Context;
import org.zicat.tributary.sink.function.ContextBuilder;
import org.zicat.tributary.sink.kafka.KafkaFunction;
import org.zicat.tributary.sink.kafka.KafkaFunctionFactory;

import java.nio.charset.StandardCharsets;
import java.util.*;

import static org.zicat.tributary.sink.function.AbstractFunction.OPTION_METRICS_HOST;

/** KafkaFunctionTest. */
public class KafkaFunctionTest {

    private static final String topic = "kt1";
    private static final String groupId = "g2";
    private Record record1;
    private Record record2;
    private Map<String, byte[]> recordsHeader;
    private MockProducer<byte[], byte[]> mockProducer;

    @Before
    public void beforeTest() {
        mockProducer =
                new MockProducer<>(true, new ByteArraySerializer(), new ByteArraySerializer());
        final Map<String, byte[]> recordHeader1 = new HashMap<>();
        recordHeader1.put("rhk1", "rhv1".getBytes());
        record1 = new DefaultRecord(recordHeader1, "rk1".getBytes(), "rv1".getBytes());
        final Map<String, byte[]> recordHeader2 = new HashMap<>();
        recordHeader2.put("rhk2", "rhv2".getBytes());
        record2 = new DefaultRecord(recordHeader2, "rk2".getBytes(), "rv2".getBytes());
        recordsHeader = new HashMap<>();
        recordsHeader.put("rshk1", "rshv1".getBytes());
    }

    @After
    public void afterTest() {
        IOUtils.closeQuietly(mockProducer);
    }

    @Test
    public void test() throws Exception {
        final List<Records> recordsList =
                Collections.singletonList(
                        new DefaultRecords(topic, recordsHeader, Arrays.asList(record1, record2)));
        try (MockKafkaFunction function = new MockKafkaFunction()) {
            final ContextBuilder builder =
                    new ContextBuilder()
                            .id("id2")
                            .partitionId(0)
                            .groupId(groupId)
                            .startOffset(new Offset(0, 0))
                            .topic(topic);
            builder.addCustomProperty(OPTION_METRICS_HOST, "localhost");
            function.open(builder.build());

            final Offset groupOffset = new Offset(2, 0);
            function.process(groupOffset, recordsList.iterator());

            assertData(record1, mockProducer.history().get(0), recordsHeader);
            assertData(record2, mockProducer.history().get(1), recordsHeader);
            Assert.assertEquals(groupOffset, function.committableOffset());

            function.process(groupOffset.skipNextSegmentHead(), recordsList.iterator());
            assertData(record1, mockProducer.history().get(0), recordsHeader);
            assertData(record2, mockProducer.history().get(1), recordsHeader);
            Assert.assertEquals(groupOffset.skipNextSegmentHead(), function.committableOffset());

            for (ProducerRecord<byte[], byte[]> r : mockProducer.history()) {
                Assert.assertEquals(topic, r.topic());
            }

            Assert.assertEquals(function.getCount().get(), 4, 0.1);
        }
    }

    @Test
    public void testDynamicTopic() throws Exception {
        final List<Records> recordsList =
                Collections.singletonList(
                        new DefaultRecords(topic, recordsHeader, Arrays.asList(record1, record2)));
        try (KafkaFunction function = new MockKafkaFunction()) {
            final ContextBuilder builder =
                    new ContextBuilder()
                            .id("id")
                            .partitionId(0)
                            .groupId(groupId)
                            .startOffset(new Offset(0, 0))
                            .topic(topic);
            builder.addCustomProperty(OPTION_METRICS_HOST, "localhost");
            builder.addCustomProperty(KafkaFunctionFactory.OPTION_TOPIC, "aa_${topic}");
            function.open(builder.build());
            final Offset groupOffset = new Offset(2, 0);
            function.process(groupOffset, recordsList.iterator());
            assertData(record1, mockProducer.history().get(0), recordsHeader);
            assertData(record2, mockProducer.history().get(1), recordsHeader);
            for (ProducerRecord<byte[], byte[]> r : mockProducer.history()) {
                Assert.assertEquals("aa_" + topic, r.topic());
            }
        }
    }

    private static void assertData(
            Record record,
            ProducerRecord<byte[], byte[]> kafkaRecord,
            Map<String, byte[]> recordsHeader) {
        Assert.assertArrayEquals(record.key(), kafkaRecord.key());
        Assert.assertArrayEquals(record.value(), kafkaRecord.value());
        final Map<String, byte[]> kafkaHeaders = new HashMap<>();
        for (Header header : kafkaRecord.headers()) {
            kafkaHeaders.put(header.key(), header.value());
        }
        Assert.assertEquals(
                topic,
                new String(
                        kafkaHeaders.get(KafkaFunction.HEAD_KEY_ORIGIN_TOPIC),
                        StandardCharsets.UTF_8));
        Assert.assertTrue(kafkaHeaders.containsKey(RecordsUtils.HEAD_KEY_SENT_TS));

        for (String key : record.headers().keySet()) {
            Assert.assertArrayEquals(record.headers().get(key), kafkaHeaders.get(key));
        }

        for (String key : recordsHeader.keySet()) {
            Assert.assertArrayEquals(recordsHeader.get(key), kafkaHeaders.get(key));
        }

        Assert.assertEquals(record.headers().size() + 3, kafkaHeaders.size());
    }

    /** MockKafkaFunction. */
    private class MockKafkaFunction extends KafkaFunction {

        @Override
        protected Producer<byte[], byte[]> createProducer(Context context) {
            return mockProducer;
        }

        @Override
        public void close() {}

        public Counter.Child getCount() {
            return sinkCounterChild;
        }
    }
}
