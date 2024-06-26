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

import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.zicat.tributary.channel.GroupOffset;
import org.zicat.tributary.common.IOUtils;
import org.zicat.tributary.common.records.*;
import org.zicat.tributary.sink.function.ContextBuilder;
import org.zicat.tributary.sink.kafka.DefaultKafkaFunction;
import org.zicat.tributary.sink.kafka.DefaultKafkaFunctionFactory;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.zicat.tributary.sink.function.AbstractFunction.OPTION_METRICS_HOST;

/** DefaultKafkaFunctionTest. */
public class DefaultKafkaFunctionTest {

    private MockProducer<byte[], byte[]> mockProducer;

    @Before
    public void beforeTest() {
        mockProducer =
                new MockProducer<>(true, new ByteArraySerializer(), new ByteArraySerializer());
    }

    @After
    public void afterTest() {
        IOUtils.closeQuietly(mockProducer);
    }

    @Test
    public void test() throws Exception {
        final String topic = "kt1";
        final String groupId = "g2";
        final Map<String, byte[]> recordHeader1 = new HashMap<>();
        recordHeader1.put("rhk1", "rhv1".getBytes());
        final Record record1 = new DefaultRecord(recordHeader1, "rk1".getBytes(), "rv1".getBytes());
        final Map<String, byte[]> recordHeader2 = new HashMap<>();
        recordHeader2.put("rhk2", "rhv2".getBytes());
        Record record2 = new DefaultRecord(recordHeader2, "rk2".getBytes(), "rv2".getBytes());

        final Map<String, byte[]> recordsHeader = new HashMap<>();
        recordHeader1.put("rshk1", "rshv1".getBytes());
        final Records records =
                new DefaultRecords(topic, 1, recordsHeader, Arrays.asList(record1, record2));

        try (DefaultKafkaFunction kafkaFunction = new MockDefaultKafkaFunction()) {
            final ContextBuilder builder =
                    new ContextBuilder()
                            .id("id2")
                            .partitionId(0)
                            .startGroupOffset(new GroupOffset(0, 0, groupId))
                            .topic(topic);
            builder.addCustomProperty(OPTION_METRICS_HOST.key(), "localhost");
            kafkaFunction.open(builder.build());

            final GroupOffset groupOffset = new GroupOffset(2, 0, groupId);
            kafkaFunction.process(groupOffset, Collections.singletonList(records).iterator());

            assertData(topic, record1, mockProducer.history().get(0));
            assertData(topic, record2, mockProducer.history().get(1));
            Assert.assertEquals(groupOffset, kafkaFunction.committableOffset());

            kafkaFunction.process(
                    groupOffset.skipNextSegmentHead(),
                    Collections.singletonList(records).iterator());
            assertData(topic, record1, mockProducer.history().get(0));
            assertData(topic, record2, mockProducer.history().get(1));
            Assert.assertEquals(
                    groupOffset.skipNextSegmentHead(), kafkaFunction.committableOffset());

            for (ProducerRecord<byte[], byte[]> r : mockProducer.history()) {
                Assert.assertEquals(topic, r.topic());
            }
        }

        mockProducer.clear();

        final String topic2 = "kt2";
        final Records records2 =
                new DefaultRecords(topic2, 1, recordsHeader, Arrays.asList(record1, record2));
        try (DefaultKafkaFunction kafkaFunction = new MockDefaultKafkaFunction()) {
            final ContextBuilder builder =
                    new ContextBuilder()
                            .id("id2")
                            .partitionId(0)
                            .startGroupOffset(new GroupOffset(0, 0, groupId))
                            .topic(topic2);
            builder.addCustomProperty(OPTION_METRICS_HOST.key(), "localhost");
            builder.addCustomProperty(
                    DefaultKafkaFunctionFactory.OPTION_TOPIC.key(), "aa_${topic}");
            kafkaFunction.open(builder.build());
            final GroupOffset groupOffset = new GroupOffset(2, 0, groupId);
            kafkaFunction.process(groupOffset, Collections.singletonList(records2).iterator());
            assertData(topic2, record1, mockProducer.history().get(0));
            assertData(topic2, record2, mockProducer.history().get(1));

            for (ProducerRecord<byte[], byte[]> r : mockProducer.history()) {
                Assert.assertEquals("aa_" + topic2, r.topic());
            }
        }
    }

    private static void assertData(
            String originTopic, Record record, ProducerRecord<byte[], byte[]> kafkaRecord) {
        Assert.assertArrayEquals(record.key(), kafkaRecord.key());
        Assert.assertArrayEquals(record.value(), kafkaRecord.value());
        final Map<String, byte[]> kafkaHeaders = new HashMap<>();
        for (Header header : kafkaRecord.headers()) {
            kafkaHeaders.put(header.key(), header.value());
        }
        Assert.assertEquals(
                originTopic,
                new String(
                        kafkaHeaders.get(DefaultKafkaFunction.HEAD_KEY_ORIGIN_TOPIC),
                        StandardCharsets.UTF_8));
        Assert.assertTrue(kafkaHeaders.containsKey(RecordsUtils.HEAD_KEY_SENT_TS));

        for (String key : record.headers().keySet()) {
            Assert.assertArrayEquals(record.headers().get(key), kafkaHeaders.get(key));
        }
        Assert.assertEquals(record.headers().size() + 2, kafkaHeaders.size());
    }

    /** MockDefaultKafkaFunction. */
    private class MockDefaultKafkaFunction extends DefaultKafkaFunction {

        @Override
        protected Producer<byte[], byte[]> getOrCreateProducer() {
            return mockProducer;
        }
    }
}
