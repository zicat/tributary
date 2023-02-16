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
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.zicat.tributary.channel.GroupOffset;
import org.zicat.tributary.common.IOUtils;
import org.zicat.tributary.sink.function.Context;
import org.zicat.tributary.sink.function.ContextBuilder;
import org.zicat.tributary.sink.kafka.AbstractKafkaFunction;

import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Iterator;

/** AbstractKafkaFunctionTest. */
public class AbstractKafkaFunctionTest {

    final MockProducer<byte[], byte[]> producer = new MockProducer<>();
    private AbstractKafkaFunction function;

    @Before
    public void before() {
        function =
                new AbstractKafkaFunction() {
                    @Override
                    public void process(GroupOffset groupOffset, Iterator<byte[]> iterator) {
                        int count = 0;
                        while (iterator.hasNext()) {
                            sendKafka(
                                    "broker_test" + count % 10,
                                    new ProducerRecord<>("topic_test", iterator.next()));
                            count++;
                        }
                        sendKafka(
                                "broker_test" + count,
                                new ProducerRecord<>("topic_test", "test_value".getBytes()),
                                (metadata, exception) -> Assert.assertNull(exception));
                        flush(groupOffset);
                    }

                    @Override
                    protected Producer<byte[], byte[]> createProducer(String broker) {
                        return producerMap.computeIfAbsent(broker, key -> producer);
                    }
                };
        final ContextBuilder builder =
                ContextBuilder.newBuilder()
                        .partitionId(0)
                        .startGroupOffset(new GroupOffset(1, 1, "g1"));
        final Context config = builder.build();
        function.open(config);
    }

    @Test
    public void test() throws Exception {

        final String value = "test_data";
        final GroupOffset nextGroupOffset = function.committableOffset().skipNextSegmentHead();
        function.process(
                nextGroupOffset,
                Collections.singleton(value.getBytes(StandardCharsets.UTF_8)).iterator());
        Assert.assertEquals(value, new String(producer.history().get(0).value()));
        Assert.assertEquals(nextGroupOffset, function.committableOffset());
    }

    @Test
    public void after() {
        IOUtils.closeQuietly(function);
    }
}
