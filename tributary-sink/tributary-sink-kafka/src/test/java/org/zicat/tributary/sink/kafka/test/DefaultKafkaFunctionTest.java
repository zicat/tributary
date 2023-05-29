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
import org.junit.Assert;
import org.junit.Test;
import org.zicat.tributary.channel.GroupOffset;
import org.zicat.tributary.sink.function.ContextBuilder;
import org.zicat.tributary.sink.kafka.DefaultKafkaFunction;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/** DefaultKafkaFunctionTest. */
public class DefaultKafkaFunctionTest {

    final MockProducer<byte[], byte[]> mockProducer = new MockProducer<>();

    @Test
    public void test() {
        DefaultKafkaFunction kafkaFunction = new MockDefaultKafkaFunction();
        final ContextBuilder builder =
                new ContextBuilder()
                        .id("id2")
                        .partitionId(0)
                        .startGroupOffset(new GroupOffset(0, 0, "g2"))
                        .topic("t2");

        builder.addCustomProperty("kafka.topic", "kt1");

        kafkaFunction.open(builder.build());

        final List<String> testValues = Arrays.asList("cc", "dd");
        GroupOffset groupOffset = new GroupOffset(2, 0, "g2");
        kafkaFunction.process(
                groupOffset,
                testValues.stream()
                        .map(String::getBytes)
                        .collect(Collectors.toList())
                        .listIterator());

        Assert.assertEquals(testValues.get(0), new String(mockProducer.history().get(0).value()));
        Assert.assertEquals(testValues.get(1), new String(mockProducer.history().get(1).value()));
        Assert.assertEquals(groupOffset, kafkaFunction.committableOffset());

        kafkaFunction.process(
                groupOffset.skipNextSegmentHead(),
                testValues.stream()
                        .map(String::getBytes)
                        .collect(Collectors.toList())
                        .listIterator());
        Assert.assertEquals(testValues.get(0), new String(mockProducer.history().get(2).value()));
        Assert.assertEquals(testValues.get(1), new String(mockProducer.history().get(3).value()));
        Assert.assertEquals(groupOffset.skipNextSegmentHead(), kafkaFunction.committableOffset());
    }

    private class MockDefaultKafkaFunction extends DefaultKafkaFunction {

        @Override
        protected Producer<byte[], byte[]> getOrCreateProducer(String broker) {
            return mockProducer;
        }
    }
}
