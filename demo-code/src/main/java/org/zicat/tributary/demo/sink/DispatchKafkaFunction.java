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

package org.zicat.tributary.demo.sink;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.zicat.tributary.sink.kafka.DefaultKafkaFunction;

import java.nio.charset.StandardCharsets;

/** DispatchKafkaFunction. */
public class DispatchKafkaFunction extends DefaultKafkaFunction {

    @Override
    protected boolean sendKafka(byte[] value) {
        final String stringValue = new String(value, StandardCharsets.UTF_8);
        final int index = stringValue.indexOf(",");
        final String topic = stringValue.substring(0, index);
        final String realValue = stringValue.substring(index + 1);
        final ProducerRecord<byte[], byte[]> record =
                new ProducerRecord<>(topic, null, realValue.getBytes(StandardCharsets.UTF_8));
        sendKafka(null, record);
        return true;
    }
}
