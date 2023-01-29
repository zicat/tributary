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

import io.prometheus.client.Counter;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.zicat.tributary.channel.RecordsOffset;
import org.zicat.tributary.sink.function.Context;

import java.util.Iterator;

/** DefaultKafkaFunction. */
public class DefaultKafkaFunction extends AbstractKafkaFunction {

    private static final Counter SINK_KAFKA_COUNTER =
            Counter.build()
                    .name("sink_kafka_counter")
                    .help("sink kafka counter")
                    .labelNames("host", "groupId", "topic")
                    .register();
    public static final String KEY_TOPIC = "topic";
    protected String customTopic;

    @Override
    public void open(Context context) {
        super.open(context);
        this.customTopic =
                context.getCustomProperty(getKafkaKeyPrefix(null) + KEY_TOPIC).toString();
    }

    @Override
    public void process(RecordsOffset recordsOffset, Iterator<byte[]> iterator) {

        int totalCount = 0;
        while (iterator.hasNext()) {
            final byte[] value = iterator.next();
            if (sendKafka(value)) {
                totalCount++;
            }
        }
        flush(recordsOffset);
        SINK_KAFKA_COUNTER
                .labels(metricsHost(), context.groupId(), context.topic())
                .inc(totalCount);
    }

    /**
     * send kafka.
     *
     * @param value value
     * @return boolean send.
     */
    protected boolean sendKafka(byte[] value) {
        final ProducerRecord<byte[], byte[]> record =
                new ProducerRecord<>(customTopic, null, value);
        sendKafka(null, record);
        return true;
    }
}
