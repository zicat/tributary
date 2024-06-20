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
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.zicat.tributary.channel.GroupOffset;
import org.zicat.tributary.common.IOUtils;
import org.zicat.tributary.sink.function.AbstractFunction;
import org.zicat.tributary.sink.function.Context;

import java.util.List;
import java.util.Properties;

import static org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG;

/** AbstractKafkaFunction. */
public abstract class AbstractKafkaFunction extends AbstractFunction {

    private static final Counter SINK_KAFKA_COUNTER =
            Counter.build()
                    .name("sink_kafka_counter")
                    .help("sink kafka counter")
                    .labelNames("host", "groupId", "topic")
                    .register();

    public static final String KAFKA_KEY_PREFIX = "kafka.";

    protected volatile Producer<byte[], byte[]> producer;
    protected Counter.Child sinkCounterChild;

    @Override
    public void open(Context context) throws Exception {
        super.open(context);
        sinkCounterChild =
                SINK_KAFKA_COUNTER.labels(metricsHost(), context.groupId(), context.topic());
    }

    /**
     * get kafka properties.
     *
     * @return properties
     */
    protected Properties getKafkaProperties() {
        final Properties properties = context.filterPropertyByPrefix(KAFKA_KEY_PREFIX);
        properties.put(KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        properties.put(VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        return properties;
    }

    /**
     * set kafka data.
     *
     * @param producerRecords producerRecords
     * @param callback callback
     */
    protected void sendKafka(
            List<ProducerRecord<byte[], byte[]>> producerRecords, Callback callback) {
        if (producerRecords == null || producerRecords.isEmpty()) {
            return;
        }
        final Producer<byte[], byte[]> producer = getOrCreateProducer();
        for (ProducerRecord<byte[], byte[]> producerRecord : producerRecords) {
            producer.send(producerRecord, callback);
        }
    }

    /**
     * create producer.
     *
     * @return Producer
     */
    protected Producer<byte[], byte[]> getOrCreateProducer() {
        if (producer == null) {
            producer = new KafkaProducer<>(getKafkaProperties());
        }
        return producer;
    }

    @Override
    public void close() {
        IOUtils.closeQuietly(producer);
    }

    /**
     * try flush file offset.
     *
     * @param groupOffset groupOffset
     */
    public final void flush(GroupOffset groupOffset) {
        commit(
                groupOffset,
                () -> {
                    getOrCreateProducer().flush();
                    return true;
                });
    }

    /**
     * inc sink kafka counter.
     *
     * @param sum sum
     */
    protected void incSinkKafkaCounter(int sum) {
        sinkCounterChild.inc(sum);
    }
}
