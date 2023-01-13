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

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.zicat.tributary.channel.RecordsOffset;
import org.zicat.tributary.sink.function.AbstractFunction;
import org.zicat.tributary.sink.function.Context;
import org.zicat.tributary.sink.utils.Exceptions;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/** AbstractKafkaFunction. */
public abstract class AbstractKafkaFunction extends AbstractFunction {

    public static final String KAFKA_KEY_PREFIX = "kafka.";
    public static final String KAFKA_KEY_BOOTSTRAP_SERVERS = "bootstrap.servers";
    protected Properties kafkaProperties;

    protected Map<String, Producer<byte[], byte[]>> producerMap = new HashMap<>();

    @Override
    public void open(Context context) {
        super.open(context);
        this.kafkaProperties = context.filterPropertyByPrefix(KAFKA_KEY_PREFIX);
    }

    /**
     * set kafka data.
     *
     * @param broker broker
     * @param producerRecord producerRecord
     */
    protected void sendKafka(
            String broker, ProducerRecord<byte[], byte[]> producerRecord, Callback callback) {

        if (producerRecord == null) {
            return;
        }
        createProducer(broker).send(producerRecord, callback);
    }

    /**
     * set kafka data.
     *
     * @param broker broker
     * @param producerRecord producerRecord
     */
    protected void sendKafka(String broker, ProducerRecord<byte[], byte[]> producerRecord) {
        sendKafka(broker, producerRecord, null);
    }

    /**
     * create producer.
     *
     * @param broker broker.
     * @return Producer
     */
    protected Producer<byte[], byte[]> createProducer(String broker) {
        return producerMap.computeIfAbsent(
                broker, key -> new KafkaProducer<>(createKafkaProperties(broker, kafkaProperties)));
    }

    /**
     * create kafka properties by broker.
     *
     * @param broker broker
     * @param properties properties
     * @return Properties
     */
    protected Properties createKafkaProperties(String broker, Properties properties) {
        return KafkaUtils.createKafkaProperties(broker, properties);
    }

    @Override
    public void close() throws IOException {
        Exception lastE = null;
        for (Map.Entry<String, Producer<byte[], byte[]>> entry : producerMap.entrySet()) {
            try {
                entry.getValue().close();
            } catch (Exception e) {
                lastE = e;
            }
        }
        producerMap.clear();
        if (lastE != null) {
            throw Exceptions.castAsIOException(lastE);
        }
    }

    /**
     * try flush file offset.
     *
     * @param recordsOffset recordsOffset
     */
    public final void flush(RecordsOffset recordsOffset) {
        flush(
                recordsOffset,
                () -> {
                    producerMap.forEach((k, v) -> v.flush());
                    return true;
                });
    }
}
