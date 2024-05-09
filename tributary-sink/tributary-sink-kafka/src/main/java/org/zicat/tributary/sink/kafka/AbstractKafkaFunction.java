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
import org.zicat.tributary.channel.GroupOffset;
import org.zicat.tributary.sink.function.AbstractFunction;
import org.zicat.tributary.sink.function.Context;
import org.zicat.tributary.sink.utils.Exceptions;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/** AbstractKafkaFunction. */
public abstract class AbstractKafkaFunction extends AbstractFunction {

    private static final Counter SINK_KAFKA_COUNTER =
            Counter.build()
                    .name("sink_kafka_counter")
                    .help("sink kafka counter")
                    .labelNames("host", "groupId", "topic")
                    .register();

    public static final String KAFKA_KEY_PREFIX = "kafka.";

    protected final Map<String, Producer<byte[], byte[]>> producerMap = new HashMap<>();
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
    protected Properties getKafkaProperties(String clusterId) {
        final String kafkaPrefix = getKafkaKeyPrefix(clusterId);
        return context.filterPropertyByPrefix(kafkaPrefix);
    }

    /**
     * get kafka key prefix by cluster id.
     *
     * @param clusterId clusterId
     * @return kafka key prefix
     */
    protected String getKafkaKeyPrefix(String clusterId) {
        return clusterId == null || clusterId.trim().isEmpty()
                ? KAFKA_KEY_PREFIX
                : KAFKA_KEY_PREFIX + clusterId + ".";
    }

    /**
     * set kafka data.
     *
     * @param clusterId clusterId
     * @param producerRecord producerRecord
     */
    protected void sendKafka(
            String clusterId, ProducerRecord<byte[], byte[]> producerRecord, Callback callback) {
        if (producerRecord == null) {
            return;
        }
        getOrCreateProducer(clusterId).send(producerRecord, callback);
    }

    /**
     * send producer record to kafka.
     *
     * @param clusterId clusterId
     * @param producerRecord producerRecord
     */
    protected void sendKafka(String clusterId, ProducerRecord<byte[], byte[]> producerRecord) {
        sendKafka(clusterId, producerRecord, null);
    }

    /**
     * create producer.
     *
     * @param clusterId clusterId.
     * @return Producer
     */
    protected Producer<byte[], byte[]> getOrCreateProducer(String clusterId) {
        return producerMap.computeIfAbsent(
                clusterId, key -> new KafkaProducer<>(getKafkaProperties(clusterId)));
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
     * @param groupOffset groupOffset
     */
    public final void flush(GroupOffset groupOffset) {
        commit(
                groupOffset,
                () -> {
                    producerMap.forEach((k, v) -> v.flush());
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
