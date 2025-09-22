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

import static org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG;
import static org.zicat.tributary.common.records.RecordsUtils.defaultSinkExtraHeaders;
import static org.zicat.tributary.common.records.RecordsUtils.foreachRecord;
import static org.zicat.tributary.sink.kafka.KafkaFunctionFactory.OPTION_TOPIC;

import io.prometheus.client.Counter;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.zicat.tributary.channel.Offset;
import org.zicat.tributary.common.IOUtils;
import org.zicat.tributary.common.records.Records;
import org.zicat.tributary.sink.function.AbstractFunction;
import org.zicat.tributary.sink.function.Context;

import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.Map.Entry;

/** KafkaFunction. */
public class KafkaFunction extends AbstractFunction {

    public static final String HEAD_KEY_ORIGIN_TOPIC = "_origin_topic";
    public static final String TOPIC_TEMPLATE = "${topic}";

    private static final Counter SINK_KAFKA_COUNTER =
            Counter.build()
                    .name("tributary_sink_kafka_counter")
                    .help("tributary sink kafka counter")
                    .labelNames("host", "id")
                    .register();

    public static final String KAFKA_KEY_PREFIX = "kafka.";

    protected transient Producer<byte[], byte[]> producer;
    protected transient Counter.Child sinkCounter;
    protected transient TributaryKafkaCallback callback;
    protected transient String defaultTopic;
    protected transient Offset lastOffset;

    @Override
    public void open(Context context) throws Exception {
        super.open(context);
        sinkCounter = labelHostId(SINK_KAFKA_COUNTER);
        producer = createProducer(context);
        defaultTopic = context.get(OPTION_TOPIC);
        callback = new TributaryKafkaCallback();
    }

    @Override
    public void process(Offset offset, Iterator<Records> iterator) throws Exception {
        callback.checkState();
        int totalCount = 0;
        while (iterator.hasNext()) {
            totalCount += sendKafka(iterator.next());
        }
        lastOffset = offset;
        sinkCounter.inc(totalCount);
    }

    @Override
    public void snapshot() throws Exception {
        flushAndCommit(lastOffset);
    }

    /**
     * send kafka.
     *
     * @param records records
     * @return boolean send.
     */
    protected int sendKafka(Records records) throws Exception {
        final String originTopic = records.topic();
        final String targetTopic = targetTopic(originTopic);
        final Map<String, byte[]> extraHeaders = new HashMap<>(defaultSinkExtraHeaders());
        extraHeaders.put(HEAD_KEY_ORIGIN_TOPIC, originTopic.getBytes(StandardCharsets.UTF_8));
        foreachRecord(
                records,
                (key, value, allHeaders) -> sendRecord(targetTopic, key, value, allHeaders),
                extraHeaders);
        return records.count();
    }

    /**
     * get target topic by origin topic.
     *
     * @param originTopic originTopic
     * @return string target topic
     */
    protected String targetTopic(String originTopic) {
        return defaultTopic == null
                ? originTopic
                : defaultTopic.replace(TOPIC_TEMPLATE, originTopic);
    }

    /**
     * create kafka producer.
     *
     * @param context context
     * @return producer
     */
    protected Producer<byte[], byte[]> createProducer(Context context) {
        final Properties properties = context.filterPropertyByPrefix(KAFKA_KEY_PREFIX);
        properties.put(KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        properties.put(VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        return new KafkaProducer<>(properties);
    }

    /**
     * create producer record.
     *
     * @param topic topic
     * @param key key
     * @param value value
     * @param headers headers
     */
    private void sendRecord(String topic, byte[] key, byte[] value, Map<String, byte[]> headers) {
        final Collection<Header> kafkaHeaders = new ArrayList<>(headers.size());
        for (Entry<String, byte[]> entry : headers.entrySet()) {
            kafkaHeaders.add(new RecordHeader(entry.getKey(), entry.getValue()));
        }
        producer.send(createProducerRecord(topic, key, value, kafkaHeaders), callback);
    }

    /**
     * flush and commit offset.
     *
     * @param offset offset
     */
    protected void flushAndCommit(Offset offset) throws Exception {
        producer.flush();
        callback.checkState();
        commit(offset);
    }

    /**
     * create producer record.
     *
     * @param topic topic
     * @param key key
     * @param value value
     * @param headers headers
     * @return ProducerRecord
     */
    private static ProducerRecord<byte[], byte[]> createProducerRecord(
            String topic, byte[] key, byte[] value, Collection<Header> headers) {
        return new ProducerRecord<>(topic, null, null, key, value, headers);
    }

    @Override
    public void close() {
        IOUtils.closeQuietly(producer);
    }
}
