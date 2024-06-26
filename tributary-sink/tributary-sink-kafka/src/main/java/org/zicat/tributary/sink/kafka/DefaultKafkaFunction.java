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
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.zicat.tributary.channel.GroupOffset;
import org.zicat.tributary.common.records.Records;
import org.zicat.tributary.sink.function.Context;

import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.Map.Entry;

import static org.zicat.tributary.common.records.RecordsUtils.defaultSinkExtraHeaders;
import static org.zicat.tributary.common.records.RecordsUtils.foreachRecord;
import static org.zicat.tributary.sink.kafka.DefaultKafkaFunctionFactory.OPTION_TOPIC;

/** DefaultKafkaFunction. */
public class DefaultKafkaFunction extends AbstractKafkaFunction {

    public static final String HEAD_KEY_ORIGIN_TOPIC = "_origin_topic";
    public static final String TOPIC_TEMPLATE = "${topic}";

    protected transient DefaultCallback callback;
    protected transient String defaultTopic;

    @Override
    public void open(Context context) throws Exception {
        super.open(context);
        this.defaultTopic = context.get(OPTION_TOPIC);
        this.callback = new DefaultCallback();
    }

    @Override
    public void process(GroupOffset groupOffset, Iterator<Records> iterator) throws Exception {
        callback.checkState();
        int totalCount = 0;
        while (iterator.hasNext()) {
            final Records records = iterator.next();
            totalCount += sendKafka(records);
        }
        flush(groupOffset);
        incSinkKafkaCounter(totalCount);
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

        final List<ProducerRecord<byte[], byte[]>> producerRecords =
                new ArrayList<>(records.count());
        foreachRecord(
                records,
                (key, value, headers) ->
                        producerRecords.add(createProducerRecord(targetTopic, key, value, headers)),
                extraHeaders);
        sendKafka(producerRecords, callback);
        return producerRecords.size();
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
     * create producer record.
     *
     * @param targetTopic targetTopic
     * @param key key
     * @param value value
     * @param headers headers
     * @return ProducerRecord
     */
    private ProducerRecord<byte[], byte[]> createProducerRecord(
            String targetTopic, byte[] key, byte[] value, Map<String, byte[]> headers) {
        final Collection<Header> kafkaHeaders = new ArrayList<>(headers.size());
        for (Entry<String, byte[]> entry : headers.entrySet()) {
            kafkaHeaders.add(new RecordHeader(entry.getKey(), entry.getValue()));
        }
        return new ProducerRecord<>(targetTopic, null, null, key, value, kafkaHeaders);
    }

    /** DefaultCallback. */
    public static class DefaultCallback implements Callback {

        private volatile Exception lastException;

        @Override
        public void onCompletion(RecordMetadata metadata, Exception exception) {
            if (exception != null) {
                this.lastException = exception;
            }
        }

        /**
         * throw last exception.
         *
         * @throws Exception exception
         */
        public void checkState() throws Exception {
            final Exception tmp = lastException;
            if (tmp != null) {
                lastException = null;
                throw tmp;
            }
        }
    }
}
