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

import org.apache.kafka.clients.producer.*;
import org.zicat.tributary.channel.GroupOffset;
import org.zicat.tributary.common.ConfigOption;
import org.zicat.tributary.common.ConfigOptions;
import org.zicat.tributary.common.IOUtils;
import org.zicat.tributary.sink.function.Context;

import java.io.IOException;
import java.util.Iterator;

/** DefaultKafkaFunction. */
public class DefaultKafkaFunction extends AbstractKafkaFunction {

    protected static final String DEFAULT_CLUSTER = null;
    private static final ConfigOption<String> OPTION_TOPIC =
            ConfigOptions.key("topic")
                    .stringType()
                    .description("the kafka topic to send data")
                    .defaultValue("sink_kafka_channel");
    protected String customTopic;
    protected KafkaProducer<byte[], byte[]> producer;
    protected DefaultCallback callback;

    @Override
    public void open(Context context) throws Exception {
        super.open(context);
        final String keyPrefix = getKafkaKeyPrefix(DEFAULT_CLUSTER) + "topic";
        this.customTopic = context.get(OPTION_TOPIC.changeKey(keyPrefix));
        this.callback = new DefaultCallback();
    }

    @Override
    public void process(GroupOffset groupOffset, Iterator<byte[]> iterator) throws Exception {

        callback.checkState();

        int totalCount = 0;
        while (iterator.hasNext()) {
            final byte[] value = iterator.next();
            if (sendKafka(value)) {
                totalCount++;
            }
        }
        flush(groupOffset);
        incSinkKafkaCounter(totalCount);
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
        sendKafka(record);
        return true;
    }

    /**
     * send producer record to kafka.
     *
     * @param producerRecord producerRecord
     */
    protected void sendKafka(ProducerRecord<byte[], byte[]> producerRecord) {
        sendKafka(DEFAULT_CLUSTER, producerRecord, callback);
    }

    @Override
    protected Producer<byte[], byte[]> getOrCreateProducer(String clusterId) {
        if (producer == null) {
            producer = new KafkaProducer<>(getKafkaProperties(DEFAULT_CLUSTER));
        }
        return producer;
    }

    @Override
    public void close() throws IOException {
        try {
            super.close();
        } finally {
            IOUtils.closeQuietly(producer);
        }
    }

    /** DefaultCallback. */
    public static class DefaultCallback implements Callback {

        private Exception lastException;

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
