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
import org.apache.kafka.clients.producer.RecordMetadata;
import org.zicat.tributary.channel.GroupOffset;
import org.zicat.tributary.common.ConfigOption;
import org.zicat.tributary.common.ConfigOptions;
import org.zicat.tributary.common.ReadableConfig;
import org.zicat.tributary.common.SpiFactory;
import org.zicat.tributary.sink.function.Context;

import java.util.Iterator;

/** DefaultKafkaFunction. */
public class DefaultKafkaFunction extends AbstractKafkaFunction {

    public static final ConfigOption<String> OPTION_BYTE_2_RECORD_IDENTITY =
            ConfigOptions.key("decoder.identity")
                    .stringType()
                    .description("the byte2Record identity")
                    .defaultValue("default");
    protected transient DefaultCallback callback;
    protected transient Byte2Record byte2Record;

    @Override
    public void open(Context context) throws Exception {
        super.open(context);
        this.byte2Record = createByte2Record(context);
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
        sendKafka(byte2Record.convert(value), callback);
        return true;
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

    /**
     * create byte 2 record.
     *
     * @param config config
     * @return byte2Record
     */
    private static Byte2Record createByte2Record(ReadableConfig config) {
        return SpiFactory.findFactory(
                        config.get(OPTION_BYTE_2_RECORD_IDENTITY), Byte2RecordFactory.class)
                .create(config);
    }
}
