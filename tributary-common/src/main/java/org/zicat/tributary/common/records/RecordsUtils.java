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

package org.zicat.tributary.common.records;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

/** RecordsUtils. */
public class RecordsUtils {

    public static final String HEAD_KEY_SENT_TS = "_sent_ts";

    /**
     * create string records.
     *
     * @param topic topic
     * @param value value
     * @return Records
     */
    public static Records createStringRecords(String topic, String... value) {
        return createStringRecords(topic, Arrays.asList(value));
    }

    /**
     * create string records.
     *
     * @param topic topic
     * @param values values
     * @return Records
     */
    public static Records createStringRecords(String topic, Collection<String> values) {
        return createStringRecords(topic, null, values);
    }

    /**
     * create bytes records.
     *
     * @param topic topic
     * @param values values
     * @return Records
     */
    public static Records createBytesRecords(
            String topic, Map<String, byte[]> headers, Collection<byte[]> values) {
        return new DefaultRecords(
                topic,
                headers,
                values.stream()
                        .map(v -> new DefaultRecord(null, null, v))
                        .collect(Collectors.toList()));
    }

    /**
     * create bytes records.
     *
     * @param topic topic
     * @param values values
     * @return Records
     */
    public static Records createBytesRecords(String topic, Collection<byte[]> values) {
        return createBytesRecords(topic, null, values);
    }

    /**
     * create bytes records.
     *
     * @param topic topic
     * @param values values
     * @return Records
     */
    public static Records createBytesRecords(String topic, byte[]... values) {
        return createBytesRecords(topic, null, Arrays.asList(values));
    }

    /**
     * create string records.
     *
     * @param topic topic
     * @param values values
     * @return Records
     */
    public static Records createStringRecords(
            String topic, Map<String, byte[]> headers, Collection<String> values) {
        return createBytesRecords(
                topic,
                headers,
                values.stream()
                        .map(v -> v.getBytes(StandardCharsets.UTF_8))
                        .collect(Collectors.toList()));
    }

    /**
     * foreach record.
     *
     * @param records records
     * @param consumer consumer
     * @param extraHeaders extraHeaders
     */
    public static void foreachRecord(
            Records records, RecordConsumer consumer, Map<String, byte[]> extraHeaders)
            throws Exception {
        for (Record record : records) {
            final Map<String, byte[]> headers = new HashMap<>(record.headers());
            headers.putAll(records.headers());
            if (extraHeaders != null) {
                headers.putAll(extraHeaders);
            }
            consumer.accept(record.key(), record.value(), headers);
        }
    }

    /**
     * foreach record.
     *
     * @param records records
     * @param consumer consumer
     */
    public static void foreachRecord(Records records, RecordConsumer consumer) throws Exception {
        foreachRecord(records, consumer, null);
    }

    /** RecordConsumer. */
    public interface RecordConsumer {

        /**
         * consume record.
         *
         * @param key key
         * @param value value
         * @param allHeaders allHeaders
         */
        void accept(byte[] key, byte[] value, Map<String, byte[]> allHeaders) throws Exception;
    }

    /**
     * default sink extra headers.
     *
     * @return headers
     */
    public static Map<String, byte[]> defaultSinkExtraHeaders() {
        final Map<String, byte[]> headers = new HashMap<>();
        final long sentTs = System.currentTimeMillis();
        headers.put(HEAD_KEY_SENT_TS, String.valueOf(sentTs).getBytes(StandardCharsets.UTF_8));
        return headers;
    }
}
