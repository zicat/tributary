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

import org.zicat.tributary.common.Clock;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/** RecordsUtils. */
public class RecordsUtils {

    public static final String HEAD_KEY_SENT_TS = "_sent_ts";
    public static final String HEADER_KEY_REC_TS = "_rec_ts";

    public static final Set<String> CONCAT_HEADER_KEYS =
            new HashSet<>(Arrays.asList(HEADER_KEY_REC_TS, HEAD_KEY_SENT_TS));
    public static final byte[] CONCAT_SPLIT = " ".getBytes(StandardCharsets.UTF_8);

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
        for (Record0 record : records) {
            final Map<String, byte[]> headers = new HashMap<>(record.headers());
            appendHead(extraHeaders, headers);
            appendHead(records.headers(), headers);
            consumer.accept(record.key(), record.value(), headers);
        }
    }

    /**
     * append head.
     *
     * @param source source
     * @param target target
     */
    private static void appendHead(Map<String, byte[]> source, Map<String, byte[]> target) {
        if (source == null) {
            return;
        }
        for (Map.Entry<String, byte[]> entry : source.entrySet()) {
            final String key = entry.getKey();
            final byte[] value = entry.getValue();
            if (CONCAT_HEADER_KEYS.contains(key)) {
                target.compute(key, (k, vInCache) -> concatBytesArray(vInCache, value));
            } else {
                target.put(key, value);
            }
        }
    }

    /**
     * concat two bytes array.
     *
     * @param bytes1 bytes1
     * @param bytes2 bytes1
     * @return new bytes
     */
    private static byte[] concatBytesArray(byte[] bytes1, byte[] bytes2) {
        if (bytes1 == null) {
            return bytes2;
        }
        if (bytes2 == null) {
            return bytes1;
        }
        final byte[] concatTs = new byte[bytes1.length + CONCAT_SPLIT.length + bytes2.length];
        System.arraycopy(bytes1, 0, concatTs, 0, bytes1.length);
        System.arraycopy(CONCAT_SPLIT, 0, concatTs, bytes1.length, CONCAT_SPLIT.length);
        System.arraycopy(bytes2, 0, concatTs, bytes1.length + CONCAT_SPLIT.length, bytes2.length);
        return concatTs;
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

    /** default sink extra headers. */
    public static void sinkExtraHeaders(Clock clock, Map<String, byte[]> map) {
        final byte[] sentTs = String.valueOf(clock.currentTimeMillis()).getBytes();
        map.compute(HEAD_KEY_SENT_TS, (k, v) -> concatBytesArray(v, sentTs));
    }

    /**
     * append record timestamp to headers. like: 1760162349000,1760162349001
     *
     * @param clock clock
     * @param map map
     */
    public static void appendRecTs(Clock clock, Map<String, byte[]> map) {
        final byte[] recTs = String.valueOf(clock.currentTimeMillis()).getBytes();
        map.compute(HEADER_KEY_REC_TS, (k, v) -> concatBytesArray(v, recTs));
    }
}
