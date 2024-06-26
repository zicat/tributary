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
import java.util.*;

/** DefaultRecords. */
public class DefaultRecords implements Records {

    private final String topic;
    private final Map<String, byte[]> headers;
    private final Collection<Record> records;

    public DefaultRecords(String topic, Map<String, byte[]> headers, Collection<Record> records) {
        if (topic == null) {
            throw new IllegalArgumentException("topic is null");
        }
        this.topic = topic;
        this.headers = headers == null ? new HashMap<>() : headers;
        this.records = records == null ? new ArrayList<>() : records;
    }

    public DefaultRecords(String topic, Map<String, byte[]> headers) {
        this(topic, headers, new ArrayList<>());
    }

    @Override
    public String topic() {
        return topic;
    }

    @Override
    public Map<String, byte[]> headers() {
        return headers;
    }

    @Override
    public int count() {
        return records.size();
    }

    public void addRecord(Record record) {
        records.add(record);
    }

    @Override
    public Iterator<Record> iterator() {
        return records.iterator();
    }

    @Override
    public String toString() {
        final StringBuilder sb =
                new StringBuilder("{topic:")
                        .append(topic)
                        .append(", partition:")
                        .append(", headers:[");
        final Iterator<Map.Entry<String, byte[]>> it = headers.entrySet().iterator();
        while (it.hasNext()) {
            final Map.Entry<String, byte[]> entry = it.next();
            sb.append(entry.getKey())
                    .append(":")
                    .append(new String(entry.getValue(), StandardCharsets.UTF_8));
            if (it.hasNext()) {
                sb.append(",");
            }
        }
        sb.append("], records:[");
        final Iterator<Record> recordIt = records.iterator();
        while (recordIt.hasNext()) {
            sb.append(recordIt.next().toString());
            if (recordIt.hasNext()) {
                sb.append(",");
            }
        }
        return sb.append("]}").toString();
    }
}
