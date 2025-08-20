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

import java.util.*;

/** SingleRecords. */
public class SingleRecords implements Records {

    private final String topic;
    private final Map<String, byte[]> headers;
    private final Record record;

    public SingleRecords(String topic, Map<String, byte[]> headers, Record record) {
        if (topic == null) {
            throw new IllegalArgumentException("topic is null");
        }
        this.topic = topic;
        this.headers = headers == null ? new HashMap<>() : headers;
        this.record = record;
    }

    public SingleRecords(String topic, Record record) {
        this(topic, null, record);
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
        return 1;
    }

    @SuppressWarnings("NullableProblems")
    @Override
    public Iterator<Record> iterator() {
        return new Iterator<Record>() {
            Record offset = record;

            @Override
            public boolean hasNext() {
                return offset != null;
            }

            @Override
            public Record next() {
                Record result = offset;
                offset = null;
                return result;
            }
        };
    }
}
