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

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

/** JsonRecord. */
public class JsonRecord implements Record {
    private static final byte[] EMPTY = new byte[0];
    private final String key;
    private final String value;
    private final Map<String, String> headers;

    public JsonRecord() {
        this(null, null, null);
    }

    public JsonRecord(String key, String value, Map<String, String> headers) {
        this.key = key;
        this.value = value;
        this.headers = headers == null ? new HashMap<>() : headers;
    }

    @JsonProperty("key")
    public String getKey() {
        return key;
    }

    @JsonProperty("value")
    public String getValue() {
        return value;
    }

    @JsonProperty("headers")
    public Map<String, String> getHeaders() {
        return headers;
    }

    @Override
    @JsonIgnore
    public Map<String, byte[]> headers() {
        final Map<String, byte[]> result = new HashMap<>();
        headers.forEach((k, v) -> result.put(k, v.getBytes(StandardCharsets.UTF_8)));
        return result;
    }

    @Override
    @JsonIgnore
    public byte[] key() {
        return key == null ? EMPTY : key.getBytes(StandardCharsets.UTF_8);
    }

    @Override
    @JsonIgnore
    public byte[] value() {
        return value == null ? EMPTY : value.getBytes(StandardCharsets.UTF_8);
    }
}
