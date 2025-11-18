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
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/** DefaultRecord. */
public class DefaultRecord implements Record0 {

    private static final byte[] EMPTY = new byte[0];

    private final Map<String, byte[]> headers;
    private final byte[] key;
    private final byte[] value;

    public DefaultRecord(Map<String, byte[]> headers, byte[] key, byte[] value) {
        this.headers = headers == null ? new HashMap<>() : headers;
        this.key = key == null ? EMPTY : key;
        this.value = value == null ? EMPTY : value;
    }

    public DefaultRecord(Map<String, byte[]> headers, byte[] value) {
        this(headers, null, value);
    }

    public DefaultRecord(byte[] value) {
        this(null, null, value);
    }

    @Override
    public Map<String, byte[]> headers() {
        return headers;
    }

    @Override
    public byte[] key() {
        return key;
    }

    @Override
    public byte[] value() {
        return value;
    }

    @Override
    public String toString() {
        final StringBuilder sb =
                new StringBuilder("{Key:")
                        .append(new String(key, StandardCharsets.UTF_8))
                        .append(", Value:")
                        .append(new String(value, StandardCharsets.UTF_8))
                        .append(", Headers:[");
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
        return sb.append("]").toString();
    }
}
