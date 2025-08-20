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

package org.zicat.tributary.source.logstash.http;

import static org.zicat.tributary.source.http.HttpMessageDecoder.MAPPER;

import com.fasterxml.jackson.core.type.TypeReference;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;

/** Codec. */
public enum Codec {
    PLAIN("plain") {

        final String key = "message";

        @Override
        public void encode(Map<String, Object> target, byte[] body) {
            target.put(key, new String(body, StandardCharsets.UTF_8));
        }
    },
    JSON("json") {

        final TypeReference<Map<String, Object>> mapStringType =
                new TypeReference<Map<String, Object>>() {};

        @Override
        public void encode(Map<String, Object> target, byte[] body) throws IOException {
            target.putAll(MAPPER.readValue(body, mapStringType));
        }
    };

    private final String name;

    Codec(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

    /**
     * encode body to target map.
     *
     * @param target target
     * @param body body
     */
    public abstract void encode(Map<String, Object> target, byte[] body) throws IOException;
}
