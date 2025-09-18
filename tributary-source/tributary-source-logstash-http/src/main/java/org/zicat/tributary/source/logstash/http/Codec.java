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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import static org.zicat.tributary.source.http.HttpMessageDecoder.MAPPER;

import com.fasterxml.jackson.core.type.TypeReference;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** Codec. */
public enum Codec {
    PLAIN("plain") {

        final String key = "message";

        @Override
        public List<Map<String, Object>> encode(byte[] body) {
            final Map<String, Object> value = new HashMap<>();
            value.put(key, new String(body, StandardCharsets.UTF_8));
            return Collections.singletonList(value);
        }
    },
    JSON("json") {

        final TypeReference<List<Map<String, Object>>> listMapStringType =
                new TypeReference<List<Map<String, Object>>>() {};
        final TypeReference<Map<String, Object>> mapStringType =
                new TypeReference<Map<String, Object>>() {};

        @Override
        public List<Map<String, Object>> encode(byte[] body) throws IOException {
            JsonNode root = MAPPER.readTree(body);
            if (root instanceof ArrayNode) {
                ArrayNode arrayNode = (ArrayNode) root;
                return MAPPER.convertValue(arrayNode, listMapStringType);
            } else if (root instanceof ObjectNode) {
                ObjectNode objectNode = (ObjectNode) root;
                return Collections.singletonList(MAPPER.convertValue(objectNode, mapStringType));
            } else {
                throw new RuntimeException("Unsupported json type: " + root.getNodeType());
            }
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
     * @param body body
     * @return encoded map list
     */
    public abstract List<Map<String, Object>> encode(byte[] body) throws IOException;
}
