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

package org.zicat.tributary.sink.elasticsearch;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.xcontent.XContentType;
import org.zicat.tributary.common.ReadableConfig;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Map.Entry;

import static org.zicat.tributary.sink.elasticsearch.ElasticsearchFunctionFactory.OPTION_INDEX;

/** DefaultRequestIndexer. */
public class DefaultRequestIndexer implements RequestIndexer {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private transient String index;

    @Override
    public void open(ReadableConfig config) {
        index = config.get(OPTION_INDEX);
    }

    @Override
    public boolean add(
            BulkRequest bulkRequest,
            String topic,
            byte[] key,
            byte[] value,
            Map<String, byte[]> headers)
            throws Exception {
        final IndexRequest indexRequest = new IndexRequest(index);
        if (key != null) {
            indexRequest.id(new String(key, StandardCharsets.UTF_8));
        }
        final JsonNode jsonNode = MAPPER.readTree(new String(value, StandardCharsets.UTF_8));
        if (!(jsonNode instanceof ObjectNode)) {
            return false;
        }
        final ObjectNode objectNode = (ObjectNode) jsonNode;
        objectNode.put("_topic", topic);
        for (Entry<String, byte[]> entry : headers.entrySet()) {
            final String v = new String(entry.getValue(), StandardCharsets.UTF_8);
            objectNode.put("_h_" + entry.getKey(), v);
        }
        indexRequest.source(objectNode.toString(), XContentType.JSON);
        bulkRequest.add(indexRequest);
        return true;
    }

    @Override
    public String identity() {
        return "default";
    }
}
