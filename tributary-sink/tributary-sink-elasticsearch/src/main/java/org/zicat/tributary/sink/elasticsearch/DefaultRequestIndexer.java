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

import com.fasterxml.jackson.core.JsonProcessingException;
import static java.nio.charset.StandardCharsets.UTF_8;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zicat.tributary.common.config.ConfigOption;
import org.zicat.tributary.common.config.ConfigOptions;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.xcontent.XContentType;
import org.zicat.tributary.common.config.MemorySize;
import org.zicat.tributary.common.metric.MetricKey;
import static org.zicat.tributary.sink.elasticsearch.ElasticsearchFunctionFactory.OPTION_BULK_MAX_BYTES;
import org.zicat.tributary.sink.config.Context;

import java.util.Collections;
import java.util.Map;
import java.util.Map.Entry;

/** DefaultRequestIndexer. */
public class DefaultRequestIndexer implements RequestIndexer {

    public static final String IDENTITY = "default";
    public static final ConfigOption<String> OPTION_REQUEST_INDEXER_DEFAULT_INDEX =
            ConfigOptions.key("request.indexer.default.index")
                    .stringType()
                    .description("Elasticsearch index for every record.")
                    .defaultValue(null);

    public static final ConfigOption<MemorySize> OPTION_REQUEST_INDEX_DEFAULT_RECORD_SIZE_LIMIT =
            ConfigOptions.key("request.indexer.default.record-size-limit")
                    .memoryType()
                    .description("The max size of a record, default bulk.max.bytes")
                    .defaultValue(null);

    public static final MetricKey DISCARD_COUNTER =
            new MetricKey("tributary_sink_elasticsearch_discard_counter");

    private static final Logger LOG = LoggerFactory.getLogger(DefaultRequestIndexer.class);
    private static final String ERROR_VALUE_FORMAT = "skip invalid message, index:{}, value: {}";
    private static final ObjectMapper MAPPER = new ObjectMapper();
    public static final String KEY_TOPIC = "_topic";

    private transient String index;
    private transient long discardCounter;
    private transient long recordSizeLimit;

    @Override
    public void open(Context context) {
        index = context.get(OPTION_REQUEST_INDEXER_DEFAULT_INDEX);
        recordSizeLimit =
                context.get(OPTION_REQUEST_INDEX_DEFAULT_RECORD_SIZE_LIMIT, OPTION_BULK_MAX_BYTES)
                        .getBytes();
    }

    @Override
    public boolean add(
            BulkRequest bulkRequest,
            String topic,
            byte[] key,
            byte[] value,
            Map<String, byte[]> headers)
            throws Exception {
        final IndexRequest indexRequest = indexRequest(topic, key, value, headers);
        if (indexRequest == null) {
            return false;
        }
        bulkRequest.add(indexRequest);
        return true;
    }

    /**
     * create index request.
     *
     * @param topic topic
     * @param key key
     * @param value value
     * @param headers headers
     * @return IndexRequest
     */
    protected IndexRequest indexRequest(
            String topic, byte[] key, byte[] value, Map<String, byte[]> headers)
            throws JsonProcessingException {
        final String realIndex = index == null ? topic : index;
        if (realIndex == null) {
            discardCounter++;
            LOG.warn("skip invalid message, index is null");
            return null;
        }
        final int valueLength = value == null ? 0 : value.length;
        if (valueLength == 0 || valueLength >= recordSizeLimit) {
            discardCounter++;
            LOG.warn("skip invalid message, index:{}, length: {}", realIndex, valueLength);
            return null;
        }
        final IndexRequest indexRequest = new IndexRequest(realIndex);
        indexRequest.id(id(topic, key, value, headers));
        final JsonNode jsonNode;
        try {
            jsonNode = MAPPER.readTree(value);
            if (!(jsonNode instanceof ObjectNode)) {
                LOG.warn(ERROR_VALUE_FORMAT, realIndex, new String(value, UTF_8));
                discardCounter++;
                return null;
            }
        } catch (Exception e) {
            discardCounter++;
            LOG.warn(ERROR_VALUE_FORMAT, realIndex, new String(value, UTF_8), e);
            return null;
        }
        final ObjectNode objectNode = (ObjectNode) jsonNode;
        for (Entry<String, byte[]> entry : headers.entrySet()) {
            if (objectNode.has(entry.getKey())) {
                continue;
            }
            final String v = new String(entry.getValue(), UTF_8);
            objectNode.put(entry.getKey(), v);
        }
        if (index != null) {
            objectNode.put(KEY_TOPIC, topic);
        }
        indexRequest.source(MAPPER.writeValueAsBytes(objectNode), XContentType.JSON);
        return indexRequest;
    }

    @Override
    public Map<MetricKey, Double> counterFamily() {
        return Collections.singletonMap(DISCARD_COUNTER, (double) discardCounter);
    }

    /**
     * parse id.
     *
     * @param topic topic
     * @param key key
     * @param value value
     * @param headers headers
     * @return id
     */
    protected String id(String topic, byte[] key, byte[] value, Map<String, byte[]> headers) {
        if (key == null || key.length == 0) {
            return null;
        }
        return new String(key, UTF_8);
    }

    @Override
    public String identity() {
        return IDENTITY;
    }

    @Override
    public void close() {}
}
