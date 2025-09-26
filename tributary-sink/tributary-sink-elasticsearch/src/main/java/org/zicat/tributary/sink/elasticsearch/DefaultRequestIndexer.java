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
import org.zicat.tributary.common.ConfigOption;
import org.zicat.tributary.common.ConfigOptions;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import io.prometheus.client.Counter;

import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.xcontent.XContentType;
import org.zicat.tributary.common.MemorySize;
import static org.zicat.tributary.sink.elasticsearch.ElasticsearchFunctionFactory.OPTION_BULK_MAX_BYTES;
import org.zicat.tributary.sink.function.Context;
import static org.zicat.tributary.sink.handler.PartitionHandler.OPTION_METRICS_HOST;

import java.util.Map;
import java.util.Map.Entry;

/** DefaultRequestIndexer. */
public class DefaultRequestIndexer implements RequestIndexer {

    public static final String IDENTITY = "default";
    public static final ConfigOption<Boolean> OPTION_REQUEST_INDEXER_DEFAULT_USING_TOPIC_AS_INDEX =
            ConfigOptions.key("request.indexer.default.topic_as_index")
                    .booleanType()
                    .description("Using topic as index.")
                    .defaultValue(false);

    public static final ConfigOption<String> OPTION_REQUEST_INDEXER_DEFAULT_INDEX =
            ConfigOptions.key("request.indexer.default.index")
                    .stringType()
                    .description("Elasticsearch index for every record.")
                    .defaultValue(null);

    public static final ConfigOption<MemorySize> OPTION_REQUEST_INDEX_DEFAULT_RECORD_SIZE_LIMIT =
            ConfigOptions.key("request.indexer.default.record_size_limit")
                    .memoryType()
                    .description("The max size of a record, default bulk.max.bytes")
                    .defaultValue(null);

    public static final Counter SINK_ELASTICSEARCH_DISCARD_COUNTER =
            Counter.build()
                    .name("tributary_sink_elasticsearch_discard_counter")
                    .help("tributary sink elasticsearch discard counter")
                    .labelNames("host", "id")
                    .register();

    private static final Logger LOG = LoggerFactory.getLogger(DefaultRequestIndexer.class);

    private static final ObjectMapper MAPPER = new ObjectMapper();
    public static final String KEY_TOPIC = "_topic";

    private transient String index;
    private transient boolean topicAsIndex;
    private transient Counter.Child sinkDiscardCounter;
    private transient long recordSizeLimit;

    @Override
    public void open(Context context) {
        topicAsIndex = context.get(OPTION_REQUEST_INDEXER_DEFAULT_USING_TOPIC_AS_INDEX);
        index = context.get(OPTION_REQUEST_INDEXER_DEFAULT_INDEX);
        recordSizeLimit =
                context.get(
                                OPTION_REQUEST_INDEX_DEFAULT_RECORD_SIZE_LIMIT,
                                context.get(OPTION_BULK_MAX_BYTES))
                        .getBytes();
        sinkDiscardCounter =
                SINK_ELASTICSEARCH_DISCARD_COUNTER.labels(
                        context.get(OPTION_METRICS_HOST), context.id());
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
        final String realIndex = topicAsIndex ? topic : index;
        if (realIndex == null) {
            sinkDiscardCounter.inc();
            LOG.warn("skip invalid message, index is null");
            return null;
        }
        final int valueLength = value == null ? 0 : value.length;
        if (valueLength == 0 || valueLength >= recordSizeLimit) {
            sinkDiscardCounter.inc();
            LOG.warn("skip invalid message, index:{}, length: {}", realIndex, valueLength);
            return null;
        }
        final IndexRequest indexRequest = new IndexRequest(realIndex);
        indexRequest.id(id(topic, key, value, headers));
        final JsonNode jsonNode;
        try {
            jsonNode = MAPPER.readTree(value);
            if (!(jsonNode instanceof ObjectNode)) {
                LOG.warn("skip invalid message, index:{}, value: {}", realIndex, new String(value, UTF_8));
                sinkDiscardCounter.inc();
                return null;
            }
        } catch (Exception ignore) {
            sinkDiscardCounter.inc();
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
        if (!topicAsIndex) {
            objectNode.put(KEY_TOPIC, topic);
        }
        indexRequest.source(MAPPER.writeValueAsBytes(objectNode), XContentType.JSON);
        return indexRequest;
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
    @SuppressWarnings("unused")
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
