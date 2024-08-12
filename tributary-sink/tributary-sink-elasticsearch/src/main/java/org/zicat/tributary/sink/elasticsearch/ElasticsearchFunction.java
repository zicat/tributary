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

import io.prometheus.client.Counter;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.zicat.tributary.channel.Offset;
import org.zicat.tributary.common.IOUtils;
import org.zicat.tributary.common.records.Records;
import org.zicat.tributary.sink.function.AbstractFunction;
import org.zicat.tributary.sink.function.Context;

import java.io.IOException;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicInteger;

import static org.zicat.tributary.common.SpiFactory.findFactory;
import static org.zicat.tributary.common.records.RecordsUtils.defaultSinkExtraHeaders;
import static org.zicat.tributary.common.records.RecordsUtils.foreachRecord;
import static org.zicat.tributary.sink.elasticsearch.ElasticsearchFunctionFactory.OPTION_REQUEST_INDEXER_IDENTITY;
import static org.zicat.tributary.sink.elasticsearch.ElasticsearchFunctionFactory.createRestClientBuilder;

/** ElasticsearchFunction. */
@SuppressWarnings("deprecation")
public class ElasticsearchFunction extends AbstractFunction {

    private static final Counter SINK_ELASTICSEARCH_COUNTER =
            Counter.build()
                    .name("sink_elasticsearch_counter")
                    .help("sink elasticsearch counter")
                    .labelNames("host", "id")
                    .register();

    private static final Counter SINK_ELASTICSEARCH_DISCARD_COUNTER =
            Counter.build()
                    .name("sink_elasticsearch_discard_counter")
                    .help("sink elasticsearch discard counter")
                    .labelNames("host", "id")
                    .register();

    protected transient RestHighLevelClient client;
    protected transient Counter.Child sinkCounterChild;
    protected transient Counter.Child sinkDiscardCounterChild;
    protected transient RequestIndexer indexer;

    @Override
    public void open(Context context) throws Exception {
        super.open(context);
        client = new RestHighLevelClient(createRestClientBuilder(context));
        sinkCounterChild = labelHostId(SINK_ELASTICSEARCH_COUNTER);
        sinkDiscardCounterChild = labelHostId(SINK_ELASTICSEARCH_DISCARD_COUNTER);
        indexer = findFactory(context.get(OPTION_REQUEST_INDEXER_IDENTITY), RequestIndexer.class);
        indexer.open(context);
    }

    @Override
    public void process(Offset offset, Iterator<Records> iterator) throws Exception {
        final AtomicInteger sendCount = new AtomicInteger();
        final AtomicInteger discardCount = new AtomicInteger();
        final BulkRequest request = new BulkRequest();
        while (iterator.hasNext()) {
            final Records records = iterator.next();
            final String topic = records.topic();
            foreachRecord(
                    records,
                    (key, value, allHeaders) -> {
                        final boolean success = indexer.add(request, topic, key, value, allHeaders);
                        if (success) {
                            sendCount.incrementAndGet();
                        } else {
                            discardCount.incrementAndGet();
                        }
                    },
                    defaultSinkExtraHeaders());
        }
        send(request);
        commit(offset);
        sinkCounterChild.inc(sendCount.get());
        sinkDiscardCounterChild.inc(discardCount.get());
    }

    /**
     * send bulk request.
     *
     * @param request request.
     * @throws IOException IOException
     */
    protected void send(BulkRequest request) throws IOException {
        if (request.numberOfActions() == 0) {
            return;
        }

        /*
           bulkAsync may cause previous-error request callback later than next-success request.
           In this case, the success request will commit offset cause previous-error request data lost.
           So use bulk sync api to avoid this issue.
        */
        final BulkResponse response = client.bulk(request, RequestOptions.DEFAULT);
        if (!response.hasFailures()) {
            return;
        }
        for (BulkItemResponse item : response.getItems()) {
            if (!item.isFailed()) {
                continue;
            }
            final String index = item.getIndex();
            final String id = item.getId();
            final String error = item.getFailureMessage();
            throw new RuntimeException(
                    String.format(
                            "Failed to index document with id %s in index %s: %s",
                            id, index, error));
        }
    }

    @Override
    public void close() {
        IOUtils.closeQuietly(client);
    }
}
