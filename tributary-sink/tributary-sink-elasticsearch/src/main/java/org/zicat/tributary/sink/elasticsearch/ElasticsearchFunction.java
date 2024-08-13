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
import org.elasticsearch.action.ActionListener;
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

import java.util.Iterator;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.zicat.tributary.common.SpiFactory.findFactory;
import static org.zicat.tributary.common.records.RecordsUtils.defaultSinkExtraHeaders;
import static org.zicat.tributary.common.records.RecordsUtils.foreachRecord;
import static org.zicat.tributary.sink.elasticsearch.ElasticsearchFunctionFactory.*;

/** ElasticsearchFunction. */
@SuppressWarnings("deprecation")
public class ElasticsearchFunction extends AbstractFunction {

    private static final Exception NO_EXCEPTION = new Exception();
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
    protected transient BlockingQueue<DefaultActionListener> listenerQueue;

    @Override
    public void open(Context context) throws Exception {
        super.open(context);
        client = new RestHighLevelClient(createRestClientBuilder(context));
        sinkCounterChild = labelHostId(SINK_ELASTICSEARCH_COUNTER);
        listenerQueue = new ArrayBlockingQueue<>(context.get(OPTION_ASYNC_BULK_QUEUE_SIZE));
        sinkDiscardCounterChild = labelHostId(SINK_ELASTICSEARCH_DISCARD_COUNTER);
        indexer = findFactory(context.get(OPTION_REQUEST_INDEXER_IDENTITY), RequestIndexer.class);
        indexer.open(context);
    }

    @Override
    public void process(Offset offset, Iterator<Records> iterator) throws Exception {
        checkState();
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
        sendAndCommit(request, offset);
        sinkCounterChild.inc(sendCount.get());
        sinkDiscardCounterChild.inc(discardCount.get());
    }

    /**
     * send bulk request.
     *
     * @param request request.
     * @param offset offset
     */
    protected void sendAndCommit(BulkRequest request, Offset offset) {
        if (request.numberOfActions() == 0) {
            return;
        }
        final DefaultActionListener listener = new DefaultActionListener(offset);
        if (!listenerQueue.offer(listener)) {
            listenerQueue.clear();
            throw new IllegalStateException("listener queue is full");
        }
        client.bulkAsync(request, RequestOptions.DEFAULT, listener);
    }

    @Override
    public void close() {
        IOUtils.closeQuietly(client);
    }

    /**
     * checkState.
     *
     * @throws Exception Exception
     */
    protected void checkState() throws Exception {
        DefaultActionListener listener;
        while ((listener = listenerQueue.peek()) != null) {
            if (!listener.isDone()) {
                break;
            }
            final Exception e = listener.exception();
            if (e == null) {
                listener.commit();
                listenerQueue.poll();
            } else {
                listenerQueue.clear();
                throw e;
            }
        }
    }

    /** DefaultActionListener. */
    protected class DefaultActionListener implements ActionListener<BulkResponse> {

        private final Offset offset;
        private final AtomicReference<Exception> state;

        public DefaultActionListener(Offset offset) {
            this.offset = offset;
            this.state = new AtomicReference<>();
        }

        @Override
        public void onResponse(BulkResponse response) {
            if (!response.hasFailures()) {
                state.set(NO_EXCEPTION);
                return;
            }
            for (BulkItemResponse item : response.getItems()) {
                if (!item.isFailed()) {
                    continue;
                }
                final String index = item.getIndex();
                final String id = item.getId();
                final String error = item.getFailureMessage();
                state.set(
                        new IllegalStateException(
                                "Failed to index document id: "
                                        + id
                                        + ", index: "
                                        + index
                                        + ", error: "
                                        + error));
                return;
            }
            state.set(NO_EXCEPTION);
        }

        @Override
        public void onFailure(Exception e) {
            state.set(e);
        }

        /** commit offset. */
        public void commit() {
            ElasticsearchFunction.super.commit(offset);
        }

        /**
         * check is done.
         *
         * @return return true if done.
         */
        public boolean isDone() {
            return state.get() != null;
        }

        /**
         * get exception.
         *
         * @return exception.
         */
        public Exception exception() {
            final Exception e = state.get();
            if (e == null) {
                throw new IllegalStateException("ActionListener is not done.");
            }
            return e == NO_EXCEPTION ? null : e;
        }
    }
}
