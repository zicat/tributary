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

import static org.zicat.tributary.common.SpiFactory.findFactory;
import static org.zicat.tributary.common.records.RecordsUtils.defaultSinkExtraHeaders;
import static org.zicat.tributary.common.records.RecordsUtils.foreachRecord;
import static org.zicat.tributary.sink.elasticsearch.ElasticsearchFunctionFactory.*;

import io.prometheus.client.Counter;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.zicat.tributary.channel.Offset;
import org.zicat.tributary.common.IOUtils;
import org.zicat.tributary.common.records.Records;
import org.zicat.tributary.sink.elasticsearch.listener.AbstractActionListener;
import org.zicat.tributary.sink.elasticsearch.listener.BulkResponseActionListenerFactory;
import org.zicat.tributary.sink.function.AbstractFunction;
import org.zicat.tributary.sink.function.Context;

import java.util.Iterator;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/** ElasticsearchFunction. */
@SuppressWarnings("deprecation")
public class ElasticsearchFunction extends AbstractFunction {

    private static final Counter SINK_ELASTICSEARCH_COUNTER =
            Counter.build()
                    .name("tributary_sink_elasticsearch_counter")
                    .help("tributary sink elasticsearch counter")
                    .labelNames("host", "id")
                    .register();
    private static final Counter SINK_ELASTICSEARCH_BULK_COUNTER =
            Counter.build()
                    .name("tributary_sink_elasticsearch_bulk_counter")
                    .help("tributary sink elasticsearch bulk counter")
                    .labelNames("host", "id")
                    .register();

    protected transient RestHighLevelClient client;
    protected transient Counter.Child sinkCounter;
    protected transient Counter.Child sinkBulkCounter;
    protected transient RequestIndexer indexer;
    protected transient BulkResponseActionListenerFactory listenerFactory;
    protected transient BlockingQueue<AbstractActionListener> listenerQueue;
    protected transient long awaitTimeout;
    protected transient volatile BulkRequest request = new BulkRequest();
    protected int buckMaxCount;
    protected long buckMaxBytes;
    protected Offset lastOffset;

    @Override
    public void open(Context context) throws Exception {
        super.open(context);
        buckMaxCount = context.get(OPTION_BUCK_MAX_COUNT);
        buckMaxBytes = context.get(OPTION_BULK_MAX_BYTES).getBytes();
        client = createRestHighLevelClient(context);
        sinkCounter = labelHostId(SINK_ELASTICSEARCH_COUNTER);
        sinkBulkCounter = labelHostId(SINK_ELASTICSEARCH_BULK_COUNTER);
        listenerQueue = new ArrayBlockingQueue<>(context.get(OPTION_ASYNC_BULK_QUEUE_SIZE));
        awaitTimeout = context.get(QUEUE_FULL_AWAIT_TIMEOUT).toMillis();
        indexer = findFactory(context.get(OPTION_REQUEST_INDEXER_IDENTITY), RequestIndexer.class);
        indexer.open(context);
        listenerFactory =
                findFactory(
                        context.get(OPTION_BULK_RESPONSE_ACTION_LISTENER_IDENTITY),
                        BulkResponseActionListenerFactory.class);
    }

    @Override
    public void process(Offset offset, Iterator<Records> iterator) throws Exception {
        checkFirstListenerStateIfDone();
        final AtomicInteger sendCount = new AtomicInteger();
        while (iterator.hasNext()) {
            final Records records = iterator.next();
            final String topic = records.topic();
            foreachRecord(
                    records,
                    (key, value, headers) -> {
                        if (indexer.add(request, topic, key, value, headers)) {
                            sendCount.incrementAndGet();
                        }
                    },
                    defaultSinkExtraHeaders());
        }
        sinkCounter.inc(sendCount.get());
        lastOffset = offset;
        if (request.numberOfActions() >= buckMaxCount
                || request.estimatedSizeInBytes() >= buckMaxBytes) {
            sendAsync(offset);
        }
    }

    @Override
    public void snapshot() throws Exception {
        sendAsync(lastOffset);
        checkAndClearDoneListeners();
    }

    /**
     * send bulk request.
     *
     * @param offset offset
     */
    protected void sendAsync(Offset offset) throws Exception {
        if (request.numberOfActions() == 0) {
            return;
        }
        final AbstractActionListener listener = createAbstractActionListener(offset);
        if (!listenerQueue.offer(listener)) {
            awaitFirstListenerFinished();
            listenerQueue.add(listener);
        }
        bulkAsync(request, listener);
        sinkBulkCounter.inc();
        request = new BulkRequest();
    }

    /**
     * create AbstractActionListener.
     *
     * @param offset offset.
     * @return AbstractActionListener
     */
    protected AbstractActionListener createAbstractActionListener(Offset offset) {
        return listenerFactory.create(context, request, offset);
    }

    /**
     * bulk async.
     *
     * @param request request
     * @param listener listener
     */
    protected void bulkAsync(BulkRequest request, ActionListener<BulkResponse> listener) {
        client.bulkAsync(request, RequestOptions.DEFAULT, listener);
    }

    /**
     * create RestHighLevelClient.
     *
     * @param context context
     * @return RestHighLevelClient
     */
    protected RestHighLevelClient createRestHighLevelClient(Context context) {
        return new RestHighLevelClient(createRestClientBuilder(context));
    }

    @Override
    public void close() {
        try {
            sync();
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            IOUtils.closeQuietly(indexer, client);
        }
    }

    /**
     * wait down.
     *
     * @throws Exception Exception
     */
    public void sync() throws Exception {
        sendAsync(lastOffset);
        if (listenerQueue == null) {
            return;
        }
        while (!listenerQueue.isEmpty()) {
            awaitFirstListenerFinished();
        }
    }

    /** check and clear done listeners. */
    protected void checkAndClearDoneListeners() {
        AbstractActionListener listener;
        // quick remove all finished listeners orderly in queue
        while ((listener = listenerQueue.peek()) != null) {
            if (listener.isRunning()) {
                break;
            }
            checkListenerException(listener);
            listenerQueue.poll();
        }
    }

    /**
     * consumer first listener.
     *
     * @throws Exception Exception
     */
    protected void awaitFirstListenerFinished() throws Exception {
        final AbstractActionListener listener = listenerQueue.poll();
        if (listener == null) {
            return;
        }
        if (!listener.awaitDone(awaitTimeout, TimeUnit.MILLISECONDS)) {
            listenerQueue.clear();
            throw new IllegalStateException("await listener timeout " + awaitTimeout + " ms");
        }
        checkListenerException(listener);
    }

    /** checkFirstListenerStateIfDone. */
    protected void checkFirstListenerStateIfDone() {
        final AbstractActionListener listener = listenerQueue.peek();
        if (listener == null || listener.isRunning()) {
            return;
        }
        checkListenerException(listener);
        listenerQueue.poll();
    }

    /**
     * check listener exception.
     *
     * @param listener listener
     */
    private void checkListenerException(AbstractActionListener listener) {
        final Exception e = listener.exception();
        if (e != null) {
            listenerQueue.clear();
            throw new RuntimeException(e);
        } else {
            commit(listener.offset());
        }
    }
}
