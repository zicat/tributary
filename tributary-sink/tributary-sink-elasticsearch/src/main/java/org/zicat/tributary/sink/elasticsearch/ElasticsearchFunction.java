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

import org.zicat.tributary.common.MetricKey;
import org.zicat.tributary.common.SimpleLRUCache;
import static org.zicat.tributary.common.SpiFactory.findFactory;
import static org.zicat.tributary.common.records.RecordsUtils.foreachRecord;
import static org.zicat.tributary.sink.elasticsearch.ElasticsearchFunctionFactory.*;

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

import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

/** ElasticsearchFunction. */
@SuppressWarnings("deprecation")
public class ElasticsearchFunction extends AbstractFunction {

    private static final MetricKey COUNTER = new MetricKey("tributary_sink_elasticsearch_counter");
    private static final MetricKey BULK_COUNTER =
            new MetricKey("tributary_sink_elasticsearch_bulk_counter");

    protected transient RestHighLevelClient client;
    protected transient long sinkCounter;
    protected transient long sinkBulkCounter;
    protected transient RequestIndexer indexer;
    protected transient BulkResponseActionListenerFactory listenerFactory;
    protected transient BlockingQueue<AbstractActionListener> listenerQueue;
    protected transient long awaitTimeout;
    protected transient volatile BulkRequest request = new BulkRequest();
    protected int buckMaxCount;
    protected long buckMaxBytes;
    protected Offset lastOffset;
    protected Map<MetricKey, Double> listenerCounterMetrics = SimpleLRUCache.create(1024);
    protected Map<MetricKey, Double> listenerGaugeMetrics = SimpleLRUCache.create(1024);

    @Override
    public void open(Context context) throws Exception {
        super.open(context);
        buckMaxCount = context.get(OPTION_BUCK_MAX_COUNT);
        buckMaxBytes = context.get(OPTION_BULK_MAX_BYTES).getBytes();
        awaitTimeout = context.get(OPTION_REQUEST_TIMEOUT).toMillis();
        listenerQueue = new ArrayBlockingQueue<>(calculateQueueSize(context));
        client = createRestHighLevelClient(context);
        listenerFactory =
                findFactory(
                        context.get(OPTION_BULK_RESPONSE_ACTION_LISTENER_IDENTITY),
                        BulkResponseActionListenerFactory.class);
        indexer = findFactory(context.get(OPTION_REQUEST_INDEXER_IDENTITY), RequestIndexer.class);
        indexer.open(context);
    }

    @Override
    public void process(Offset offset, Iterator<Records> iterator) throws Exception {
        checkFirstListenerStateIfDone();
        while (iterator.hasNext()) {
            final Records records = iterator.next();
            final String topic = records.topic();
            foreachRecord(
                    records,
                    (key, value, headers) -> {
                        if (indexer.add(request, topic, key, value, headers)) {
                            sinkCounter++;
                        }
                    },
                    defaultSinkExtraHeaders());
        }
        if (request.numberOfActions() >= buckMaxCount
                || request.estimatedSizeInBytes() >= buckMaxBytes) {
            sendAsync(offset);
        }
        lastOffset = offset;
    }

    @Override
    public void snapshot() throws Exception {
        sendAsync(lastOffset);
        checkAndClearDoneListeners();
    }

    @Override
    public Map<MetricKey, Double> gaugeFamily() {
        final Map<MetricKey, Double> base = new HashMap<>(super.gaugeFamily());
        for (Map.Entry<MetricKey, Double> entry : indexer.gaugeFamily().entrySet()) {
            base.merge(entry.getKey(), entry.getValue(), Double::sum);
        }
        for (Map.Entry<MetricKey, Double> entry : listenerGaugeMetrics.entrySet()) {
            base.merge(entry.getKey(), entry.getValue(), Double::sum);
        }
        return base;
    }

    @Override
    public Map<MetricKey, Double> counterFamily() {
        final Map<MetricKey, Double> base = new HashMap<>(super.counterFamily());
        base.merge(COUNTER, (double) sinkCounter, Double::sum);
        base.merge(BULK_COUNTER, (double) sinkBulkCounter, Double::sum);
        for (Map.Entry<MetricKey, Double> entry : indexer.counterFamily().entrySet()) {
            base.merge(entry.getKey(), entry.getValue(), Double::sum);
        }
        for (Map.Entry<MetricKey, Double> entry : listenerCounterMetrics.entrySet()) {
            base.merge(entry.getKey(), entry.getValue(), Double::sum);
        }
        return base;
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
        sinkBulkCounter++;
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
        if (listenerQueue == null || listenerQueue.isEmpty()) {
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
            addListenerMetrics(listenerQueue.poll());
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
        addListenerMetrics(listener);
    }

    /** checkFirstListenerStateIfDone. */
    protected void checkFirstListenerStateIfDone() {
        final AbstractActionListener listener = listenerQueue.peek();
        if (listener == null || listener.isRunning()) {
            return;
        }
        checkListenerException(listener);
        addListenerMetrics(listenerQueue.poll());
    }

    /**
     * calculate queue size.
     *
     * @param context context
     * @return queue size
     */
    protected int calculateQueueSize(Context context) {
        final int threadMaxPerRouting = context.get(OPTION_THREAD_MAX_PER_ROUTING);
        final List<String> hosts = context.get(OPTION_HOSTS, Collections.emptyList());
        return Math.max(threadMaxPerRouting * hosts.size() * 2, 2);
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

    /**
     * addListenerMetrics.
     *
     * @param listener listener
     */
    private void addListenerMetrics(AbstractActionListener listener) {
        if (listener == null) {
            return;
        }
        for (Map.Entry<MetricKey, Double> entry : listener.counterFamily().entrySet()) {
            listenerCounterMetrics.merge(entry.getKey(), entry.getValue(), Double::sum);
        }
        for (Map.Entry<MetricKey, Double> entry : listener.gaugeFamily().entrySet()) {
            listenerGaugeMetrics.merge(entry.getKey(), entry.getValue(), Double::sum);
        }
    }
}
