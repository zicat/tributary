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

package org.zicat.tributary.sink.elasticsearch.test;

import static org.zicat.tributary.sink.elasticsearch.DefaultRequestIndexer.DISCARD_COUNTER;
import static org.zicat.tributary.sink.elasticsearch.DefaultRequestIndexer.OPTION_REQUEST_INDEXER_DEFAULT_INDEX;

import static java.lang.Long.parseLong;

import com.carrotsearch.randomizedtesting.RandomizedRunner;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.zicat.tributary.channel.Offset;
import org.zicat.tributary.common.util.IOUtils;
import org.zicat.tributary.common.util.Threads;
import org.zicat.tributary.common.records.*;
import static org.zicat.tributary.sink.elasticsearch.ElasticsearchFunctionFactory.OPTION_REQUEST_TIMEOUT;
import org.zicat.tributary.sink.elasticsearch.listener.DefaultBulkResponseActionListener;
import org.zicat.tributary.sink.elasticsearch.DefaultRequestIndexer;
import org.zicat.tributary.sink.elasticsearch.ElasticsearchFunction;
import org.zicat.tributary.sink.config.Context;
import org.zicat.tributary.sink.config.ContextBuilder;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

/** ElasticsearchFunctionTest. */
@SuppressWarnings("ALL")
@RunWith(RandomizedRunner.class)
@ThreadLeakScope(ThreadLeakScope.Scope.NONE)
public class ElasticsearchFunctionTest extends ESSingleNodeTestCase {

    private static final String topic = "kt1";

    @Test
    public void test() throws Exception {

        final long start = System.currentTimeMillis();
        final ContextBuilder builder =
                new ContextBuilder()
                        .id("f1")
                        .groupId("g1")
                        .partitionId(0)
                        .topic("t1")
                        .addConfig(OPTION_REQUEST_INDEXER_DEFAULT_INDEX, topic)
                        .addConfig(OPTION_REQUEST_TIMEOUT, Duration.ofSeconds(3));
        final Context context = builder.build();

        final Map<String, byte[]> recordHeader1 = new HashMap<>();
        recordHeader1.put("rhk1", "rhv1".getBytes());
        final Record record1 =
                new DefaultRecord(
                        recordHeader1,
                        "rk1".getBytes(),
                        "{\"name\":\"n1\", \"score\":10}".getBytes());

        final Map<String, byte[]> recordHeader2 = new HashMap<>();
        recordHeader2.put("rhk2", "rhv2".getBytes());
        final Record record2 =
                new DefaultRecord(
                        recordHeader2,
                        null,
                        "{\"name\":\"中\", \"score\":20}".getBytes(StandardCharsets.UTF_8));

        final Map<String, byte[]> recordHeader3 = new HashMap<>();
        recordHeader3.put("rhk3", "rhv3".getBytes());
        final Record record3 =
                new DefaultRecord(recordHeader3, null, "{\"name\":\"n1\", score:30}".getBytes());

        final Map<String, byte[]> recordsHeader = new HashMap<>();
        recordsHeader.put("rshk1", "rshv1".getBytes());
        final DefaultRecords records =
                new DefaultRecords(topic, recordsHeader, Arrays.asList(record1, record2, record3));

        final List<Records> recordsList = Collections.singletonList(records);

        final ElasticsearchFunctionITCase function =
                new ElasticsearchFunctionITCase(node().client());
        function.open(context);
        try {
            function.process(Offset.ZERO, recordsList.iterator());
            Assert.assertEquals(3, function.sinkCount());
            Assert.assertEquals(0, (int) function.counterFamily().get(DISCARD_COUNTER).intValue());
            function.sync();
            Assert.assertEquals(Offset.ZERO, function.committableOffset());
            Assert.assertTrue(function.isEmpty());
            function.sync();
            Assert.assertEquals(1, function.bulkInsertCount());
            node().client().admin().indices().refresh(new RefreshRequest(topic)).actionGet();

            final GetResponse response = node().client().get(new GetRequest(topic, "rk1")).get();
            final Map<String, Object> source = response.getSource();
            Assert.assertEquals(topic, source.get(DefaultRequestIndexer.KEY_TOPIC));
            Assert.assertEquals("n1", source.get("name"));
            Assert.assertEquals(10, source.get("score"));
            Assert.assertEquals("rhv1", source.get("rhk1"));
            Assert.assertEquals("rshv1", source.get("rshk1"));
            final long sentTs = parseLong(source.get(RecordsUtils.HEAD_KEY_SENT_TS).toString());
            Assert.assertTrue(sentTs >= start && sentTs <= System.currentTimeMillis());

            final QueryBuilder query = new TermQueryBuilder("name", "中");
            final SearchResponse searchResponse =
                    node().client()
                            .search(
                                    new SearchRequest(topic)
                                            .source(
                                                    new SearchSourceBuilder()
                                                            .from(0)
                                                            .size(1)
                                                            .query(query)))
                            .get();
            Assert.assertEquals(1L, searchResponse.getHits().getTotalHits().value);
            final Map<String, Object> source2 =
                    searchResponse.getHits().getHits()[0].getSourceAsMap();
            Assert.assertEquals(topic, source2.get(DefaultRequestIndexer.KEY_TOPIC));
            Assert.assertEquals("中", source2.get("name"));
            Assert.assertEquals(20, source2.get("score"));
            Assert.assertEquals("rhv2", source2.get("rhk2"));
            Assert.assertEquals("rshv1", source2.get("rshk1"));
            final long sentTs2 = parseLong(source2.get(RecordsUtils.HEAD_KEY_SENT_TS).toString());
            Assert.assertTrue(sentTs2 >= start && sentTs2 <= System.currentTimeMillis());

            // test timeout
            function.setSleepTime(10000);
            function.process(new Offset(0, 1), recordsList.iterator());
            function.process(new Offset(0, 2), recordsList.iterator());
            try {
                function.process(new Offset(0, 3), recordsList.iterator());
                function.sync();
                Assert.fail();
            } catch (Exception ignore) {
                Assert.assertTrue(function.isEmpty());
                Assert.assertEquals(Offset.ZERO, function.committableOffset());
            }
            function.setSleepTime(0);
            function.process(new Offset(0, 10), recordsList.iterator());
        } finally {
            IOUtils.closeQuietly(function);
        }
        Assert.assertTrue(function.isEmpty());
        Assert.assertEquals(2, function.bulkInsertCount());
    }

    /** ElasticsearchFunctionMock. */
    private static class ElasticsearchFunctionITCase extends ElasticsearchFunction {

        private final Client client;
        private final AtomicInteger bulkInsertCounter = new AtomicInteger();
        private volatile int sleepTime = 0;

        public ElasticsearchFunctionITCase(Client client) {
            this.client = client;
        }

        @Override
        protected RestHighLevelClient createRestHighLevelClient(Context context) {
            return null;
        }

        @Override
        protected void bulkAsync(BulkRequest request, ActionListener<BulkResponse> listener) {
            client.bulk(request, listener);
        }

        public void setSleepTime(int sleepTime) {
            this.sleepTime = sleepTime;
        }

        @Override
        protected DefaultBulkResponseActionListener createAbstractActionListener(Offset offset) {
            return new DefaultBulkResponseActionListener(offset) {
                @Override
                public void onResponse(BulkResponse response) {
                    bulkInsertCounter.incrementAndGet();
                    Threads.sleepQuietly(sleepTime);
                    super.onResponse(response);
                }

                @Override
                public void onFailure(Exception e) {
                    bulkInsertCounter.incrementAndGet();
                    Threads.sleepQuietly(sleepTime);
                    super.onFailure(e);
                }
            };
        }

        public long sinkCount() {
            return sinkCounter;
        }

        public int bulkInsertCount() {
            return bulkInsertCounter.get();
        }

        public boolean isEmpty() {
            return listenerQueue.isEmpty();
        }
    }
}
