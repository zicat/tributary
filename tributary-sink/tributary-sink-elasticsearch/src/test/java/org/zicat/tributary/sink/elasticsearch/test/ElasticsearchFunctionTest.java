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

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.junit.Test;
import org.zicat.tributary.channel.Offset;
import org.zicat.tributary.common.records.DefaultRecord;
import org.zicat.tributary.common.records.DefaultRecords;
import org.zicat.tributary.common.records.Record;
import org.zicat.tributary.common.records.Records;
import org.zicat.tributary.sink.elasticsearch.ElasticsearchFunction;
import org.zicat.tributary.sink.function.Context;
import org.zicat.tributary.sink.function.ContextBuilder;

import java.util.*;

import static org.zicat.tributary.sink.elasticsearch.ElasticsearchFunctionFactory.OPTION_ASYNC_BULK_QUEUE_SIZE;
import static org.zicat.tributary.sink.function.AbstractFunction.OPTION_METRICS_HOST;

/** ElasticsearchFunctionTest. */
@SuppressWarnings("ALL")
public class ElasticsearchFunctionTest extends ESSingleNodeTestCase {

    private static final String topic = "kt1";

    @Test
    public void test() throws Exception {

        final ContextBuilder builder =
                new ContextBuilder()
                        .id("f1")
                        .groupId("g1")
                        .partitionId(0)
                        .topic("t1")
                        .startOffset(Offset.ZERO);
        builder.addCustomProperty(OPTION_METRICS_HOST, "localhost");
        builder.addCustomProperty(OPTION_ASYNC_BULK_QUEUE_SIZE, 2);

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
                        recordHeader2, null, "{\"name\":\"n1\", \"score\":20}".getBytes());

        final Map<String, byte[]> recordHeader3 = new HashMap<>();
        recordHeader3.put("rhk3", "rhv3".getBytes());
        final Record record3 =
                new DefaultRecord(recordHeader3, null, "{\"name\":\"n1\", score:30}".getBytes());

        final Map<String, byte[]> recordsHeader = new HashMap<>();
        recordsHeader.put("rshk1", "rshv1".getBytes());
        final DefaultRecords records =
                new DefaultRecords(topic, recordsHeader, Arrays.asList(record1, record2, record3));

        final List<Records> recordsList = Collections.singletonList(records);
        try (ElasticsearchFunctionITCase function =
                new ElasticsearchFunctionITCase(node().client())) {
            function.open(builder.build());
            function.process(Offset.ZERO, recordsList.iterator());
        }
    }

    /** ElasticsearchFunctionMock. */
    private static class ElasticsearchFunctionITCase extends ElasticsearchFunction {

        private final Client client;

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

        public void waitDone() {}
    }
}
