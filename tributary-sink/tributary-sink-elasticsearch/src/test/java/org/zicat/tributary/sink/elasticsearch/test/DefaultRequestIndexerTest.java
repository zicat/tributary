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

import org.elasticsearch.action.bulk.BulkRequest;
import org.junit.Assert;
import org.junit.Test;
import static org.zicat.tributary.common.SpiFactory.findFactory;
import org.zicat.tributary.sink.elasticsearch.DefaultRequestIndexer;
import static org.zicat.tributary.sink.elasticsearch.DefaultRequestIndexer.OPTION_REQUEST_INDEXER_DEFAULT_INDEX;
import static org.zicat.tributary.sink.elasticsearch.DefaultRequestIndexer.OPTION_REQUEST_INDEX_DEFAULT_RECORD_SIZE_LIMIT;
import org.zicat.tributary.sink.elasticsearch.RequestIndexer;
import org.zicat.tributary.sink.function.ContextBuilder;

import java.util.Collections;

/** DefaultRequestIndexerTest. */
public class DefaultRequestIndexerTest {

    @Test
    public void testAdd() throws Exception {
        try (final RequestIndexer requestIndexer =
                findFactory(DefaultRequestIndexer.IDENTITY, RequestIndexer.class)) {
            Assert.assertEquals(DefaultRequestIndexer.class, requestIndexer.getClass());

            BulkRequest bulkRequest = new BulkRequest();
            ContextBuilder builder =
                    ContextBuilder.newBuilder().topic("t1").groupId("g1").partitionId(0).id("id1");
            builder.addCustomProperty(OPTION_REQUEST_INDEX_DEFAULT_RECORD_SIZE_LIMIT, "1K");
            requestIndexer.open(builder.build());

            Assert.assertFalse(
                    requestIndexer.add(
                            bulkRequest, null, null, "{}".getBytes(), Collections.emptyMap()));
            Assert.assertEquals(0, bulkRequest.numberOfActions());

            builder.addCustomProperty(OPTION_REQUEST_INDEXER_DEFAULT_INDEX, "default_index");
            requestIndexer.open(builder.build());
            Assert.assertTrue(
                    requestIndexer.add(
                            bulkRequest, null, null, "{}".getBytes(), Collections.emptyMap()));
            Assert.assertEquals(1, bulkRequest.numberOfActions());
        }

        try (final RequestIndexer requestIndexer =
                findFactory(DefaultRequestIndexer.IDENTITY, RequestIndexer.class)) {
            Assert.assertEquals(DefaultRequestIndexer.class, requestIndexer.getClass());

            BulkRequest bulkRequest = new BulkRequest();
            ContextBuilder builder =
                    ContextBuilder.newBuilder().topic("t1").groupId("g1").partitionId(0).id("id1");
            builder.addCustomProperty(OPTION_REQUEST_INDEX_DEFAULT_RECORD_SIZE_LIMIT, "1K");
            builder.addCustomProperty(OPTION_REQUEST_INDEXER_DEFAULT_INDEX, "default_index");
            requestIndexer.open(builder.build());

            Assert.assertFalse(
                    requestIndexer.add(
                            bulkRequest, null, null, new byte[1024], Collections.emptyMap()));
            Assert.assertEquals(0, bulkRequest.numberOfActions());

            Assert.assertFalse(
                    requestIndexer.add(
                            bulkRequest, null, null, "[]".getBytes(), Collections.emptyMap()));
            Assert.assertEquals(0, bulkRequest.numberOfActions());
        }
    }
}
