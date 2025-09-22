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

package org.zicat.tributary.sink.elasticsearch.listener;

import io.prometheus.client.Counter;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkResponse;
import org.zicat.tributary.channel.Offset;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/** MuteInsertErrorBulkResponseActionListener. */
public class MuteInsertErrorBulkResponseActionListener extends AbstractActionListener {

    public static final Counter ERROR_COUNTER =
            Counter.build()
                    .name("tributary_sink_elasticsearch_bulk_requests_with_errors")
                    .help("tributary sink elasticsearch bulk requests with errors")
                    .labelNames("topic", "instance")
                    .register();

    private final String instance;

    public MuteInsertErrorBulkResponseActionListener(Offset offset, String instance) {
        super(offset);
        this.instance = instance;
    }

    @Override
    protected Exception checkResponseItems(BulkResponse response) {
        final Map<String, AtomicInteger> errorMap = new HashMap<>();
        for (BulkItemResponse item : response.getItems()) {
            if (!item.isFailed()) {
                continue;
            }
            errorMap.computeIfAbsent(item.getIndex(), k -> new AtomicInteger(0)).incrementAndGet();
        }
        for (Map.Entry<String, AtomicInteger> entry : errorMap.entrySet()) {
            String topic = entry.getKey();
            int bulkErrorCnt = entry.getValue().get();
            ERROR_COUNTER.labels(topic, instance).inc(bulkErrorCnt);
        }
        return null;
    }
}
