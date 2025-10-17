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

import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkResponse;
import org.zicat.tributary.channel.Offset;
import org.zicat.tributary.common.metric.MetricKey;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/** MuteInsertErrorBulkResponseActionListener. */
public class MuteInsertErrorBulkResponseActionListener extends AbstractActionListener {

    public static final MetricKey ERROR_COUNTER =
            new MetricKey("tributary_sink_elasticsearch_bulk_requests_with_errors");
    public static final String LABEL_INDEX = "index";

    private final Map<String, AtomicLong> indexErrorMapCounters = new HashMap<>();

    public MuteInsertErrorBulkResponseActionListener(Offset offset) {
        super(offset);
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
            String index = entry.getKey();
            int bulkErrorCnt = entry.getValue().get();
            indexErrorMapCounters
                    .computeIfAbsent(index, k -> new AtomicLong(0L))
                    .addAndGet(bulkErrorCnt);
        }
        return null;
    }

    @Override
    public Map<MetricKey, Double> counterFamily() {
        final Map<MetricKey, Double> base = new HashMap<>(super.counterFamily());
        for (Entry<String, AtomicLong> entry : indexErrorMapCounters.entrySet()) {
            final String index = entry.getKey();
            final double value = entry.getValue().doubleValue();
            base.merge(ERROR_COUNTER.addLabel(LABEL_INDEX, index), value, Double::sum);
        }
        return base;
    }
}
