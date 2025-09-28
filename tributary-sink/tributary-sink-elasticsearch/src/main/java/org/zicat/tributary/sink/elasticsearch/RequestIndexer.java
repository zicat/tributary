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

import org.elasticsearch.action.bulk.BulkRequest;
import org.zicat.tributary.common.MetricCollector;
import org.zicat.tributary.common.MetricKey;
import org.zicat.tributary.common.SpiFactory;
import org.zicat.tributary.sink.function.Context;

import java.io.Closeable;
import java.util.Collections;
import java.util.Map;

/** RequestIndexer. */
public interface RequestIndexer extends SpiFactory, Closeable, MetricCollector {

    /**
     * open .
     *
     * @param context context
     */
    void open(Context context);

    /**
     * Add a document to the request.
     *
     * @param bulkRequest bulkRequest
     * @param topic topic
     * @param key key
     * @param value value
     * @param headers headers
     */
    boolean add(
            BulkRequest bulkRequest,
            String topic,
            byte[] key,
            byte[] value,
            Map<String, byte[]> headers)
            throws Exception;

    @Override
    default Map<MetricKey, Double> gaugeFamily() {
        return Collections.emptyMap();
    }

    @Override
    default Map<MetricKey, Double> counterFamily() {
        return Collections.emptyMap();
    }
}
