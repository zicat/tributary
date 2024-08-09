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
import org.zicat.tributary.common.ReadableConfig;
import org.zicat.tributary.common.SpiFactory;

import java.util.Map;

/** RequestIndexer. */
public interface RequestIndexer extends SpiFactory {

    /**
     * open .
     *
     * @param config config
     */
    void open(ReadableConfig config);

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
}
