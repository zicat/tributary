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

package org.zicat.tributary.source.logstash.base;

import org.zicat.tributary.common.records.Records;

import java.util.Collections;
import java.util.Iterator;

/**
 * MessageFilter.
 *
 * @param <PAYLOAD> payload
 */
public interface MessageFilter<PAYLOAD> {

    /**
     * filter message.
     *
     * @param iterator iterator
     * @return convert data to record, filter if null
     */
    Iterable<Records> convert(String topic, Iterator<Message<PAYLOAD>> iterator) throws Exception;

    /**
     * filter single message.
     *
     * @param message message
     * @return Records
     * @throws Exception Exception
     */
    default Records convert(String topic, Message<PAYLOAD> message) throws Exception {
        final Iterable<Records> recordsList = convert(topic, Collections.singleton(message).iterator());
        return recordsList == null ? null : recordsList.iterator().next();
    }
}
