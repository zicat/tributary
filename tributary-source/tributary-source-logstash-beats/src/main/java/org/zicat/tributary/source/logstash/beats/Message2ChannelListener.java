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

package org.zicat.tributary.source.logstash.beats;

import org.zicat.tributary.common.records.Records;
import org.zicat.tributary.source.base.Source;
import org.zicat.tributary.source.logstash.base.Message;
import org.zicat.tributary.source.logstash.base.MessageFilterFactory;

import java.util.*;

/** Message2ChannelListener. */
public class Message2ChannelListener implements BatchMessageListener {

    private final Source source;
    private final int partition;
    private final MessageFilterFactory messageFilterFactory;

    public Message2ChannelListener(
            Source source, int partition, MessageFilterFactory messageFilterFactory) {
        this.source = source;
        this.partition = partition;
        this.messageFilterFactory = messageFilterFactory;
    }

    @Override
    public void consume(Iterator<Message<Object>> iterator) throws Exception {
        final Iterable<Records> recordsList =
                messageFilterFactory.getMessageFilter().convert(source.topic(), iterator);
        if (recordsList == null) {
            return;
        }
        for (Records records : recordsList) {
            source.append(partition, records);
        }
    }
}
