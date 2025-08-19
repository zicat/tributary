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

import org.logstash.beats.Message;
import org.zicat.tributary.common.records.DefaultRecord;
import org.zicat.tributary.common.records.DefaultRecords;
import org.zicat.tributary.common.records.Record;
import org.zicat.tributary.source.base.Source;

import java.io.IOException;
import java.util.*;

/** Message2ChannelListener. */
public class Message2ChannelListener implements BatchMessageListener {

    private static final String HEAD_KEY_SEQUENCE = "sequence";
    private final Source source;
    private final int partition;

    public Message2ChannelListener(Source source, int partition) {
        this.source = source;
        this.partition = partition;
    }

    @Override
    public void consume(Iterator<Message> iterator) throws InterruptedException, IOException {
        final List<Record> recordList = new ArrayList<>();
        while (iterator.hasNext()) {
            final Message message = iterator.next();
            if (message == null) {
                continue;
            }
            final Record record =
                    new DefaultRecord(
                            Collections.singletonMap(
                                    HEAD_KEY_SEQUENCE,
                                    String.valueOf(message.getSequence()).getBytes()),
                            message.getData());
            recordList.add(record);
        }
        source.append(partition, new DefaultRecords(source.sourceId(), null, recordList));
    }
}
