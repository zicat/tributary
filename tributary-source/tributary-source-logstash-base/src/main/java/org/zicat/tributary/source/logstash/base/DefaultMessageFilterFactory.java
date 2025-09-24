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

import com.fasterxml.jackson.databind.ObjectMapper;
import org.zicat.tributary.common.ConfigOption;
import org.zicat.tributary.common.ConfigOptions;
import org.zicat.tributary.common.ReadableConfig;
import org.zicat.tributary.common.records.DefaultRecord;
import org.zicat.tributary.common.records.DefaultRecords;
import org.zicat.tributary.common.records.Record;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;

/** DefaultMessageFilterFactory. */
public class DefaultMessageFilterFactory implements MessageFilterFactory {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    public static final String IDENTITY = "default_message_filter";
    public static final ConfigOption<String> OPTION_TOPIC =
            ConfigOptions.key("topic").stringType().defaultValue("default_topic");
    private String topic;

    @Override
    public void open(ReadableConfig config) {
        this.topic = config.get(OPTION_TOPIC);
    }

    @Override
    public MessageFilter<Object> getMessageFilter() {
        return (topic, iterator) -> {
            final Collection<Record> result = new ArrayList<>();
            while (iterator.hasNext()) {
                final Message<Object> message = iterator.next();
                if (message == null) {
                    continue;
                }
                final Map<String, Object> data = message.getData();
                result.add(new DefaultRecord(MAPPER.writeValueAsBytes(data)));
            }
            final String realTopic = topic == null ? DefaultMessageFilterFactory.this.topic : topic;
            return Collections.singletonList(new DefaultRecords(realTopic, result));
        };
    }

    @Override
    public void close() {}

    @Override
    public String identity() {
        return IDENTITY;
    }
}
