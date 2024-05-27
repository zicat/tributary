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

package org.zicat.tributary.sink.kafka;

import org.zicat.tributary.common.ConfigOption;
import org.zicat.tributary.common.ConfigOptions;
import org.zicat.tributary.common.ReadableConfig;

/** ProxyKafkaRecord2RecordFactory. */
public class ProxyKafkaRecord2RecordFactory implements Byte2RecordFactory {

    private static final ConfigOption<String> OPTION_TOPIC =
            ConfigOptions.key("topic")
                    .stringType()
                    .description("the kafka topic to send data")
                    .defaultValue(null);

    @Override
    public String identity() {
        return "kafkaRecordDecoder";
    }

    @Override
    public Byte2Record create(ReadableConfig readableConfig) {
        return new ProxyKafkaRecord2Record(readableConfig.get(OPTION_TOPIC));
    }
}
