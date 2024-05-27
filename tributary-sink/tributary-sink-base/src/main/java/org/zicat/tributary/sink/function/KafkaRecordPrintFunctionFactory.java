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

package org.zicat.tributary.sink.function;

import org.apache.kafka.common.record.MutableRecordBatch;
import org.apache.kafka.common.record.Record;
import org.zicat.tributary.channel.GroupOffset;
import org.zicat.tributary.common.BytesUtils;
import org.zicat.tributary.common.KafkaRecordSerDeSer;
import org.zicat.tributary.common.KafkaRecordSerDeSer.KafkaTopicPartitionRecords;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Iterator;

/** KafkaRecordPrintFunctionFactory. */
public class KafkaRecordPrintFunctionFactory extends PrintFunctionFactory {

    private static final KafkaRecordSerDeSer DE_SER = new KafkaRecordSerDeSer();

    @Override
    public Function create() {
        return new PrintFunction() {

            @Override
            public void process(GroupOffset groupOffset, Iterator<byte[]> iterator) {
                int i = 0;
                while (iterator.hasNext()) {
                    final KafkaTopicPartitionRecords records = DE_SER.fromBytes(iterator.next());
                    final Iterator<MutableRecordBatch> it =
                            records.getMemoryRecords().batchIterator();
                    final String topic = new String(records.getTopic(), StandardCharsets.UTF_8);
                    final int partition = records.getPartition();
                    while (it.hasNext()) {
                        final MutableRecordBatch batch = it.next();
                        for (Record record : batch) {
                            final String key =
                                    record.hasKey() ? BytesUtils.toString(record.key()) : null;
                            final String value =
                                    record.hasValue() ? BytesUtils.toString(record.value()) : null;
                            LOG.info(
                                    "kafka record, topic:{},partition:{},key:{},value:{},extra_header:{},header:{}",
                                    topic,
                                    partition,
                                    key,
                                    value,
                                    records.getHeaders(),
                                    Arrays.asList(record.headers()));
                        }
                    }
                }
                commit(groupOffset, null);
                sinkCountChild.inc(i);
            }
        };
    }

    @Override
    public String identity() {
        return "kafka_record_print";
    }
}
