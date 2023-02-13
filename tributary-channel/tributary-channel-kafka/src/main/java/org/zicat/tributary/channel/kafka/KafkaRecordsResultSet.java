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

package org.zicat.tributary.channel.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.zicat.tributary.channel.GroupOffset;
import org.zicat.tributary.channel.RecordsResultSet;

import java.util.Iterator;
import java.util.concurrent.atomic.AtomicLong;

import static org.zicat.tributary.channel.kafka.PartitionKafkaConsumer.fromKafkaOffset;
import static org.zicat.tributary.channel.kafka.PartitionKafkaConsumer.getKafkaOffset;

/** KafkaRecordsResultSet. */
public class KafkaRecordsResultSet implements RecordsResultSet {

    private final GroupOffset nextOffset;
    private final Iterator<ConsumerRecord<byte[], byte[]>> iterator;
    private final AtomicLong readBytes = new AtomicLong();

    public KafkaRecordsResultSet(
            ConsumerRecords<byte[], byte[]> consumerRecords, GroupOffset preGroupOffset) {
        this.nextOffset = skipDelta(preGroupOffset, consumerRecords.count());
        this.iterator = consumerRecords.iterator();
        for (ConsumerRecord<byte[], byte[]> record : consumerRecords) {
            readBytes.getAndAdd(record.value().length);
        }
    }

    /**
     * skip to new group offset with delta offset.
     *
     * @param groupOffset groupOffset
     * @param delta delta
     * @return GroupOffset
     */
    public static GroupOffset skipDelta(GroupOffset groupOffset, long delta) {
        return fromKafkaOffset(getKafkaOffset(groupOffset) + delta, groupOffset.groupId());
    }

    @Override
    public GroupOffset nexGroupOffset() {
        return nextOffset;
    }

    @Override
    public long readBytes() {
        return readBytes.get();
    }

    @Override
    public boolean hasNext() {
        return iterator.hasNext();
    }

    @Override
    public byte[] next() {
        return iterator.next().value();
    }
}
