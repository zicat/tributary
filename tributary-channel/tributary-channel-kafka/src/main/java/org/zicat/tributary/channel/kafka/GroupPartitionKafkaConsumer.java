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

import static org.apache.kafka.clients.consumer.ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_ID_CONFIG;
import static org.zicat.tributary.channel.kafka.PartitionKafkaConsumer.getKafkaOffset;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.zicat.tributary.channel.GroupOffset;
import org.zicat.tributary.channel.Offset;
import org.zicat.tributary.common.IOUtils;

import java.io.Closeable;
import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/** GroupPartitionKafkaConsumer. */
public class GroupPartitionKafkaConsumer implements Closeable {

    private final AtomicBoolean closed = new AtomicBoolean();
    private final String groupId;
    private final KafkaConsumer<byte[], byte[]> consumer;
    private final TopicPartition topicPartition;
    private final Collection<TopicPartition> singleTopicPartition;
    private Offset nextExpectOffset = null;

    public GroupPartitionKafkaConsumer(
            String groupId, TopicPartition topicPartition, Properties properties) {
        this.groupId = groupId;
        this.topicPartition = topicPartition;
        this.singleTopicPartition = Collections.singletonList(topicPartition);
        this.consumer = new KafkaConsumer<>(copyProperties(groupId, properties));
        consumer.assign(Collections.singletonList(topicPartition));
    }

    /**
     * copy properties.
     *
     * @param groupId groupId
     * @param properties properties
     * @return Properties
     */
    private static Properties copyProperties(String groupId, Properties properties) {
        final Properties result = new Properties();
        result.putAll(properties);
        result.put(GROUP_ID_CONFIG, groupId);
        result.put(ENABLE_AUTO_COMMIT_CONFIG, false);
        return result;
    }

    /**
     * end offset.
     *
     * @return offset
     */
    public synchronized Long endOffsets() {
        checkOpen();
        final Map<TopicPartition, Long> result = consumer.endOffsets(singleTopicPartition);
        return result == null ? null : result.get(topicPartition);
    }

    /**
     * poll data from kafka consumer.
     *
     * @param offset offset
     * @param time time
     * @param unit unit
     * @return ConsumerRecords
     */
    public synchronized KafkaRecordsResultSet poll(Offset offset, long time, TimeUnit unit) {
        checkOpen();
        if (!offset.equals(nextExpectOffset)) {
            consumer.seek(topicPartition, getKafkaOffset(offset));
        }
        final ConsumerRecords<byte[], byte[]> records =
                consumer.poll(Duration.ofMillis(unit.toMillis(time)));
        final GroupOffset groupOffset = GroupOffset.cast(offset, groupId);
        final KafkaRecordsResultSet resultSet = new KafkaRecordsResultSet(records, groupOffset);
        this.nextExpectOffset = resultSet.nexGroupOffset();
        return resultSet;
    }

    /**
     * commit offset.
     *
     * @param offset offset
     */
    public synchronized void commit(Offset offset) {
        checkOpen();
        final OffsetAndMetadata metadata = new OffsetAndMetadata(getKafkaOffset(offset));
        consumer.commitSync(Collections.singletonMap(topicPartition, metadata));
    }

    @Override
    public synchronized void close() {
        if (closed.compareAndSet(false, true)) {
            IOUtils.closeQuietly(consumer);
        }
    }

    /** check open. */
    private void checkOpen() {
        if (closed.get()) {
            throw new IllegalStateException("group partition kafka consumer closed");
        }
    }
}
