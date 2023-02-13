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

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsResult;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.zicat.tributary.channel.GroupOffset;
import org.zicat.tributary.channel.group.SingleGroupManager;
import org.zicat.tributary.common.IOUtils;
import org.zicat.tributary.common.TributaryRuntimeException;

import java.io.Closeable;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.kafka.clients.consumer.ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_ID_CONFIG;

/** PartitionKafkaConsumer. */
public class PartitionKafkaConsumer implements Closeable, SingleGroupManager {

    private static final int SEGMENT_SIZE = 100_000;

    private final AtomicBoolean closed = new AtomicBoolean();
    private final TopicPartition topicPartition;
    private final Set<String> groups;
    private final Properties properties;
    private final Map<String, KafkaConsumer<byte[], byte[]>> consumerGroupCache;

    public PartitionKafkaConsumer(
            TopicPartition topicPartition, Set<String> groups, Properties properties) {
        this.topicPartition = topicPartition;
        this.groups = groups;
        this.properties = properties;
        this.consumerGroupCache = createConsumerGroupCache(properties, groups, topicPartition);
    }

    /**
     * create consumer group cache.
     *
     * @param properties properties
     * @param groups groups
     * @param topicPartition topicPartition
     * @return kafka consumer map
     */
    private static Map<String, KafkaConsumer<byte[], byte[]>> createConsumerGroupCache(
            Properties properties, Set<String> groups, TopicPartition topicPartition) {
        final Map<String, KafkaConsumer<byte[], byte[]>> result = new ConcurrentHashMap<>();
        final Collection<TopicPartition> partitions = Collections.singletonList(topicPartition);
        for (String group : groups) {
            final Properties newProperties = copyProperties(group, properties);
            final KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(newProperties);
            consumer.assign(partitions);
            result.put(group, consumer);
        }
        return result;
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
        for (Map.Entry<Object, Object> entry : properties.entrySet()) {
            result.put(entry.getKey(), entry.getValue());
        }
        result.put(GROUP_ID_CONFIG, groupId);
        result.put(ENABLE_AUTO_COMMIT_CONFIG, false);
        return result;
    }

    /**
     * poll records from kafka topic .
     *
     * @param groupOffset groupOffset
     * @param time time
     * @param unit unit
     * @return KafkaRecordsResultSet
     */
    public KafkaRecordsResultSet poll(GroupOffset groupOffset, long time, TimeUnit unit) {
        final KafkaConsumer<byte[], byte[]> consumer = getKafkaConsumer(groupOffset.groupId());
        consumer.seek(topicPartition, getKafkaOffset(groupOffset));
        final ConsumerRecords<byte[], byte[]> consumerRecords =
                consumer.poll(Duration.ofMillis(unit.toMillis(time)));
        return new KafkaRecordsResultSet(consumerRecords, groupOffset);
    }

    /**
     * get kafka consumer by group id.
     *
     * @param groupId groupId
     * @return kafka consumer
     */
    private KafkaConsumer<byte[], byte[]> getKafkaConsumer(String groupId) {
        final KafkaConsumer<byte[], byte[]> consumer = consumerGroupCache.get(groupId);
        if (consumer == null) {
            throw new IllegalStateException(
                    "find kafka consumer by group id fai, group id = " + groupId);
        }
        return consumer;
    }

    /**
     * topic partition.
     *
     * @return offset
     */
    public Long endOffsets(String groupId) {
        final Map<TopicPartition, Long> result =
                getKafkaConsumer(groupId).endOffsets(Collections.singletonList(topicPartition));
        return result == null ? null : result.get(topicPartition);
    }

    /**
     * get kafka offset with group offset.
     *
     * @param groupOffset groupOffset
     * @return long offset
     */
    public static long getKafkaOffset(GroupOffset groupOffset) {
        return groupOffset.segmentId() * SEGMENT_SIZE + groupOffset.offset();
    }

    /**
     * from kafka offset.
     *
     * @param kafkaOffset kafkaOffset
     * @param groupId groupId
     * @return GroupOffset
     */
    public static GroupOffset fromKafkaOffset(long kafkaOffset, String groupId) {
        final long segmentId = kafkaOffset / SEGMENT_SIZE;
        final long offset = kafkaOffset - SEGMENT_SIZE * segmentId;
        return new GroupOffset(segmentId, offset, groupId);
    }

    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            consumerGroupCache.forEach((k, v) -> IOUtils.closeQuietly(v));
        }
    }

    @Override
    public Set<String> groups() {
        return groups;
    }

    @Override
    public GroupOffset committedGroupOffset(String groupId) {
        try (AdminClient adminClient = AdminClient.create(properties)) {
            final ListConsumerGroupOffsetsResult result =
                    adminClient.listConsumerGroupOffsets(groupId);
            try {
                OffsetAndMetadata metadata =
                        result.partitionsToOffsetAndMetadata().get().get(topicPartition);
                if (metadata == null) {
                    final Long endOffset = endOffsets(groupId);
                    return endOffset == null
                            ? new GroupOffset(0, 0, groupId)
                            : fromKafkaOffset(endOffset, groupId);
                } else {
                    return fromKafkaOffset(metadata.offset(), groupId);
                }
            } catch (Exception e) {
                throw new TributaryRuntimeException(
                        "get offset from kafka fail, group = " + groupId, e);
            }
        }
    }

    @Override
    public void commit(GroupOffset groupOffset) {
        getKafkaConsumer(groupOffset.groupId())
                .commitSync(
                        Collections.singletonMap(
                                topicPartition,
                                new OffsetAndMetadata(getKafkaOffset(groupOffset))));
    }

    @Override
    public GroupOffset getMinGroupOffset() {
        throw new UnsupportedOperationException("not support get min group offset");
    }
}
