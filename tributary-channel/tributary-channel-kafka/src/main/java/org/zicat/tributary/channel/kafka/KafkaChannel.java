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

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.zicat.tributary.channel.Channel;
import org.zicat.tributary.channel.GroupOffset;
import org.zicat.tributary.channel.RecordsResultSet;
import org.zicat.tributary.common.IOUtils;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG;
import static org.zicat.tributary.channel.kafka.KafkaUtils.adjustTopicPartition;

/** KafkaChannel. */
public class KafkaChannel implements Channel {

    private final AtomicBoolean closed = new AtomicBoolean();
    private final String topic;
    private final int partitionCount;
    private final Set<String> groups;
    private final AtomicLong writeBytes = new AtomicLong();
    private final AtomicLong readBytes = new AtomicLong();

    private final KafkaProducer<byte[], byte[]> producer;
    private final Map<TopicPartition, PartitionKafkaConsumer> partitionConsumerManager;

    public KafkaChannel(String topic, int partitionCount, Set<String> groups, Properties properties)
            throws ExecutionException, InterruptedException {
        this.topic = topic;
        this.partitionCount = partitionCount;
        this.groups = groups;
        setSerializer(properties);
        adjustTopicPartition(properties, topic, partitionCount);
        this.producer = new KafkaProducer<>(properties);
        this.partitionConsumerManager = createConsumers(topic, partitionCount, groups, properties);
    }

    @Override
    public void append(int partition, byte[] record, int offset, int length) {
        final ProducerRecord<byte[], byte[]> producerRecord =
                new ProducerRecord<>(topic, partition, null, copyRecord(record, offset, length));
        producer.send(producerRecord);
        writeBytes.addAndGet(length);
    }

    /**
     * copy record if need.
     *
     * @param record record
     * @param offset offset
     * @param length length
     * @return byte array
     */
    private static byte[] copyRecord(byte[] record, int offset, int length) {
        if (record == null) {
            return null;
        }
        if (offset == 0 && record.length == length) {
            return record;
        }
        return Arrays.copyOfRange(record, offset, offset + length);
    }

    @Override
    public RecordsResultSet poll(int partition, GroupOffset groupOffset, long time, TimeUnit unit) {
        final TopicPartition topicPartition = new TopicPartition(topic, partition);
        final PartitionKafkaConsumer consumer = getConsumer(topicPartition);
        final RecordsResultSet recordsResultSet = consumer.poll(groupOffset, time, unit);
        readBytes.addAndGet(recordsResultSet.readBytes());
        return recordsResultSet;
    }

    /**
     * get consumer by topic partition.
     *
     * @param topicPartition topicPartition
     * @return PartitionKafkaConsumer
     */
    private PartitionKafkaConsumer getConsumer(TopicPartition topicPartition) {
        final PartitionKafkaConsumer consumer = partitionConsumerManager.get(topicPartition);
        if (consumer == null) {
            throw new NullPointerException(topicPartition + " not found consumer instance");
        }
        return consumer;
    }

    @Override
    public void flush() {
        producer.flush();
    }

    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            IOUtils.closeQuietly(producer);
            partitionConsumerManager.forEach((k, v) -> IOUtils.closeQuietly(v));
        }
    }

    @Override
    public String topic() {
        return topic;
    }

    @Override
    public int partition() {
        return partitionCount;
    }

    @Override
    public int activeSegment() {
        // use metrics monitor, kafka channel don't need
        return 0;
    }

    @Override
    public long lag(int partition, GroupOffset groupOffset) {
        final TopicPartition topicPartition = new TopicPartition(topic, partition);
        final Long endOffset = getConsumer(topicPartition).endOffsets(groupOffset.groupId());
        return endOffset == null ? 0L : endOffset - groupOffset.offset();
    }

    @Override
    public long writeBytes() {
        return writeBytes.get();
    }

    @Override
    public long readBytes() {
        return readBytes.get();
    }

    @Override
    public long bufferUsage() {
        return 0L;
    }

    @Override
    public Set<String> groups() {
        return groups;
    }

    @Override
    public GroupOffset committedGroupOffset(String groupId, int partition) {
        final PartitionKafkaConsumer consumer = getConsumer(new TopicPartition(topic, partition));
        return consumer.committedGroupOffset(groupId);
    }

    @Override
    public void commit(int partition, GroupOffset groupOffset) {
        final PartitionKafkaConsumer consumer = getConsumer(new TopicPartition(topic, partition));
        consumer.commit(groupOffset);
    }

    /**
     * set default serializer.
     *
     * @param properties properties
     */
    private static void setSerializer(Properties properties) {
        properties.put(KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        properties.put(VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        properties.put(KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        properties.put(VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
    }

    /**
     * init consumer.
     *
     * @param topic topic
     * @param partitionCount partitionCount
     * @param groups groups
     * @param properties properties
     * @return consumer maps
     */
    private static Map<TopicPartition, PartitionKafkaConsumer> createConsumers(
            String topic, int partitionCount, Set<String> groups, Properties properties) {
        final Map<TopicPartition, PartitionKafkaConsumer> consumerMap = new HashMap<>();
        for (int partition = 0; partition < partitionCount; partition++) {
            final TopicPartition topicPartition = new TopicPartition(topic, partition);
            final PartitionKafkaConsumer consumer =
                    new PartitionKafkaConsumer(topicPartition, groups, properties);
            consumerMap.put(topicPartition, consumer);
        }
        return Collections.unmodifiableMap(consumerMap);
    }
}
