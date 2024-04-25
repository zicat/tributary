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

import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;

import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

/** KafkaUtils. */
public class KafkaUtils {

    /**
     * adjust topic partition.
     *
     * <p>Note: if topic is already over partition count, nothing to do.
     *
     * @param adminProperties adminProperties
     * @param topic topic
     * @param partitionCount partitionCount
     * @throws ExecutionException ExecutionException
     * @throws InterruptedException InterruptedException
     */
    public static void adjustTopicPartition(
            Properties adminProperties, String topic, int partitionCount)
            throws ExecutionException, InterruptedException {
        try (AdminClient adminClient = AdminClient.create(adminProperties)) {
            final DescribeTopicsResult result =
                    adminClient.describeTopics(Collections.singleton(topic));
            try {
                final TopicDescription description = result.all().get().get(topic);
                if (description.partitions().size() < partitionCount) {
                    CreatePartitionsResult createPartitionsResult =
                            adminClient.createPartitions(
                                    Collections.singletonMap(
                                            topic, NewPartitions.increaseTo(partitionCount)));
                    createPartitionsResult.all().get();
                }
            } catch (Throwable e) {
                if (isUnknownTopic(e)) {
                    final NewTopic newTopic = new NewTopic(topic, partitionCount, (short) 1);
                    final CreateTopicsResult createTopicsResult =
                            adminClient.createTopics(Collections.singleton(newTopic));
                    createTopicsResult.all().get();
                    return;
                }
                throw e;
            }
        }
    }

    /**
     * check exception is UnknownTopicOrPartitionException.
     *
     * @param e e
     * @return true if contains UnknownTopicOrPartitionException
     */
    private static boolean isUnknownTopic(Throwable e) {
        if (e == null) {
            return false;
        }
        if (e instanceof UnknownTopicOrPartitionException) {
            return true;
        }
        return isUnknownTopic(e.getCause());
    }

    /**
     * get topic description.
     *
     * @param adminProperties adminProperties
     * @param topic topic
     * @return TopicDescription
     * @throws ExecutionException ExecutionException
     * @throws InterruptedException InterruptedException
     */
    public static TopicDescription getTopicDescription(Properties adminProperties, String topic)
            throws ExecutionException, InterruptedException {
        try (AdminClient adminClient = AdminClient.create(adminProperties)) {
            final DescribeTopicsResult result =
                    adminClient.describeTopics(Collections.singleton(topic));
            return result.all().get().get(topic);
        }
    }
}
