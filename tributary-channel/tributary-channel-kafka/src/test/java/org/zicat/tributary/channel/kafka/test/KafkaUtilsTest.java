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

package org.zicat.tributary.channel.kafka.test;

import net.mguenther.kafka.junit.EmbeddedKafkaCluster;
import net.mguenther.kafka.junit.EmbeddedKafkaClusterConfig;
import net.mguenther.kafka.junit.TopicConfig;
import org.junit.Assert;
import org.junit.Test;
import org.zicat.tributary.channel.kafka.KafkaUtils;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

import static net.mguenther.kafka.junit.EmbeddedKafkaCluster.provisionWith;
import static org.zicat.tributary.channel.kafka.KafkaUtils.getTopicDescription;

/** KafkaUtilsTest. */
public class KafkaUtilsTest {

    @Test
    public void testAdjustTopicPartition() throws ExecutionException, InterruptedException {
        try (EmbeddedKafkaCluster kafka = provisionWith(EmbeddedKafkaClusterConfig.useDefaults())) {
            kafka.start();
            Properties properties = new Properties();
            properties.put("bootstrap.servers", kafka.getBrokerList());

            String topic = "t1";
            kafka.createTopic(new TopicConfig(topic, 2, 1, new Properties()));
            KafkaUtils.adjustTopicPartition(properties, topic, 10);
            Assert.assertEquals(10, getTopicDescription(properties, topic).partitions().size());

            topic = "t2";
            kafka.createTopic(new TopicConfig(topic, 12, 1, new Properties()));
            KafkaUtils.adjustTopicPartition(properties, topic, 11);
            Assert.assertEquals(12, getTopicDescription(properties, topic).partitions().size());

            topic = "t3";
            KafkaUtils.adjustTopicPartition(properties, topic, 20);
            Assert.assertEquals(20, getTopicDescription(properties, topic).partitions().size());
        }
    }
}
