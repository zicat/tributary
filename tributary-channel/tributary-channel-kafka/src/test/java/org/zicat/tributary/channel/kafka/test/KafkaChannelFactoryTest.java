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

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.zicat.tributary.channel.Channel;
import org.zicat.tributary.channel.ChannelFactory;
import org.zicat.tributary.channel.kafka.KafkaChannelFactory;
import org.zicat.tributary.common.DefaultReadableConfig;
import org.zicat.tributary.common.test.FileUtils;

import java.io.File;
import java.io.IOException;

import static org.zicat.tributary.channel.ChannelConfigOption.OPTION_GROUPS;
import static org.zicat.tributary.channel.ChannelFactory.findChannelFactory;
import static org.zicat.tributary.channel.kafka.KafkaChannelFactory.OPTIONS_KAFKA_TOPIC_META_DIR;
import static org.zicat.tributary.channel.kafka.KafkaChannelFactory.getTopic;
import static org.zicat.tributary.channel.kafka.test.EmbeddedKafkaHandler.startEmbeddedKafka;
import static org.zicat.tributary.channel.test.ChannelBaseTest.testChannelCorrect;
import static org.zicat.tributary.channel.test.ChannelBaseTest.testChannelStorage;
import static org.zicat.tributary.common.IOUtils.deleteDir;
import static org.zicat.tributary.common.IOUtils.makeDir;

/** KafkaChannelFactoryTest. */
public class KafkaChannelFactoryTest {

    private static final File DIR = FileUtils.createTmpDir("kafka_channel_factory_test");

    @BeforeClass
    public static void before() throws IOException {
        deleteDir(DIR);
        if (!makeDir(DIR)) {
            throw new IOException("create dir fail, " + DIR.getPath());
        }
    }

    @AfterClass
    public static void after() {
        deleteDir(DIR);
    }

    @Test
    public void testBaseStorage() throws Exception {
        startEmbeddedKafka(
                kafka -> {
                    final ChannelFactory factory = findChannelFactory(KafkaChannelFactory.TYPE);
                    final String topic = "kafka_channel_factory_test_topic2";
                    final DefaultReadableConfig config = new DefaultReadableConfig();
                    config.put("kafka.bootstrap.servers", kafka.getBrokerList());
                    config.put(OPTIONS_KAFKA_TOPIC_META_DIR, DIR.getPath());
                    config.put(OPTION_GROUPS, "g1, g2, g3");
                    testChannelStorage(factory, topic, config);
                });
    }

    @Test
    public void testCreateChannel() throws Exception {
        startEmbeddedKafka(
                kafka -> {
                    final ChannelFactory factory = findChannelFactory(KafkaChannelFactory.TYPE);
                    final String topic = "kafka_channel_factory_test_topic";
                    final DefaultReadableConfig config = new DefaultReadableConfig();
                    config.put("kafka.bootstrap.servers", kafka.getBrokerList());
                    config.put(OPTIONS_KAFKA_TOPIC_META_DIR, DIR.getPath());
                    config.put(OPTION_GROUPS, "g1, g2, g3");
                    try (Channel channel = factory.createChannel(topic, config)) {
                        testChannelCorrect(channel);
                    }
                });
    }

    @Test
    public void testGetTopic() throws IOException {
        final File topicMetaFile = new File(DIR, "my_test_file");
        final String topic1 = getTopic(topicMetaFile);

        final File topicMetaFile2 = new File(DIR, "my_test_file");
        final String topic2 = getTopic(topicMetaFile2);
        Assert.assertEquals(topic1, topic2);

        Assert.assertTrue(topicMetaFile.delete());
        final File topicMetaFile3 = new File(DIR, "my_test_file");
        final String topic3 = getTopic(topicMetaFile3);
        Assert.assertNotEquals(topic1, topic3);
    }
}
