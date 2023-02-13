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
import org.junit.BeforeClass;
import org.junit.Test;
import org.zicat.tributary.channel.Channel;
import org.zicat.tributary.channel.kafka.KafkaChannel;
import org.zicat.tributary.common.test.FileUtils;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

import static org.zicat.tributary.channel.kafka.test.EmbeddedKafkaHandler.startEmbeddedKafka;
import static org.zicat.tributary.channel.test.ChannelBaseTest.testChannelCorrect;
import static org.zicat.tributary.common.IOUtils.deleteDir;
import static org.zicat.tributary.common.IOUtils.makeDir;

/** KafkaChannelTest. */
public class KafkaChannelTest {

    private static final File DIR = FileUtils.createTmpDir("kafka_channel_test");

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
    public void testCorrection() throws Exception {
        startEmbeddedKafka(
                kafka -> {
                    final Set<String> groups = new HashSet<>(Arrays.asList("g1", "g2"));
                    final Properties properties = new Properties();
                    properties.put("bootstrap.servers", kafka.getBrokerList());
                    try (Channel channel =
                            new KafkaChannel("test_topic_t1", 2, groups, properties)) {
                        testChannelCorrect(channel);
                    }
                });
    }
}
