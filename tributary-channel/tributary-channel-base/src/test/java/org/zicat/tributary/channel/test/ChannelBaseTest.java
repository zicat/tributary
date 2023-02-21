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

package org.zicat.tributary.channel.test;

import org.junit.Assert;
import org.zicat.tributary.channel.Channel;
import org.zicat.tributary.channel.ChannelFactory;
import org.zicat.tributary.channel.GroupOffset;
import org.zicat.tributary.channel.RecordsResultSet;
import org.zicat.tributary.common.ReadableConfig;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/** FileBaseTest. */
public class ChannelBaseTest {

    /**
     * test channel storage.
     *
     * @param factory factory
     * @param topic topic
     * @param config config
     * @throws Exception Exception
     */
    public static void testChannelStorage(
            ChannelFactory factory, String topic, ReadableConfig config) throws Exception {

        final Map<String, Map<Integer, GroupOffset>> startOffset = new HashMap<>();

        // consumer not commit
        try (Channel channel = factory.createChannel(topic, config)) {
            for (String group : channel.groups()) {
                for (int i = 0; i < channel.partition(); i++) {
                    final GroupOffset groupOffset = channel.committedGroupOffset(group, i);
                    startOffset.computeIfAbsent(group, g -> new HashMap<>()).put(i, groupOffset);
                    channel.commit(i, groupOffset);
                }
            }
            for (int i = 0; i < channel.partition(); i++) {
                channel.append(i, ("partition-" + i + "-value-0").getBytes());
                channel.append(i, ("partition-" + i + "-value-1").getBytes());
                channel.flush();
            }
            for (String group : channel.groups()) {
                for (int i = 0; i < channel.partition(); i++) {
                    RecordsResultSet recordsResultSet =
                            channel.poll(
                                    i, startOffset.get(group).get(i), 1000, TimeUnit.MILLISECONDS);
                    Assert.assertTrue(recordsResultSet.hasNext());
                    Assert.assertEquals(
                            "partition-" + i + "-value-0",
                            new String(recordsResultSet.next(), StandardCharsets.UTF_8));
                    Assert.assertEquals(
                            "partition-" + i + "-value-1",
                            new String(recordsResultSet.next(), StandardCharsets.UTF_8));
                }
            }
        }
        // consume again with commit
        try (Channel channel = factory.createChannel(topic, config)) {
            for (String group : channel.groups()) {
                for (int i = 0; i < channel.partition(); i++) {
                    final GroupOffset groupOffset = channel.committedGroupOffset(group, i);
                    startOffset.computeIfAbsent(group, g -> new HashMap<>()).put(i, groupOffset);
                    channel.commit(i, groupOffset);
                }
            }
            for (String group : channel.groups()) {
                for (int i = 0; i < channel.partition(); i++) {
                    RecordsResultSet recordsResultSet =
                            channel.poll(
                                    i, startOffset.get(group).get(i), 1000, TimeUnit.MILLISECONDS);
                    Assert.assertTrue(recordsResultSet.hasNext());
                    Assert.assertEquals(
                            "partition-" + i + "-value-0",
                            new String(recordsResultSet.next(), StandardCharsets.UTF_8));
                    Assert.assertEquals(
                            "partition-" + i + "-value-1",
                            new String(recordsResultSet.next(), StandardCharsets.UTF_8));
                    channel.commit(i, recordsResultSet.nexGroupOffset());
                }
            }
        }

        // because committed, no data can consumer
        try (Channel channel = factory.createChannel(topic, config)) {
            for (String group : channel.groups()) {
                for (int i = 0; i < channel.partition(); i++) {
                    final GroupOffset groupOffset = channel.committedGroupOffset(group, i);
                    startOffset.computeIfAbsent(group, g -> new HashMap<>()).put(i, groupOffset);
                    channel.commit(i, groupOffset);
                }
            }
            for (String group : channel.groups()) {
                for (int i = 0; i < channel.partition(); i++) {
                    RecordsResultSet recordsResultSet =
                            channel.poll(
                                    i, startOffset.get(group).get(i), 1000, TimeUnit.MILLISECONDS);
                    Assert.assertFalse(recordsResultSet.hasNext());
                }
            }
        }
    }

    /**
     * test correct.
     *
     * @param channel channel
     * @throws IOException IOException
     * @throws InterruptedException InterruptedException
     */
    public static void testChannelCorrect(Channel channel)
            throws IOException, InterruptedException {

        final Map<String, Map<Integer, GroupOffset>> startOffset = new HashMap<>();
        for (String group : channel.groups()) {
            for (int i = 0; i < channel.partition(); i++) {
                final GroupOffset groupOffset = channel.committedGroupOffset(group, i);
                startOffset.computeIfAbsent(group, g -> new HashMap<>()).put(i, groupOffset);
                channel.commit(i, groupOffset);
            }
        }
        for (int i = 0; i < channel.partition(); i++) {
            channel.append(i, ("partition-" + i + "-value-0").getBytes());
            channel.append(i, ("partition-" + i + "-value-1").getBytes());
            channel.flush();
        }

        final Map<String, Map<Integer, GroupOffset>> nextOffset = new HashMap<>();
        for (String group : channel.groups()) {
            for (int i = 0; i < channel.partition(); i++) {
                RecordsResultSet recordsResultSet =
                        channel.poll(i, startOffset.get(group).get(i), 1000, TimeUnit.MILLISECONDS);
                Assert.assertTrue(recordsResultSet.hasNext());
                Assert.assertEquals(
                        "partition-" + i + "-value-0",
                        new String(recordsResultSet.next(), StandardCharsets.UTF_8));

                if (!recordsResultSet.hasNext()) {
                    recordsResultSet =
                            channel.poll(
                                    i,
                                    recordsResultSet.nexGroupOffset(),
                                    1000,
                                    TimeUnit.MILLISECONDS);
                }
                Assert.assertEquals(
                        "partition-" + i + "-value-1",
                        new String(recordsResultSet.next(), StandardCharsets.UTF_8));
                nextOffset
                        .computeIfAbsent(group, g -> new HashMap<>())
                        .put(i, recordsResultSet.nexGroupOffset());
            }
        }

        for (int i = 0; i < channel.partition(); i++) {
            channel.append(i, ("partition-" + i + "-value-2").getBytes());
            channel.append(i, ("partition-" + i + "-value-3").getBytes());
            channel.flush();
        }

        for (String group : channel.groups()) {
            for (int i = 0; i < channel.partition(); i++) {
                RecordsResultSet recordsResultSet =
                        channel.poll(i, nextOffset.get(group).get(i), 1000, TimeUnit.MILLISECONDS);
                Assert.assertTrue(recordsResultSet.hasNext());
                Assert.assertEquals(
                        "partition-" + i + "-value-2",
                        new String(recordsResultSet.next(), StandardCharsets.UTF_8));
                Assert.assertEquals(
                        "partition-" + i + "-value-3",
                        new String(recordsResultSet.next(), StandardCharsets.UTF_8));
            }
        }

        // test rollback
        for (String group : channel.groups()) {
            for (int i = 0; i < channel.partition(); i++) {
                RecordsResultSet recordsResultSet =
                        channel.poll(i, startOffset.get(group).get(i), 1000, TimeUnit.MILLISECONDS);
                Assert.assertTrue(recordsResultSet.hasNext());
                Assert.assertEquals(
                        "partition-" + i + "-value-0",
                        new String(recordsResultSet.next(), StandardCharsets.UTF_8));
                if (!recordsResultSet.hasNext()) {
                    recordsResultSet =
                            channel.poll(
                                    i,
                                    recordsResultSet.nexGroupOffset(),
                                    1000,
                                    TimeUnit.MILLISECONDS);
                }
                Assert.assertEquals(
                        "partition-" + i + "-value-1",
                        new String(recordsResultSet.next(), StandardCharsets.UTF_8));
                nextOffset
                        .computeIfAbsent(group, g -> new HashMap<>())
                        .put(i, recordsResultSet.nexGroupOffset());
            }
        }

        // test commit
        for (String group : channel.groups()) {
            for (int i = 0; i < channel.partition(); i++) {
                Assert.assertEquals(
                        startOffset.get(group).get(i), channel.committedGroupOffset(group, i));
                channel.commit(i, nextOffset.get(group).get(i));
                Assert.assertEquals(
                        nextOffset.get(group).get(i), channel.committedGroupOffset(group, i));
            }
        }
    }
}
