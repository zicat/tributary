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

package org.zicat.tributary.channel.memory.test;

import org.junit.Assert;
import org.junit.Test;
import org.zicat.tributary.channel.*;
import org.zicat.tributary.channel.Segment.AppendResult;
import org.zicat.tributary.channel.memory.MemoryChannel;
import org.zicat.tributary.channel.memory.MemoryChannelFactory;
import org.zicat.tributary.channel.test.ChannelBaseTest.DataOffset;
import org.zicat.tributary.common.DefaultReadableConfig;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.zicat.tributary.channel.ChannelConfigOption.*;
import static org.zicat.tributary.channel.group.GroupManager.uninitializedOffset;
import static org.zicat.tributary.channel.group.MemoryGroupManager.createMemoryGroupManagerFactory;
import static org.zicat.tributary.channel.memory.MemoryChannelFactory.createMemoryChannel;
import static org.zicat.tributary.channel.test.ChannelBaseTest.readChannel;
import static org.zicat.tributary.channel.test.ChannelBaseTest.testChannelCorrect;

/** OnePartitionMemoryChannelTest. */
public class MemoryChannelTest {

    @Test
    public void testSyncAwait() throws IOException, InterruptedException {
        final Map<String, Offset> groupOffsets = new HashMap<>();
        final AbstractChannel.MemoryGroupManagerFactory factory =
                createMemoryGroupManagerFactory(groupOffsets);
        final AtomicBoolean finished = new AtomicBoolean();
        try (Channel channel =
                new MemoryChannel("t1", factory, 10240, 10240L, CompressionType.NONE, 10) {
                    @Override
                    public void append(ByteBuffer byteBuffer) throws IOException {
                        final AppendResult appendResult = innerAppend(byteBuffer);
                        if (!appendResult.appended()) {
                            throw new IOException("append fail");
                        }
                        try {
                            Assert.assertTrue(appendResult.await2Storage(10, TimeUnit.SECONDS));
                        } catch (InterruptedException e) {
                            throw new RuntimeException(e);
                        } finally {
                            finished.set(true);
                        }
                    }
                }) {
            final Thread t1 =
                    new Thread(
                            () -> {
                                try {
                                    channel.append(0, "aa".getBytes());
                                } catch (IOException | InterruptedException e) {
                                    throw new RuntimeException(e);
                                }
                            });
            t1.start();
            // try join 1s
            t1.join(1000);
            Assert.assertFalse(finished.get());
            channel.flush();
            t1.join();
            Assert.assertTrue(finished.get());
        }
    }

    @Test
    public void testBaseCorrect() throws Exception {
        final DefaultReadableConfig config = new DefaultReadableConfig();
        config.put(OPTION_PARTITION_COUNT, 4);
        config.put(OPTION_GROUPS, Arrays.asList("g1", "g2", "g3"));
        config.put(OPTION_COMPRESSION, CompressionType.ZSTD);
        final ChannelFactory factory = new MemoryChannelFactory();
        try (Channel channel = factory.createChannel("t1", config)) {
            testChannelCorrect(channel);
        }
    }

    @Test
    public void test() throws IOException, InterruptedException {

        final String groupId = "g1";
        final Map<String, Offset> groupOffsets = new HashMap<>();
        groupOffsets.put(groupId, uninitializedOffset());
        try (MemoryChannel channel =
                createMemoryChannel(
                        "t1",
                        createMemoryGroupManagerFactory(groupOffsets),
                        1024 * 4,
                        102400L,
                        CompressionType.NONE,
                        1)) {
            final Offset offset = channel.committedOffset(groupId);
            final Random random = new Random(1023312);
            final List<byte[]> result = new ArrayList<>();
            for (int i = 0; i < 200000; i++) {
                final int count = random.nextInt(60) + 60;
                final byte[] bs = new byte[count];
                for (int j = 0; j < count; j++) {
                    bs[j] = (byte) random.nextInt(256);
                }
                result.add(bs);
                channel.append(0, bs);
            }
            channel.flush();
            Assert.assertTrue(channel.activeSegment() > 1);
            final DataOffset dataOffset = readChannel(channel, 0, offset, result.size());
            Assert.assertEquals(result.size(), dataOffset.data.size());
            for (int i = 0; i < result.size(); i++) {
                Assert.assertArrayEquals(result.get(i), dataOffset.data.get(i));
            }
            channel.commit(groupId, dataOffset.offset);
            Assert.assertEquals(1, channel.activeSegment());
        }
    }
}
