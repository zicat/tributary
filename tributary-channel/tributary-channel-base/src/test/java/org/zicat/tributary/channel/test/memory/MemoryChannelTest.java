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

package org.zicat.tributary.channel.test.memory;

import org.junit.Assert;
import org.junit.Test;
import org.zicat.tributary.channel.*;
import org.zicat.tributary.channel.memory.MemoryChannel;
import org.zicat.tributary.channel.memory.MemoryChannelFactory;
import org.zicat.tributary.common.DefaultReadableConfig;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.TimeUnit;

import static org.zicat.tributary.channel.ChannelConfigOption.*;
import static org.zicat.tributary.channel.group.MemoryGroupManager.createMemoryGroupManagerFactory;
import static org.zicat.tributary.channel.group.MemoryGroupManager.defaultGroupOffset;
import static org.zicat.tributary.channel.memory.MemoryChannelFactory.createMemoryChannel;
import static org.zicat.tributary.channel.test.ChannelBaseTest.testChannelCorrect;

/** OnePartitionMemoryChannelTest. */
public class MemoryChannelTest {

    @Test
    public void testBaseCorrect() throws Exception {
        final DefaultReadableConfig config = new DefaultReadableConfig();
        config.put(OPTION_PARTITION_COUNT, 4);
        config.put(OPTION_GROUPS, "g1,g2,g3");
        config.put(OPTION_COMPRESSION, "zstd");
        final ChannelFactory factory = new MemoryChannelFactory();
        try (Channel channel = factory.createChannel("t1", config)) {
            testChannelCorrect(channel);
        }
    }

    @Test
    public void test() throws IOException, InterruptedException {

        final Set<GroupOffset> groupOffsets = new HashSet<>();
        groupOffsets.add(defaultGroupOffset("g1"));
        final MemoryChannel channel =
                createMemoryChannel(
                        "t1",
                        createMemoryGroupManagerFactory(groupOffsets),
                        1024 * 4,
                        102400L,
                        CompressionType.NONE);
        GroupOffset groupOffset = channel.committedGroupOffset("g1");
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
        RecordsResultSet recordsResultSet;
        int offset = 0;
        while (offset < result.size()) {
            recordsResultSet = channel.poll(groupOffset, 10, TimeUnit.MILLISECONDS);
            if (recordsResultSet.isEmpty()) {
                break;
            }
            while (recordsResultSet.hasNext()) {
                Assert.assertArrayEquals(result.get(offset), recordsResultSet.next());
                offset++;
            }
            groupOffset = recordsResultSet.nexGroupOffset();
            channel.commit(groupOffset);
        }
        Assert.assertEquals(1, channel.activeSegment());
        channel.close();
    }
}
