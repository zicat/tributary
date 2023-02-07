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
import org.zicat.tributary.channel.CompressionType;
import org.zicat.tributary.channel.OnePartitionMemoryGroupManager;
import org.zicat.tributary.channel.RecordsOffset;
import org.zicat.tributary.channel.RecordsResultSet;
import org.zicat.tributary.channel.memory.OnePartitionMemoryChannel;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.TimeUnit;

/** OnePartitionMemoryChannelTest. */
public class OnePartitionMemoryChannelTest {

    @Test
    public void test() throws IOException, InterruptedException {
        final String topic = "t1";
        final Map<String, RecordsOffset> groupOffsets = new HashMap<>();
        groupOffsets.put("g1", RecordsOffset.startRecordOffset());
        final OnePartitionMemoryGroupManager groupManager =
                OnePartitionMemoryGroupManager.createUnPersistGroupManager(topic, groupOffsets);
        final OnePartitionMemoryChannel channel =
                new OnePartitionMemoryChannel(
                        "t1", groupManager, 1024 * 4, 102400L, CompressionType.NONE, true);

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
        RecordsOffset recordsOffset = groupManager.getRecordsOffset("g1");
        RecordsResultSet recordsResultSet;
        int offset = 0;
        while (offset < result.size()) {
            recordsResultSet = channel.poll(recordsOffset, 10, TimeUnit.MILLISECONDS);
            if (recordsResultSet.isEmpty()) {
                break;
            }
            while (recordsResultSet.hasNext()) {
                Assert.assertArrayEquals(result.get(offset), recordsResultSet.next());
                offset++;
            }
            recordsOffset = recordsResultSet.nexRecordsOffset();
            channel.commit("g1", recordsOffset);
        }
        Assert.assertEquals(1, channel.activeSegment());
        channel.close();
    }
}
