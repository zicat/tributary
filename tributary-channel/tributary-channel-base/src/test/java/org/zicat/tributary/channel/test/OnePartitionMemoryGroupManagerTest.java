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
import org.junit.Test;
import org.zicat.tributary.channel.OnePartitionMemoryGroupManager;
import org.zicat.tributary.channel.RecordsOffset;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/** OnePartitionMemoryGroupManagerTest. */
public class OnePartitionMemoryGroupManagerTest {

    @Test
    public void testPersist() throws InterruptedException {
        final Map<String, RecordsOffset> groupOffsets = new HashMap<>();
        groupOffsets.put("g1", new RecordsOffset(2, 100));
        groupOffsets.put("g2", new RecordsOffset(3, 25));
        final OnePartitionMemoryGroupManagerMock manager =
                new OnePartitionMemoryGroupManagerMock("t1", groupOffsets) {
                    @Override
                    public void schedule() {
                        schedule.scheduleWithFixedDelay(
                                this::persist, 10, 10, TimeUnit.MILLISECONDS);
                    }
                };
        Thread.sleep(15);
        Assert.assertEquals(1, manager.persistCount.get());
        manager.close();
        Assert.assertEquals(2, manager.persistCount.get());
    }

    @Test
    public void testCommit() {

        final Map<String, RecordsOffset> groupOffsets = new HashMap<>();
        groupOffsets.put("g1", new RecordsOffset(2, 100));
        groupOffsets.put("g2", new RecordsOffset(3, 25));
        final OnePartitionMemoryGroupManager manager =
                new OnePartitionMemoryGroupManagerMock("t1", groupOffsets);
        manager.commit("g1", new RecordsOffset(1, 101));
        manager.commit("g2", new RecordsOffset(3, 75));
        try {
            manager.commit("g3", new RecordsOffset(100, 1));
            Assert.fail("expect commit fail");
        } catch (RuntimeException e) {
            Assert.assertTrue(true);
        }
        Assert.assertEquals(new RecordsOffset(2, 100), manager.getRecordsOffset("g1"));
        Assert.assertEquals(new RecordsOffset(3, 75), manager.getRecordsOffset("g2"));
        Assert.assertEquals(new RecordsOffset(2, 100), manager.getMinRecordsOffset());

        manager.commit("g1", new RecordsOffset(3, 25));
        Assert.assertEquals(new RecordsOffset(3, 25), manager.getMinRecordsOffset());

        manager.commit("g1", new RecordsOffset(3, 80));
        Assert.assertEquals(new RecordsOffset(3, 75), manager.getMinRecordsOffset());

        manager.commit("g2", new RecordsOffset(3, 90));
        Assert.assertEquals(new RecordsOffset(3, 80), manager.getMinRecordsOffset());

        Assert.assertEquals("t1", manager.topic());
        Assert.assertEquals(2, manager.groups().size());
        manager.close();
    }

    /** MemoryOnePartitionGroupManagerMock. */
    private static class OnePartitionMemoryGroupManagerMock extends OnePartitionMemoryGroupManager {

        private final AtomicInteger persistCount = new AtomicInteger(0);

        public OnePartitionMemoryGroupManagerMock(
                String topic, Map<String, RecordsOffset> groupOffsets) {
            super(topic, groupOffsets, DEFAULT_GROUP_PERSIST_PERIOD_SECOND);
        }

        @Override
        public void persist() {
            persistCount.incrementAndGet();
        }
    }
}
