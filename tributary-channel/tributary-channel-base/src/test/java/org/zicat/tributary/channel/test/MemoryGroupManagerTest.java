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
import org.zicat.tributary.channel.RecordsOffset;
import org.zicat.tributary.channel.group.MemoryGroupManager;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/** OnePartitionMemoryGroupManagerTest. */
public class MemoryGroupManagerTest {

    @Test
    public void testPersist() throws InterruptedException {
        final Set<RecordsOffset> groupOffsets = new HashSet<>();
        groupOffsets.add(new RecordsOffset(2, 100, "g1"));
        groupOffsets.add(new RecordsOffset(3, 25, "g2"));
        final MemoryGroupManagerMock manager =
                new MemoryGroupManagerMock(groupOffsets) {
                    @Override
                    public void schedule() {
                        schedule.scheduleWithFixedDelay(
                                this::persist, 10, 1000, TimeUnit.MILLISECONDS);
                    }
                };
        Thread.sleep(15);
        Assert.assertEquals(1, manager.persistCount.get());
        manager.close();
        Assert.assertEquals(2, manager.persistCount.get());
    }

    @Test
    public void testCommit() {

        final Set<RecordsOffset> groupOffsets = new HashSet<>();
        groupOffsets.add(new RecordsOffset(2, 100, "g1"));
        groupOffsets.add(new RecordsOffset(3, 25, "g2"));
        final MemoryGroupManager manager = new MemoryGroupManagerMock(groupOffsets);
        manager.commit(new RecordsOffset(1, 101, "g1"));
        manager.commit(new RecordsOffset(3, 75, "g2"));
        try {
            manager.commit(new RecordsOffset(100, 1, "g3"));
            Assert.fail("expect commit fail");
        } catch (RuntimeException e) {
            Assert.assertTrue(true);
        }
        Assert.assertEquals(new RecordsOffset(2, 100, "g1"), manager.getRecordsOffset("g1"));
        Assert.assertEquals(new RecordsOffset(3, 75, "g2"), manager.getRecordsOffset("g2"));
        Assert.assertEquals(new RecordsOffset(2, 100, "g1"), manager.getMinRecordsOffset());

        manager.commit(new RecordsOffset(3, 25, "g1"));
        Assert.assertEquals(new RecordsOffset(3, 25, "g1"), manager.getMinRecordsOffset());

        manager.commit(new RecordsOffset(3, 80, "g1"));
        Assert.assertEquals(new RecordsOffset(3, 75, "g2"), manager.getMinRecordsOffset());

        manager.commit(new RecordsOffset(3, 90, "g2"));
        Assert.assertEquals(new RecordsOffset(3, 80, "g1"), manager.getMinRecordsOffset());

        Assert.assertEquals(2, manager.groups().size());
        manager.close();
    }

    /** MemoryOnePartitionGroupManagerMock. */
    private static class MemoryGroupManagerMock extends MemoryGroupManager {

        private final AtomicInteger persistCount = new AtomicInteger(0);

        public MemoryGroupManagerMock(Set<RecordsOffset> groupOffsets) {
            super(groupOffsets, 30);
        }

        @Override
        public void persist() {
            persistCount.incrementAndGet();
        }
    }
}
