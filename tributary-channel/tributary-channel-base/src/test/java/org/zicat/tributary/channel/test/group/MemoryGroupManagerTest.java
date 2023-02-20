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

package org.zicat.tributary.channel.test.group;

import org.junit.Assert;
import org.junit.Test;
import org.zicat.tributary.channel.GroupOffset;
import org.zicat.tributary.channel.group.MemoryGroupManager;

import java.util.HashSet;
import java.util.Set;

/** OnePartitionMemoryGroupManagerTest. */
public class MemoryGroupManagerTest {

    @Test
    public void testCommit() {

        final Set<GroupOffset> groupOffsets = new HashSet<>();
        groupOffsets.add(new GroupOffset(2, 100, "g1"));
        groupOffsets.add(new GroupOffset(3, 25, "g2"));
        final MemoryGroupManager manager = new MemoryGroupManager(groupOffsets);
        manager.commit(new GroupOffset(1, 101, "g1"));
        manager.commit(new GroupOffset(3, 75, "g2"));
        try {
            manager.commit(new GroupOffset(100, 1, "g3"));
            Assert.fail("expect commit fail");
        } catch (RuntimeException e) {
            Assert.assertTrue(true);
        }
        Assert.assertEquals(new GroupOffset(2, 100, "g1"), manager.committedGroupOffset("g1"));
        Assert.assertEquals(new GroupOffset(3, 75, "g2"), manager.committedGroupOffset("g2"));
        Assert.assertEquals(new GroupOffset(2, 100, "g1"), manager.getMinGroupOffset());

        manager.commit(new GroupOffset(3, 25, "g1"));
        Assert.assertEquals(new GroupOffset(3, 25, "g1"), manager.getMinGroupOffset());

        manager.commit(new GroupOffset(3, 80, "g1"));
        Assert.assertEquals(new GroupOffset(3, 75, "g2"), manager.getMinGroupOffset());

        manager.commit(new GroupOffset(3, 90, "g2"));
        Assert.assertEquals(new GroupOffset(3, 80, "g1"), manager.getMinGroupOffset());

        Assert.assertEquals(2, manager.groups().size());
        manager.close();
    }
}
