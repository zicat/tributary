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
import org.zicat.tributary.channel.Offset;
import static org.zicat.tributary.channel.Offset.UNINITIALIZED_OFFSET;
import org.zicat.tributary.channel.group.MemoryGroupManager;

import java.util.HashMap;
import java.util.Map;

/** OnePartitionMemoryGroupManagerTest. */
public class MemoryGroupManagerTest {

    @Test
    public void testMinOffset() {
        final Map<String, Offset> groupOffsets = new HashMap<>();
        groupOffsets.put("g1", UNINITIALIZED_OFFSET);
        groupOffsets.put("g2", UNINITIALIZED_OFFSET);
        final MemoryGroupManager manager = new MemoryGroupManager(groupOffsets);
        Assert.assertTrue(manager.getMinOffset().uninitialized());
        manager.commit("g1", new Offset(1, 1));
        Assert.assertTrue(manager.getMinOffset().uninitialized());
        manager.commit("g2", new Offset(2, 2));
        Assert.assertEquals(new Offset(1, 1), manager.getMinOffset());
        manager.close();
    }

    @Test
    public void testCommit() {

        final Map<String, Offset> groupOffsets = new HashMap<>();
        groupOffsets.put("g1", new Offset(2, 100));
        groupOffsets.put("g2", new Offset(3, 25));
        final MemoryGroupManager manager = new MemoryGroupManager(groupOffsets);
        manager.commit("g1", new Offset(1, 101));
        manager.commit("g2", new Offset(3, 75));
        try {
            manager.commit("g3", new Offset(100, 1));
            Assert.fail("expect commit fail");
        } catch (RuntimeException e) {
            Assert.assertTrue(true);
        }
        Assert.assertEquals(new Offset(2, 100), manager.committedOffset("g1"));
        Assert.assertEquals(new Offset(3, 75), manager.committedOffset("g2"));
        Assert.assertEquals(new Offset(2, 100), manager.getMinOffset());

        manager.commit("g1", new Offset(3, 25));
        Assert.assertEquals(new Offset(3, 25), manager.getMinOffset());

        manager.commit("g1", new Offset(3, 80));
        Assert.assertEquals(new Offset(3, 75), manager.getMinOffset());

        manager.commit("g2", new Offset(3, 90));
        Assert.assertEquals(new Offset(3, 80), manager.getMinOffset());
        // g1 -> Offset{segmentId=3, offset=80}
        // g2 -> Offset{segmentId=3, offset=90}

        manager.commit(new Offset(3, 85));
        // g1 -> Offset{segmentId=3, offset=85}
        // g2 -> Offset{segmentId=3, offset=90}
        Assert.assertEquals(new Offset(3, 85), manager.getMinOffset());
        Assert.assertEquals(new Offset(3, 85), manager.committedOffset("g1"));
        Assert.assertEquals(new Offset(3, 90), manager.committedOffset("g2"));

        Assert.assertEquals(2, manager.groups().size());
        manager.close();
    }
}
