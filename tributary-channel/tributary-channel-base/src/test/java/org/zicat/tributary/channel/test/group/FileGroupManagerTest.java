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

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.zicat.tributary.channel.GroupOffset;
import org.zicat.tributary.channel.group.FileGroupManager;
import org.zicat.tributary.common.IOUtils;
import org.zicat.tributary.common.test.FileUtils;

import java.io.File;
import java.io.IOException;
import java.util.*;

import static org.zicat.tributary.channel.group.FileGroupManager.createFileName;
import static org.zicat.tributary.channel.group.MemoryGroupManager.defaultGroupOffset;
import static org.zicat.tributary.common.IOUtils.deleteDir;
import static org.zicat.tributary.common.IOUtils.makeDir;

/** OnePartitionFileGroupManagerTest. */
public class FileGroupManagerTest {

    private static final File DIR = FileUtils.createTmpDir("file_group_manager_test");

    private static final String topic = "topic_1";

    @Test
    public void testFirstUsedGroupId() {

        // test new group ids
        final Random random = new Random();
        final Set<String> groupIds = new HashSet<>(Arrays.asList("g1", "g2", "g3"));
        final FileGroupManager manager =
                new FileGroupManager(new File(DIR, createFileName(topic)), groupIds);
        final Map<String, GroupOffset> cache = new HashMap<>();
        Assert.assertEquals(
                defaultGroupOffset("g1").offset(), manager.getMinGroupOffset().offset());
        Assert.assertEquals(
                defaultGroupOffset("g1").segmentId(), manager.getMinGroupOffset().segmentId());
        Assert.assertTrue(groupIds.contains(manager.getMinGroupOffset().groupId()));

        for (String group : groupIds) {
            GroupOffset groupOffset = manager.committedGroupOffset(group);
            Assert.assertEquals(defaultGroupOffset(group), groupOffset);
            final GroupOffset newGroupOffset =
                    new GroupOffset(random.nextInt(10) + 1, random.nextInt(100), group);
            manager.commit(newGroupOffset);
            Assert.assertEquals(newGroupOffset, manager.committedGroupOffset(group));
            manager.commit(newGroupOffset.skip2TargetHead(newGroupOffset.segmentId() - 1));
            Assert.assertEquals(newGroupOffset, manager.committedGroupOffset(group));
            cache.put(group, manager.committedGroupOffset(group));
        }
        IOUtils.closeQuietly(manager);

        // test exist group id and new ids
        final Set<String> groupIds2 = new HashSet<>(Arrays.asList("g2", "g3", "g4"));
        final FileGroupManager manager2 =
                new FileGroupManager(new File(DIR, createFileName(topic)), groupIds2);
        Assert.assertEquals(
                defaultGroupOffset("g2").offset(), manager2.getMinGroupOffset().offset());
        Assert.assertEquals(
                defaultGroupOffset("g2").segmentId(), manager2.getMinGroupOffset().segmentId());
        Assert.assertTrue(groupIds2.contains(manager2.getMinGroupOffset().groupId()));

        for (String group : groupIds2) {
            GroupOffset groupOffset = manager2.committedGroupOffset(group);
            Assert.assertEquals(cache.getOrDefault(group, defaultGroupOffset(group)), groupOffset);
            if (cache.containsKey(group)) {
                final GroupOffset cacheGroupOffset = cache.get(group);
                manager2.commit(cacheGroupOffset.skipNextSegmentHead());
                Assert.assertEquals(
                        cacheGroupOffset.skipNextSegmentHead(),
                        manager2.committedGroupOffset(group));
                manager2.commit(cacheGroupOffset);
                Assert.assertEquals(
                        cacheGroupOffset.skipNextSegmentHead(),
                        manager2.committedGroupOffset(group));
            } else {
                final GroupOffset newGroupOffset =
                        new GroupOffset(random.nextInt(10) + 1, random.nextInt(100), group);
                manager2.commit(newGroupOffset);
                Assert.assertEquals(newGroupOffset, manager2.committedGroupOffset(group));
                manager2.commit(newGroupOffset.skip2TargetHead(newGroupOffset.segmentId() - 1));
                Assert.assertEquals(newGroupOffset, manager2.committedGroupOffset(group));
            }
            cache.put(group, manager2.committedGroupOffset(group));
        }

        IOUtils.closeQuietly(manager2);

        // test single exist groups
        final String singleGroup = "g2";
        final Set<String> groupIds3 = Collections.singleton(singleGroup);
        final FileGroupManager manager3 =
                new FileGroupManager(new File(DIR, createFileName(topic)), groupIds3);
        Assert.assertEquals(cache.get(singleGroup), manager3.getMinGroupOffset());
        Assert.assertEquals(cache.get(singleGroup), manager3.committedGroupOffset(singleGroup));
        manager3.commit(cache.get(singleGroup).skipNextSegmentHead());
        Assert.assertEquals(
                cache.get(singleGroup).skipNextSegmentHead(), manager3.getMinGroupOffset());
        Assert.assertEquals(
                cache.get(singleGroup).skipNextSegmentHead(),
                manager3.committedGroupOffset(singleGroup));

        manager3.commit(
                cache.get(singleGroup).skip2TargetHead(cache.get(singleGroup).segmentId() - 1));
        Assert.assertEquals(
                cache.get(singleGroup).skipNextSegmentHead(), manager3.getMinGroupOffset());
        Assert.assertEquals(
                cache.get(singleGroup).skipNextSegmentHead(),
                manager3.committedGroupOffset(singleGroup));

        IOUtils.closeQuietly(manager3);

        // test new topics
        final FileGroupManager manager4 =
                new FileGroupManager(new File(DIR, createFileName(topic + "_new")), groupIds);
        Assert.assertEquals(
                defaultGroupOffset("g2").offset(), manager4.getMinGroupOffset().offset());
        Assert.assertEquals(
                defaultGroupOffset("g2").segmentId(), manager4.getMinGroupOffset().segmentId());
        Assert.assertTrue(groupIds.contains(manager4.getMinGroupOffset().groupId()));

        for (String group : groupIds) {
            GroupOffset groupOffset = manager4.committedGroupOffset(group);
            Assert.assertEquals(-1, groupOffset.segmentId());
            final GroupOffset newGroupOffset =
                    new GroupOffset(random.nextInt(10) + 1, random.nextInt(100), group);
            manager4.commit(newGroupOffset);
            Assert.assertEquals(newGroupOffset, manager4.committedGroupOffset(group));
            manager4.commit(newGroupOffset.skip2TargetHead(newGroupOffset.segmentId() - 1));
            Assert.assertEquals(newGroupOffset, manager4.committedGroupOffset(group));
            cache.put(group, manager4.committedGroupOffset(group));
        }
        IOUtils.closeQuietly(manager4);
    }

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
}
