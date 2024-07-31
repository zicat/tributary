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
import org.zicat.tributary.channel.Offset;
import org.zicat.tributary.channel.group.FileGroupManager;
import org.zicat.tributary.channel.group.MemoryGroupManager.GroupOffset;
import org.zicat.tributary.common.test.FileUtils;

import java.io.File;
import java.io.IOException;
import java.util.*;

import static org.zicat.tributary.channel.group.FileGroupManager.createFileName;
import static org.zicat.tributary.channel.group.GroupManager.uninitializedOffset;
import static org.zicat.tributary.common.IOUtils.deleteDir;
import static org.zicat.tributary.common.IOUtils.makeDir;

/** OnePartitionFileGroupManagerTest. */
public class FileGroupManagerTest {

    private static final File DIR = FileUtils.createTmpDir("file_group_manager_test");

    private static final String topic = "topic_1";

    @Test
    public void testFirstUsedGroupId() {

        final Map<String, Offset> cache = new HashMap<>();

        // test new group ids
        final Random random = new Random();
        final Set<String> groupIds = new HashSet<>(Arrays.asList("g1", "g2", "g3"));
        try (final FileGroupManager manager =
                new FileGroupManager(new File(DIR, createFileName(topic)), groupIds)) {

            GroupOffset minGroupOffset = manager.getMinGroupOffset();
            Assert.assertEquals(uninitializedOffset().offset(), minGroupOffset.offset());
            Assert.assertEquals(uninitializedOffset().segmentId(), minGroupOffset.segmentId());
            Assert.assertTrue(groupIds.contains(minGroupOffset.groupId()));

            for (String group : groupIds) {
                Offset groupOffset = manager.committedOffset(group);
                Assert.assertEquals(uninitializedOffset(), groupOffset);
                final Offset newOffset = new Offset(random.nextInt(10) + 1, random.nextInt(100));
                manager.commit(group, newOffset);
                Assert.assertEquals(newOffset, manager.committedOffset(group));
                manager.commit(group, new Offset(newOffset.segmentId() - 1, 0L));
                Assert.assertEquals(newOffset, manager.committedOffset(group));
                cache.put(group, manager.committedOffset(group));
            }
        }

        // test exist group id and new ids
        final Set<String> groupIds2 = new HashSet<>(Arrays.asList("g2", "g3", "g4"));
        try (FileGroupManager manager2 =
                new FileGroupManager(new File(DIR, createFileName(topic)), groupIds2)) {
            GroupOffset minGroupOffset = manager2.getMinGroupOffset();
            Assert.assertEquals(uninitializedOffset().offset(), minGroupOffset.offset());
            Assert.assertEquals(uninitializedOffset().segmentId(), minGroupOffset.segmentId());
            Assert.assertTrue(groupIds2.contains(minGroupOffset.groupId()));

            for (String group : groupIds2) {
                Offset offset = manager2.committedOffset(group);
                Assert.assertEquals(cache.getOrDefault(group, uninitializedOffset()), offset);
                if (cache.containsKey(group)) {
                    final Offset cacheOffset = cache.get(group);
                    final Offset nextSegmentHead = new Offset(cacheOffset.segmentId() + 1, 0L);
                    manager2.commit(group, nextSegmentHead);
                    Assert.assertEquals(nextSegmentHead, manager2.committedOffset(group));
                    manager2.commit(group, cacheOffset);
                    Assert.assertEquals(nextSegmentHead, manager2.committedOffset(group));
                } else {
                    final Offset newOffset =
                            new Offset(random.nextInt(10) + 1, random.nextInt(100));
                    manager2.commit(group, newOffset);
                    Assert.assertEquals(newOffset, manager2.committedOffset(group));
                    manager2.commit(group, new Offset(newOffset.segmentId() - 1, 0L));
                    Assert.assertEquals(newOffset, manager2.committedOffset(group));
                }
                cache.put(group, manager2.committedOffset(group));
            }
        }

        // test single exist groups
        final String singleGroup = "g2";
        final Set<String> groupIds3 = Collections.singleton(singleGroup);
        try (FileGroupManager manager3 =
                new FileGroupManager(new File(DIR, createFileName(topic)), groupIds3)) {
            Assert.assertEquals(cache.get(singleGroup), manager3.getMinGroupOffset());
            Assert.assertEquals(cache.get(singleGroup), manager3.committedOffset(singleGroup));
            Offset nextSegmentHead = new Offset(cache.get(singleGroup).segmentId() + 1, 0L);
            manager3.commit(singleGroup, nextSegmentHead);
            nextSegmentHead = new Offset(cache.get(singleGroup).segmentId() + 1, 0L);
            Assert.assertEquals(nextSegmentHead, manager3.getMinGroupOffset());
            nextSegmentHead = new Offset(cache.get(singleGroup).segmentId() + 1, 0L);
            Assert.assertEquals(nextSegmentHead, manager3.committedOffset(singleGroup));

            manager3.commit(singleGroup, new Offset(cache.get(singleGroup).segmentId() - 1, 0L));
            nextSegmentHead = new Offset(cache.get(singleGroup).segmentId() + 1, 0L);
            Assert.assertEquals(nextSegmentHead, manager3.getMinGroupOffset());

            nextSegmentHead = new Offset(cache.get(singleGroup).segmentId() + 1, 0L);
            Assert.assertEquals(nextSegmentHead, manager3.committedOffset(singleGroup));
        }

        // test new topics
        try (FileGroupManager manager4 =
                new FileGroupManager(new File(DIR, createFileName(topic + "_new")), groupIds)) {
            GroupOffset minGroupOffset = manager4.getMinGroupOffset();
            Assert.assertEquals(uninitializedOffset().offset(), minGroupOffset.offset());
            Assert.assertEquals(uninitializedOffset().segmentId(), minGroupOffset.segmentId());
            Assert.assertTrue(groupIds.contains(minGroupOffset.groupId()));

            for (String group : groupIds) {
                Offset offset = manager4.committedOffset(group);
                Assert.assertEquals(-1, offset.segmentId());
                final Offset newOffset = new Offset(random.nextInt(10) + 1, random.nextInt(100));
                manager4.commit(group, newOffset);
                Assert.assertEquals(newOffset, manager4.committedOffset(group));
                manager4.commit(group, new Offset(newOffset.segmentId() - 1, 0L));
                Assert.assertEquals(newOffset, manager4.committedOffset(group));
                cache.put(group, manager4.committedOffset(group));
            }
        }
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
