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

package org.zicat.tributary.channle.file.test;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.zicat.tributary.channel.RecordsOffset;
import org.zicat.tributary.channel.file.FileGroupManager;
import org.zicat.tributary.common.IOUtils;
import org.zicat.tributary.common.test.FileUtils;

import java.io.File;
import java.io.IOException;
import java.util.*;

import static org.zicat.tributary.channel.file.FileGroupManager.createFileName;
import static org.zicat.tributary.channel.memory.MemoryGroupManager.defaultRecordsOffset;
import static org.zicat.tributary.common.IOUtils.deleteDir;
import static org.zicat.tributary.common.IOUtils.makeDir;

/** OnePartitionFileGroupManagerTest. */
public class OnePartitionFileGroupManagerTest {

    private static final File DIR = FileUtils.createTmpDir("file_group_manager_test");

    private static final String topic = "topic_1";

    @Test
    public void testFirstUsedGroupId() {

        // test new group ids
        final Random random = new Random();
        final Set<String> groupIds = new HashSet<>(Arrays.asList("g1", "g2", "g3"));
        final FileGroupManager manager =
                new FileGroupManager(new File(DIR, createFileName(topic)), groupIds);
        final Map<String, RecordsOffset> cache = new HashMap<>();
        Assert.assertEquals(
                defaultRecordsOffset("g1").offset(), manager.getMinRecordsOffset().offset());
        Assert.assertEquals(
                defaultRecordsOffset("g1").segmentId(), manager.getMinRecordsOffset().segmentId());
        Assert.assertTrue(groupIds.contains(manager.getMinRecordsOffset().groupId()));

        for (String group : groupIds) {
            RecordsOffset recordsOffset = manager.getRecordsOffset(group);
            Assert.assertEquals(defaultRecordsOffset(group), recordsOffset);
            final RecordsOffset newRecordsOffset =
                    new RecordsOffset(random.nextInt(10) + 1, random.nextInt(100), group);
            manager.commit(newRecordsOffset);
            Assert.assertEquals(newRecordsOffset, manager.getRecordsOffset(group));
            manager.commit(newRecordsOffset.skip2TargetHead(newRecordsOffset.segmentId() - 1));
            Assert.assertEquals(newRecordsOffset, manager.getRecordsOffset(group));
            cache.put(group, manager.getRecordsOffset(group));
        }
        IOUtils.closeQuietly(manager);

        // test exist group id and new ids
        final Set<String> groupIds2 = new HashSet<>(Arrays.asList("g2", "g3", "g4"));
        final FileGroupManager manager2 =
                new FileGroupManager(new File(DIR, createFileName(topic)), groupIds2);
        Assert.assertEquals(
                defaultRecordsOffset("g2").offset(), manager2.getMinRecordsOffset().offset());
        Assert.assertEquals(
                defaultRecordsOffset("g2").segmentId(), manager2.getMinRecordsOffset().segmentId());
        Assert.assertTrue(groupIds2.contains(manager2.getMinRecordsOffset().groupId()));

        for (String group : groupIds2) {
            RecordsOffset recordsOffset = manager2.getRecordsOffset(group);
            Assert.assertEquals(
                    cache.getOrDefault(group, defaultRecordsOffset(group)), recordsOffset);
            if (cache.containsKey(group)) {
                final RecordsOffset cacheRecordsOffset = cache.get(group);
                manager2.commit(cacheRecordsOffset.skipNextSegmentHead());
                Assert.assertEquals(
                        cacheRecordsOffset.skipNextSegmentHead(), manager2.getRecordsOffset(group));
                manager2.commit(cacheRecordsOffset);
                Assert.assertEquals(
                        cacheRecordsOffset.skipNextSegmentHead(), manager2.getRecordsOffset(group));
            } else {
                final RecordsOffset newRecordsOffset =
                        new RecordsOffset(random.nextInt(10) + 1, random.nextInt(100), group);
                manager2.commit(newRecordsOffset);
                Assert.assertEquals(newRecordsOffset, manager2.getRecordsOffset(group));
                manager2.commit(newRecordsOffset.skip2TargetHead(newRecordsOffset.segmentId() - 1));
                Assert.assertEquals(newRecordsOffset, manager2.getRecordsOffset(group));
            }
            cache.put(group, manager2.getRecordsOffset(group));
        }

        IOUtils.closeQuietly(manager2);

        // test single exist groups
        final String singleGroup = "g2";
        final Set<String> groupIds3 = Collections.singleton(singleGroup);
        final FileGroupManager manager3 =
                new FileGroupManager(new File(DIR, createFileName(topic)), groupIds3);
        Assert.assertEquals(cache.get(singleGroup), manager3.getMinRecordsOffset());
        Assert.assertEquals(cache.get(singleGroup), manager3.getRecordsOffset(singleGroup));
        manager3.commit(cache.get(singleGroup).skipNextSegmentHead());
        Assert.assertEquals(
                cache.get(singleGroup).skipNextSegmentHead(), manager3.getMinRecordsOffset());
        Assert.assertEquals(
                cache.get(singleGroup).skipNextSegmentHead(),
                manager3.getRecordsOffset(singleGroup));

        manager3.commit(
                cache.get(singleGroup).skip2TargetHead(cache.get(singleGroup).segmentId() - 1));
        Assert.assertEquals(
                cache.get(singleGroup).skipNextSegmentHead(), manager3.getMinRecordsOffset());
        Assert.assertEquals(
                cache.get(singleGroup).skipNextSegmentHead(),
                manager3.getRecordsOffset(singleGroup));

        IOUtils.closeQuietly(manager3);

        // test new topics
        final FileGroupManager manager4 =
                new FileGroupManager(new File(DIR, createFileName(topic + "_new")), groupIds);
        Assert.assertEquals(
                defaultRecordsOffset("g2").offset(), manager4.getMinRecordsOffset().offset());
        Assert.assertEquals(
                defaultRecordsOffset("g2").segmentId(), manager4.getMinRecordsOffset().segmentId());
        Assert.assertTrue(groupIds.contains(manager4.getMinRecordsOffset().groupId()));

        for (String group : groupIds) {
            RecordsOffset recordsOffset = manager4.getRecordsOffset(group);
            Assert.assertEquals(-1, recordsOffset.segmentId());
            final RecordsOffset newRecordsOffset =
                    new RecordsOffset(random.nextInt(10) + 1, random.nextInt(100), group);
            manager4.commit(newRecordsOffset);
            Assert.assertEquals(newRecordsOffset, manager4.getRecordsOffset(group));
            manager4.commit(newRecordsOffset.skip2TargetHead(newRecordsOffset.segmentId() - 1));
            Assert.assertEquals(newRecordsOffset, manager4.getRecordsOffset(group));
            cache.put(group, manager4.getRecordsOffset(group));
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
