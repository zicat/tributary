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

package org.zicat.tributary.queue.test.file;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.zicat.tributary.queue.RecordsOffset;
import org.zicat.tributary.queue.file.FileGroupManager;
import org.zicat.tributary.queue.test.utils.FileUtils;
import org.zicat.tributary.queue.utils.IOUtils;

import java.io.File;
import java.io.IOException;
import java.util.*;

import static org.zicat.tributary.queue.file.FileGroupManager.createNewGroupRecordsOffset;
import static org.zicat.tributary.queue.utils.IOUtils.deleteDir;
import static org.zicat.tributary.queue.utils.IOUtils.makeDir;

/** FileGroupManagerTest. */
public class FileGroupManagerTest {

    private static final File DIR = FileUtils.createTmpDir("file_group_manager_test");

    private static final String topic = "topic_1";

    @Test
    public void testFirstUsedGroupId() throws IOException {

        // test new group ids
        final Random random = new Random();
        final List<String> groupIds = Arrays.asList("g1", "g2", "g3");
        final FileGroupManager manager = new FileGroupManager(DIR, topic, groupIds);
        final Map<String, RecordsOffset> cache = new HashMap<>();
        Assert.assertEquals(createNewGroupRecordsOffset(), manager.getMinRecordsOffset());

        for (String group : groupIds) {
            RecordsOffset recordsOffset = manager.getRecordsOffset(group);
            Assert.assertEquals(createNewGroupRecordsOffset(), recordsOffset);

            final RecordsOffset newRecordsOffset =
                    new RecordsOffset(random.nextInt(10) + 1, random.nextInt(100));
            manager.commit(group, newRecordsOffset);
            Assert.assertEquals(newRecordsOffset, manager.getRecordsOffset(group));
            manager.commit(
                    group, newRecordsOffset.skip2TargetHead(newRecordsOffset.segmentId() - 1));
            Assert.assertEquals(newRecordsOffset, manager.getRecordsOffset(group));
            cache.put(group, manager.getRecordsOffset(group));
        }
        IOUtils.closeQuietly(manager);
        IOUtils.closeQuietly(manager);

        // test exist group id and new ids
        final List<String> groupIds2 = Arrays.asList("g2", "g3", "g4");
        final FileGroupManager manager2 = new FileGroupManager(DIR, topic, groupIds2);
        Assert.assertEquals(createNewGroupRecordsOffset(), manager2.getMinRecordsOffset());

        for (String group : groupIds2) {
            RecordsOffset recordsOffset = manager2.getRecordsOffset(group);
            if (cache.containsKey(group)) {
                Assert.assertEquals(cache.get(group), recordsOffset);
            } else {
                Assert.assertEquals(createNewGroupRecordsOffset(), recordsOffset);
            }
            if (cache.containsKey(group)) {
                final RecordsOffset cacheRecordsOffset = cache.get(group);
                manager2.commit(group, cacheRecordsOffset.skipNextSegmentHead());
                Assert.assertEquals(
                        cacheRecordsOffset.skipNextSegmentHead(), manager2.getRecordsOffset(group));
                manager2.commit(group, cacheRecordsOffset);
                Assert.assertEquals(
                        cacheRecordsOffset.skipNextSegmentHead(), manager2.getRecordsOffset(group));
            } else {
                final RecordsOffset newRecordsOffset =
                        new RecordsOffset(random.nextInt(10) + 1, random.nextInt(100));
                manager2.commit(group, newRecordsOffset);
                Assert.assertEquals(newRecordsOffset, manager2.getRecordsOffset(group));
                manager2.commit(
                        group, newRecordsOffset.skip2TargetHead(newRecordsOffset.segmentId() - 1));
                Assert.assertEquals(newRecordsOffset, manager2.getRecordsOffset(group));
            }
            cache.put(group, manager2.getRecordsOffset(group));
        }

        IOUtils.closeQuietly(manager2);

        // test single exist groups
        final String singleGroup = "g2";
        final List<String> groupIds3 = Collections.singletonList(singleGroup);
        final FileGroupManager manager3 = new FileGroupManager(DIR, topic, groupIds3);
        Assert.assertEquals(cache.get(singleGroup), manager3.getMinRecordsOffset());
        Assert.assertEquals(cache.get(singleGroup), manager3.getRecordsOffset(singleGroup));
        manager3.commit(singleGroup, cache.get(singleGroup).skipNextSegmentHead());
        Assert.assertEquals(
                cache.get(singleGroup).skipNextSegmentHead(), manager3.getMinRecordsOffset());
        Assert.assertEquals(
                cache.get(singleGroup).skipNextSegmentHead(),
                manager3.getRecordsOffset(singleGroup));

        manager3.commit(
                singleGroup,
                cache.get(singleGroup).skip2TargetHead(cache.get(singleGroup).segmentId() - 1));
        Assert.assertEquals(
                cache.get(singleGroup).skipNextSegmentHead(), manager3.getMinRecordsOffset());
        Assert.assertEquals(
                cache.get(singleGroup).skipNextSegmentHead(),
                manager3.getRecordsOffset(singleGroup));

        IOUtils.closeQuietly(manager3);

        // test new topics
        final FileGroupManager manager4 = new FileGroupManager(DIR, topic + "_new", groupIds);
        Assert.assertEquals(createNewGroupRecordsOffset(), manager4.getMinRecordsOffset());
        for (String group : groupIds) {
            RecordsOffset recordsOffset = manager4.getRecordsOffset(group);
            Assert.assertEquals(createNewGroupRecordsOffset(), recordsOffset);
            final RecordsOffset newRecordsOffset =
                    new RecordsOffset(random.nextInt(10) + 1, random.nextInt(100));
            manager4.commit(group, newRecordsOffset);
            Assert.assertEquals(newRecordsOffset, manager4.getRecordsOffset(group));
            manager4.commit(
                    group, newRecordsOffset.skip2TargetHead(newRecordsOffset.segmentId() - 1));
            Assert.assertEquals(newRecordsOffset, manager4.getRecordsOffset(group));
            cache.put(group, manager4.getRecordsOffset(group));
        }
        IOUtils.closeQuietly(manager4);
    }

    @Test
    public void testFileNameGroupMapping() {
        final String groupId4 = "g444";
        final String topic = "t1";
        final FileGroupManager manager =
                new FileGroupManager(DIR, topic, Collections.singletonList(groupId4));
        final String fileName = manager.createFileNameByGroupId("g444");
        Assert.assertTrue(manager.isGroupIndexFile(fileName));
        Assert.assertEquals(groupId4, manager.groupIdByFileName(fileName));
        IOUtils.closeQuietly(manager);
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
