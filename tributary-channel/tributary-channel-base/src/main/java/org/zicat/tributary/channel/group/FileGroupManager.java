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

package org.zicat.tributary.channel.group;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zicat.tributary.channel.GroupOffset;
import org.zicat.tributary.common.*;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * FileGroupManager.
 *
 * <p>Store GroupOffset in local {@link FileGroupManager#groupIndexFile}.
 *
 * <p>File name example: {myTopic}_group_id.index
 *
 * <p>file content: 4 bytes(Int, GroupLength) + group body + 8 Bytes(Long, segmentId) + 8
 * Bytes(Long, segmentId).
 */
public class FileGroupManager extends MemoryGroupManager {

    public static final ConfigOption<Long> OPTION_GROUP_PERSIST_PERIOD_SECOND =
            ConfigOptions.key("groups.persist.period.second")
                    .longType()
                    .description("how long to persist group offset to storage, default 30")
                    .defaultValue(30L);

    private static final Logger LOG = LoggerFactory.getLogger(FileGroupManager.class);

    public static final String FILE_SUFFIX = "_group_id.index";
    public static final String TMP_SUFFIX = ".tmp";

    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final File groupIndexFile;
    protected ScheduledExecutorService schedule;

    public FileGroupManager(File groupIndexFile, Set<String> groupIds) {
        this(groupIndexFile, groupIds, OPTION_GROUP_PERSIST_PERIOD_SECOND.defaultValue());
    }

    public FileGroupManager(File groupIndexFile, Set<String> groupIds, long periodSecond) {
        super(getGroupOffsets(groupIndexFile, groupIds));
        if (periodSecond < 0) {
            throw new IllegalArgumentException("period flush must over 0");
        }
        this.groupIndexFile = groupIndexFile;
        this.schedule =
                Executors.newSingleThreadScheduledExecutor(
                        Threads.createThreadFactoryByName("group_persist", true));
        schedule.scheduleWithFixedDelay(
                this::persist, periodSecond, periodSecond, TimeUnit.SECONDS);
    }

    /** persist group offsets. */
    public synchronized void persist() {
        final File tmpFile = createTmpGroupIndexFile();
        if (tmpFile == null) {
            return;
        }
        try (BufferedWriter writer =
                new BufferedWriter(
                        new OutputStreamWriter(
                                new FileOutputStream(tmpFile), StandardCharsets.UTF_8))) {
            foreachGroup(
                    (index, group, groupOffset) -> {
                        if (index != 0) {
                            writer.newLine();
                        }
                        writer.write(groupOffset.toJson());
                    });
            writer.flush();
        } catch (Throwable e) {
            LOG.error("flush groups to tmp file error, file {}", tmpFile.getPath(), e);
        }
        swapIndexFileQuietly(tmpFile);
    }

    /**
     * create new tmp group index file based on groupIndexFile.
     *
     * @return tmp file
     */
    private File createTmpGroupIndexFile() {
        final File file = new File(groupIndexFile + "." + UUID.randomUUID() + TMP_SUFFIX);
        try {
            final File parent = groupIndexFile.getParentFile();
            if (!IOUtils.makeDir(parent)) {
                LOG.error("create dir fail, dir name = {}", parent.getPath());
                return null;
            }
            if (!file.createNewFile()) {
                LOG.error("create new file fail, file name = {}", file.getPath());
                return null;
            }
            return file;
        } catch (Throwable e) {
            LOG.error("create new file fail, file name = {}", file.getPath(), e);
            return null;
        }
    }

    /**
     * delete group index file.
     *
     * @return return true if delete success or not exists
     */
    private boolean deleteGroupIndexFile() {
        if (groupIndexFile.exists() && !groupIndexFile.delete()) {
            LOG.error("delete group index file {} fail", groupIndexFile.getPath());
            return false;
        }
        return true;
    }

    /**
     * swap tmp file to group index file. swap may cause current index file delete success but tmp
     * file rename fail.
     *
     * @param tmpFile tmpFile
     */
    private void swapIndexFileQuietly(File tmpFile) {
        try {
            if (deleteGroupIndexFile() && !tmpFile.renameTo(groupIndexFile)) {
                LOG.warn(
                        "rename tmp file to group index file fail, tmp file {}, group index file {}",
                        tmpFile.getPath(),
                        groupIndexFile.getPath());
            }
        } catch (Throwable e) {
            LOG.warn(
                    "rename tmp file to group index file fail, tmp file {}, group index file {}",
                    tmpFile.getPath(),
                    groupIndexFile.getPath(),
                    e);
        }
    }

    /**
     * add group ids to cache.
     *
     * @param groupIndexFile groupIndexFile
     * @param groupIds groupIds groupIds
     * @return group group offset
     */
    private static Set<GroupOffset> getGroupOffsets(File groupIndexFile, Set<String> groupIds) {

        final int cacheExpectedSize = groupIds.size();
        final List<String> allGroups = new ArrayList<>(groupIds);
        final Set<GroupOffset> existsGroups = parseExistsGroups(groupIndexFile, allGroups);
        final Set<GroupOffset> result = new HashSet<>(existsGroups);
        // AllGroups only contains new groups after call method parseExistsGroups.
        // Add new group with default offset to result ensure all groupIds has one offset.
        allGroups.forEach(groupId -> result.add(defaultGroupOffset(groupId)));
        if (result.size() != cacheExpectedSize) {
            throw new TributaryRuntimeException(
                    "cache size must equal groupIds size, expected size = "
                            + cacheExpectedSize
                            + ", real size = "
                            + result);
        }
        return result;
    }

    /**
     * valid file max tmp group index file.
     *
     * @param groupIndexFile groupIndexFile
     * @return file return null if not found
     */
    private static File findMaxValidTmpGroupIndexFile(File groupIndexFile) {
        final File dir = groupIndexFile.getParentFile();
        final File[] tmpFiles = dir.listFiles(createValidTmpGroupIndexFileFilter(groupIndexFile));
        if (tmpFiles == null || tmpFiles.length == 0) {
            return null;
        }
        return Arrays.stream(tmpFiles).max(Comparator.comparing(File::lastModified)).get();
    }

    /**
     * parse exists groups.
     *
     * @param groupIndexFile groupIndexFile
     * @param allGroups allGroups, allGroups will remove those groups that exits.
     * @return group offsets map
     */
    private static Set<GroupOffset> parseExistsGroups(
            final File groupIndexFile, List<String> allGroups) {

        final Set<GroupOffset> existsGroups = new HashSet<>();

        /*
         * Method {@link FileGroupManager#swapIndexFileQuietly} may cause delete groupIndexFile
         * success but rename tmp file to groupIndexFile fail, Using max valid tmp file if exists
         */
        final File maxTmpFile = findMaxValidTmpGroupIndexFile(groupIndexFile);
        if ((maxTmpFile == null || !maxTmpFile.exists()) && !groupIndexFile.exists()) {
            return existsGroups;
        }
        final File realGroupIndexFile = maxTmpFile != null ? maxTmpFile : groupIndexFile;
        try (BufferedReader reader =
                new BufferedReader(
                        new InputStreamReader(
                                new FileInputStream(realGroupIndexFile), StandardCharsets.UTF_8))) {
            String line;
            while ((line = reader.readLine()) != null) {
                final GroupOffset offset = GroupOffset.fromJson(line);
                if (allGroups.remove(offset.groupId())) {
                    existsGroups.add(offset);
                }
            }
        } catch (Exception e) {
            throw new TributaryRuntimeException("load group index file error", e);
        }
        return existsGroups;
    }

    /**
     * create file name by group id.
     *
     * @return file name
     */
    public static String createFileName(String topic) {
        return topic + FILE_SUFFIX;
    }

    /**
     * filter the tmp file name start with groupIndexFile and end with .tmp.
     *
     * <p>the .tmp file size must equals or over groupIndexFile, and the lastModified over
     * groupIndexFile.
     *
     * @param groupIndexFile groupIndexFile.
     * @return FileFilter
     */
    private static FileFilter createValidTmpGroupIndexFileFilter(File groupIndexFile) {
        return f ->
                f.isFile()
                        && f.getName().startsWith(groupIndexFile.getName())
                        && f.getName().endsWith(TMP_SUFFIX)
                        && f.length() > 0
                        && f.length() >= groupIndexFile.length()
                        && f.lastModified() > groupIndexFile.lastModified();
    }

    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            try {
                if (schedule != null) {
                    schedule.shutdown();
                }
                persist();
            } finally {
                super.close();
            }
        }
    }
}
