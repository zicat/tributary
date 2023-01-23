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

package org.zicat.tributary.channel.file;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zicat.tributary.channel.MemoryOnePartitionGroupManager;
import org.zicat.tributary.channel.RecordsOffset;
import org.zicat.tributary.channel.utils.IOUtils;
import org.zicat.tributary.channel.utils.Threads;
import org.zicat.tributary.channel.utils.TributaryChannelRuntimeException;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.zicat.tributary.channel.utils.IOUtils.readFull;
import static org.zicat.tributary.channel.utils.IOUtils.writeFull;

/**
 * FileGroupManager.
 *
 * <p>Store RecordsOffset in local {@link FileGroupManager#groupIndexFile}.
 *
 * <p>File name example: {myTopic}_group_id.index
 *
 * <p>file content: 4 bytes(Int, GroupLength) + group body + 8 Bytes(Long, segmentId) + 8
 * Bytes(Long, segmentId).
 */
public class FileGroupManager extends MemoryOnePartitionGroupManager {

    private static final Logger LOG = LoggerFactory.getLogger(FileGroupManager.class);
    private static final int RECORD_LENGTH = 20;

    public static final String FILE_SUFFIX = "_group_id.index";

    private final File groupIndexFile;
    private final ScheduledExecutorService schedule;
    private final AtomicBoolean needFlush = new AtomicBoolean(false);
    private ByteBuffer byteBuffer;

    public FileGroupManager(File dir, String topic, List<String> groupIds) {
        super(topic, getGroupOffsets(new File(dir, createFileName(topic)), groupIds));
        this.groupIndexFile = new File(dir, createFileName(topic));
        this.schedule =
                Executors.newScheduledThreadPool(
                        1, Threads.createThreadFactoryByName("group_id_flush_disk", true));
        schedule.scheduleWithFixedDelay(this::flush, 30, 30, TimeUnit.SECONDS);
    }

    @Override
    public void flush(String groupId, RecordsOffset recordsOffset) throws IOException {
        needFlush.set(true);
    }

    @Override
    public void closeCallback() {
        schedule.shutdownNow();
        flush();
    }

    /** flush to disk. */
    public synchronized void flush() {
        if (!needFlush.get()) {
            return;
        }
        final File tmpFile = new File(groupIndexFile + "." + UUID.randomUUID() + ".tmp");
        try (RandomAccessFile randomAccessFile = new RandomAccessFile(tmpFile, "rw");
                FileChannel channel = randomAccessFile.getChannel()) {
            foreachGroup(
                    (group, recordsOffset) -> {
                        final byte[] groupByteArray = group.getBytes(StandardCharsets.UTF_8);
                        final int bufferSize = groupByteArray.length + RECORD_LENGTH;
                        byteBuffer = IOUtils.reAllocate(byteBuffer, bufferSize);
                        byteBuffer.putInt(groupByteArray.length);
                        byteBuffer.put(groupByteArray);
                        recordsOffset.fillBuffer(byteBuffer);
                        writeFull(channel, byteBuffer);
                    });
            channel.force(true);
        } catch (IOException e) {
            LOG.error("flush groups to tmp file error, file " + tmpFile.getPath(), e);
        }
        if (groupIndexFile.exists() && !groupIndexFile.delete()) {
            LOG.error("delete group index file " + groupIndexFile.getPath() + " fail");
            return;
        }
        if (!tmpFile.renameTo(groupIndexFile)) {
            LOG.error(
                    "rename tmp file to group index file fail, tmp file "
                            + tmpFile.getPath()
                            + ", group index file "
                            + groupIndexFile.getPath());
        }
    }

    /**
     * add group ids to cache.
     *
     * @param groupIndexFile groupIndexFile
     * @param groupIds groupIds groupIds
     * @return group records offset
     */
    private static Map<String, RecordsOffset> getGroupOffsets(
            File groupIndexFile, List<String> groupIds) {

        final int cacheExpectedSize = groupIds.size();
        final List<String> allGroups = new ArrayList<>(groupIds);
        final Map<String, RecordsOffset> existsGroups =
                parseExistsGroups(groupIndexFile, allGroups);
        final Map<String, RecordsOffset> result = new HashMap<>(existsGroups);
        // AllGroups only contains new groups after call method parseExistsGroups.
        // Add new group with default offset to result ensure all groupIds has one offset.
        allGroups.forEach(groupId -> result.put(groupId, createNewGroupRecordsOffset()));
        if (result.size() != cacheExpectedSize) {
            throw new TributaryChannelRuntimeException(
                    "cache size must equal groupIds size, expected size = "
                            + cacheExpectedSize
                            + ", real size = "
                            + result);
        }
        return result;
    }

    /**
     * parse exists groups.
     *
     * @param groupIndexFile groupIndexFile
     * @param allGroups allGroups, allGroups will remove those groups that exits.
     * @return group offsets map
     */
    private static Map<String, RecordsOffset> parseExistsGroups(
            final File groupIndexFile, List<String> allGroups) {
        final Map<String, RecordsOffset> existsGroups = new HashMap<>();
        File realGroupIndexFile = groupIndexFile;
        if (!groupIndexFile.exists()) {
            final File dir = groupIndexFile.getParentFile();
            final File[] tmpFiles =
                    dir.listFiles(
                            f ->
                                    f.isFile()
                                            && f.getName().startsWith(groupIndexFile.getName())
                                            && f.getName().endsWith(".tmp"));
            if (tmpFiles == null || tmpFiles.length == 0) {
                return existsGroups;
            }
            realGroupIndexFile =
                    Arrays.stream(tmpFiles).max(Comparator.comparing(File::lastModified)).get();
        }
        try {
            final ByteBuffer byteBuffer = ByteBuffer.wrap(readFull(realGroupIndexFile));
            while (byteBuffer.hasRemaining()) {
                final int groupLength = byteBuffer.getInt();
                final byte[] groupArray = new byte[groupLength];
                byteBuffer.get(groupArray);
                final String groupId = new String(groupArray, StandardCharsets.UTF_8);
                final RecordsOffset offset = RecordsOffset.parserByteBuffer(byteBuffer);
                if (allGroups.remove(groupId)) {
                    existsGroups.put(groupId, offset);
                }
            }
        } catch (Exception e) {
            throw new TributaryChannelRuntimeException("load group index file error", e);
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
}
