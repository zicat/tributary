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
import org.zicat.tributary.channel.OnePartitionGroupManager;
import org.zicat.tributary.channel.RecordsOffset;
import org.zicat.tributary.channel.utils.IOUtils;
import org.zicat.tributary.channel.utils.TributaryChannelRuntimeException;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.zicat.tributary.channel.file.SegmentUtil.realPrefix;
import static org.zicat.tributary.channel.utils.IOUtils.*;

/**
 * FileGroupManager.
 *
 * <p>Store RecordsOffset in local {@link FileGroupManager#dir}.
 *
 * <p>each partition has different {@link FileGroupManager#dir}.
 *
 * <p>File name start with topic_name + "_" + {@link FileGroupManager#filePrefix} + "_" + {@link
 * FileGroupManager#FILE_SUFFIX} group_id, example: {myTopic}_group_id_{myGroup}.index
 *
 * <p>file content: 8 Bytes(Long, segmentId) + 8 Bytes(Long, segmentId).
 */
public class FileGroupManager implements OnePartitionGroupManager {

    private static final Logger LOG = LoggerFactory.getLogger(FileGroupManager.class);

    public static final String FILE_PREFIX = "group_id_";
    private static final String FILE_SUFFIX = ".index";

    private final Map<String, RecordsOffset> cache = new ConcurrentHashMap<>();
    private final Map<String, FileChannel> fileCache = new ConcurrentHashMap<>();
    private final File dir;
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final ByteBuffer byteBuffer = ByteBuffer.allocateDirect(16);
    private final String filePrefix;
    private final String topic;
    private final Set<String> groups;

    public FileGroupManager(File dir, String topic, List<String> groupIds) {
        this.dir = dir;
        this.topic = topic;
        this.filePrefix = realPrefix(topic, FILE_PREFIX);
        this.groups = new HashSet<>(groupIds);
        loadCache(groupIds);
    }

    /**
     * add group ids to cache.
     *
     * @param groupIds groupIds
     */
    private void loadCache(List<String> groupIds) {

        final int cacheExpectedSize = groupIds.size();
        final File[] files = dir.listFiles(file -> isGroupIndexFile(file.getName()));
        final List<String> newGroups = new ArrayList<>(groupIds);
        if (files != null) {
            for (File file : files) {
                try {
                    final String groupId = groupIdByFileName(file.getName());
                    if (!newGroups.remove(groupId)) {
                        continue;
                    }
                    final RandomAccessFile randomAccessFile = new RandomAccessFile(file, "rw");
                    final FileChannel fileChannel = randomAccessFile.getChannel();
                    readFully(fileChannel, byteBuffer, 0).flip();
                    fileCache.put(groupId, fileChannel);
                    cache.put(groupId, RecordsOffset.parserByteBuffer(byteBuffer));
                    byteBuffer.clear();
                } catch (Exception e) {
                    throw new TributaryChannelRuntimeException("load group index file error", e);
                }
            }
        }
        newGroups.forEach(
                groupId -> {
                    try {
                        commit(groupId, createNewGroupRecordsOffset());
                    } catch (Exception e) {
                        throw new TributaryChannelRuntimeException(
                                "load group index file error", e);
                    }
                });

        if (cache.size() != cacheExpectedSize) {
            throw new TributaryChannelRuntimeException(
                    "cache size must equal groupIds size, expected size = "
                            + cacheExpectedSize
                            + ", real size = "
                            + cache.size());
        }
    }

    /**
     * create new group records offset.
     *
     * @return RecordsOffset
     */
    public static RecordsOffset createNewGroupRecordsOffset() {
        return new RecordsOffset(-1, 0);
    }

    /**
     * check name is group id file.
     *
     * @param name name
     * @return true if group index file
     */
    public boolean isGroupIndexFile(String name) {
        return name.startsWith(filePrefix) && name.endsWith(FILE_SUFFIX);
    }

    /**
     * create file name by group id.
     *
     * @param groupId group id
     * @return file name
     */
    public String createFileNameByGroupId(String groupId) {
        return filePrefix + groupId + FILE_SUFFIX;
    }

    /**
     * get group id by name.
     *
     * @param name name
     * @return group id
     */
    public String groupIdByFileName(String name) {
        return name.substring(filePrefix.length(), name.length() - FILE_SUFFIX.length());
    }

    @Override
    public synchronized void commit(String groupId, RecordsOffset recordsOffset)
            throws IOException {

        isOpen();
        RecordsOffset cachedRecordsOffset = cache.get(groupId);
        if (cachedRecordsOffset != null && cachedRecordsOffset.compareTo(recordsOffset) >= 0) {
            return;
        }
        final FileChannel fileChannel =
                fileCache.computeIfAbsent(
                        groupId,
                        key -> {
                            if (!makeDir(dir)) {
                                throw new TributaryChannelRuntimeException(
                                        "create dir fail, dir " + dir.getPath());
                            }
                            final File file = new File(dir, createFileNameByGroupId(key));
                            try {
                                return new RandomAccessFile(file, "rw").getChannel();
                            } catch (IOException e) {
                                throw new TributaryChannelRuntimeException(
                                        "create random file error", e);
                            }
                        });
        recordsOffset.fillBuffer(byteBuffer);
        fileChannel.position(0);
        writeFull(fileChannel, byteBuffer);
        force(fileChannel, false);
        byteBuffer.clear();
        cache.put(groupId, recordsOffset);
    }

    /**
     * flush to page cache.
     *
     * @param fileChannel fileChannel
     * @param metaData metaData
     */
    private void force(FileChannel fileChannel, boolean metaData) {
        try {
            fileChannel.force(metaData);
        } catch (Throwable e) {
            LOG.warn("flush error", e);
        }
    }

    @Override
    public String topic() {
        return topic;
    }

    @Override
    public Set<String> groups() {
        return groups;
    }

    @Override
    public RecordsOffset getRecordsOffset(String groupId) {
        isOpen();
        return cache.get(groupId);
    }

    @Override
    public RecordsOffset getMinRecordsOffset() {
        isOpen();
        return cache.values().stream().min(RecordsOffset::compareTo).orElse(null);
    }

    /** check whether closed. */
    private void isOpen() {
        if (closed.get()) {
            throw new IllegalStateException("FileGroupManager is closed");
        }
    }

    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            fileCache.forEach((k, c) -> force(c, true));
            fileCache.forEach((k, c) -> IOUtils.closeQuietly(c));
            fileCache.clear();
            cache.clear();
        }
    }
}
