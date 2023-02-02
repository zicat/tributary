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

import org.zicat.tributary.channel.Channel;
import org.zicat.tributary.channel.ChannelFactory;
import org.zicat.tributary.channel.CompressionType;

import java.io.File;
import java.util.*;
import java.util.concurrent.TimeUnit;

import static org.zicat.tributary.channel.MemoryOnePartitionGroupManager.DEFAULT_GROUP_PERSIST_PERIOD_SECOND;

/** FileChannelFactory. */
public class FileChannelFactory implements ChannelFactory {

    public static final String TYPE = "file";

    public static final String SPLIT_STR = ",";
    public static final String KEY_FILE_DIRS = "dirs";

    public static final String KEY_GROUP_PERSIST_PERIOD_SECOND = "groupPersistPeriodSecond";

    public static final String KEY_FILE_BLOCK_SIZE = "blockSize";
    public static final String DEFAULT_FILE_BLOCK_SIZE = String.valueOf(32 * 1024);

    public static final String KEY_FILE_SEGMENT_SIZE = "segmentSize";
    public static final String DEFAULT_FILE_SEGMENT_SIZE =
            String.valueOf(4L * 1024L * 1024L * 1024L);

    public static final String KEY_FILE_FLUSH_PERIOD_MILLS = "flushPeriodMills";
    public static final String DEFAULT_FILE_FLUSH_PERIOD_MILLS = String.valueOf(500);

    public static final String KEY_FILE_FLUSH_FORCE = "flushForce";
    public static final String DEFAULT_FILE_FLUSH_FORCE = "false";

    public static final String KEY_FILE_COMPRESSION = "compression";
    public static final String DEFAULT_FILE_COMPRESSION = "none";

    public static final String KEY_FILE_FLUSH_PAGE_CACHE_SIZE = "flushPageCacheSize";
    public static final String DEFAULT_FILE_FLUSH_PAGE_CACHE_SIZE =
            String.valueOf(1024L * 1024L * 32L);

    public static final String KEY_GROUPS = "groups";

    @Override
    public String type() {
        return TYPE;
    }

    @Override
    public Channel createChannel(String topic, Map<String, String> params) {
        if (topic == null) {
            throw new IllegalStateException("topic name not configuration");
        }
        final String groupIds = params.get(KEY_GROUPS);
        if (groupIds == null) {
            throw new IllegalStateException("topic has no sink group " + topic);
        }
        final String dirsStr = params.get(KEY_FILE_DIRS);
        if (dirsStr == null) {
            throw new IllegalStateException("dirs not configuration");
        }
        final Set<String> groupSet = new HashSet<>(Arrays.asList(groupIds.split(SPLIT_STR)));
        final List<String> dirs = Arrays.asList(dirsStr.split(SPLIT_STR));
        final int blockSize =
                Integer.parseInt(params.getOrDefault(KEY_FILE_BLOCK_SIZE, DEFAULT_FILE_BLOCK_SIZE));
        final long segmentSize =
                Long.parseLong(
                        params.getOrDefault(KEY_FILE_SEGMENT_SIZE, DEFAULT_FILE_SEGMENT_SIZE));
        final int flushPeriodMills =
                Integer.parseInt(
                        params.getOrDefault(
                                KEY_FILE_FLUSH_PERIOD_MILLS, DEFAULT_FILE_FLUSH_PERIOD_MILLS));
        final long flushPageCacheSize =
                Long.parseLong(
                        params.getOrDefault(
                                KEY_FILE_FLUSH_PAGE_CACHE_SIZE,
                                DEFAULT_FILE_FLUSH_PAGE_CACHE_SIZE));
        final String compression =
                params.getOrDefault(KEY_FILE_COMPRESSION, DEFAULT_FILE_COMPRESSION);

        final boolean flushForce =
                Boolean.parseBoolean(
                        params.getOrDefault(KEY_FILE_FLUSH_FORCE, DEFAULT_FILE_FLUSH_FORCE));

        final long groupPersist =
                Long.parseLong(
                        params.getOrDefault(
                                KEY_GROUP_PERSIST_PERIOD_SECOND,
                                String.valueOf(DEFAULT_GROUP_PERSIST_PERIOD_SECOND)));

        final PartitionFileChannelBuilder builder =
                PartitionFileChannelBuilder.newBuilder()
                        .dirs(createDir(dirs))
                        .groupPersistPeriodSecond(groupPersist);
        builder.blockSize(blockSize)
                .segmentSize(segmentSize)
                .compressionType(CompressionType.getByName(compression))
                .flushPeriod(flushPeriodMills, TimeUnit.MILLISECONDS)
                .flushPageCacheSize(flushPageCacheSize)
                .flushForce(flushForce)
                .topic(topic)
                .consumerGroups(new ArrayList<>(groupSet));
        return builder.build();
    }

    /**
     * create dir.
     *
     * @param dirs dir
     * @return list files.
     */
    private List<File> createDir(List<String> dirs) {
        final List<File> result = new ArrayList<>();
        for (String dir : dirs) {
            result.add(new File(dir));
        }
        return result;
    }
}
