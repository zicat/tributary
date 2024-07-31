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
import org.zicat.tributary.common.ReadableConfig;

import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

import static org.zicat.tributary.channel.ChannelConfigOption.*;
import static org.zicat.tributary.channel.file.FileChannelConfigOption.*;
import static org.zicat.tributary.channel.group.FileGroupManager.OPTION_GROUP_PERSIST_PERIOD;

/** FileChannelFactory. */
public class FileChannelFactory implements ChannelFactory {

    public static final String TYPE = "file";
    public static final String SPLIT_STR = ",";

    @Override
    public String type() {
        return TYPE;
    }

    @Override
    public Channel createChannel(String topic, ReadableConfig config) throws IOException {

        final String partitionPath = config.get(OPTION_PARTITION_PATHS);
        final List<String> dirs = Arrays.asList(partitionPath.split(SPLIT_STR));
        final Set<String> groupSet = groupSet(config);
        final int blockSize = (int) config.get(OPTION_BLOCK_SIZE).getBytes();
        final long segmentSize = config.get(OPTION_SEGMENT_SIZE).getBytes();
        final long flushPeriodMills = config.get(OPTION_FLUSH_PERIOD).toMillis();
        final CompressionType compression = config.get(OPTION_COMPRESSION);
        final long groupPersist = config.get(OPTION_GROUP_PERSIST_PERIOD).getSeconds();
        final int blockCacheCount = config.get(OPTION_BLOCK_CACHE_PER_PARTITION_SIZE);
        final boolean appendSyncWait = config.get(OPTION_APPEND_SYNC_AWAIT);
        final Duration appendSyncWaitTimeoutDuration = config.get(OPTION_APPEND_SYNC_AWAIT_TIMEOUT);
        final long appendSyncWaitTimeoutMs =
                appendSyncWaitTimeoutDuration == null
                        ? flushPeriodMills
                        : appendSyncWaitTimeoutDuration.toMillis();
        final FileChannelBuilder builder =
                FileChannelBuilder.newBuilder()
                        .dirs(createDir(dirs))
                        .flushPeriodMills(flushPeriodMills)
                        .groupPersistPeriodSecond(groupPersist)
                        .blockCacheCount(blockCacheCount);
        return builder.blockSize(blockSize)
                .segmentSize(segmentSize)
                .compressionType(compression)
                .topic(topic)
                .consumerGroups(groupSet)
                .appendSyncWait(appendSyncWait)
                .appendSyncWaitTimeoutMs(appendSyncWaitTimeoutMs)
                .build();
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
