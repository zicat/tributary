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

import static org.zicat.tributary.channel.ChannelConfigOption.*;
import static org.zicat.tributary.channel.file.FileChannelConfigOption.*;
import static org.zicat.tributary.channel.group.FileGroupManager.OPTION_GROUP_PERSIST_PERIOD;

import org.zicat.tributary.channel.Channel;
import org.zicat.tributary.channel.ChannelFactory;
import org.zicat.tributary.channel.CompressionType;
import org.zicat.tributary.common.PercentSize;
import org.zicat.tributary.common.ReadableConfig;

import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/** FileChannelFactory. */
public class FileChannelFactory implements ChannelFactory {

    public static final String TYPE = "file";

    @Override
    public String type() {
        return TYPE;
    }

    @Override
    public Channel createChannel(String topic, ReadableConfig config) throws IOException {

        final List<String> dirs = config.get(OPTION_PARTITION_PATHS);
        final Set<String> groupSet = groupSet(config);
        final long blockSize = config.get(OPTION_BLOCK_SIZE).getBytes();
        final long segmentSize = config.get(OPTION_SEGMENT_SIZE).getBytes();
        final Duration flushPeriod = config.get(OPTION_FLUSH_PERIOD);
        final Duration cleanupExpiredSegmentPeriod =
                config.get(OPTION_CLEANUP_EXPIRED_SEGMENT_PERIOD);
        final CompressionType compression = config.get(OPTION_COMPRESSION);
        final long groupPersist = config.get(OPTION_GROUP_PERSIST_PERIOD).getSeconds();
        final int blockCacheCount = config.get(OPTION_BLOCK_CACHE_PER_PARTITION_SIZE);
        final boolean appendSyncWait = config.get(OPTION_APPEND_SYNC_AWAIT);
        final Duration appendSyncWaitTimeout =
                config.get(OPTION_APPEND_SYNC_AWAIT_TIMEOUT, OPTION_FLUSH_PERIOD);
        final PercentSize capacityProtectedPercent = config.get(OPTION_CAPACITY_PROTECTED_PERCENT);
        final FileSingleChannelBuilder builder =
                FileSingleChannelBuilder.newBuilder()
                        .dirs(createDir(dirs))
                        .flushPeriodMills(flushPeriod.toMillis())
                        .cleanupExpiredSegmentPeriodMills(cleanupExpiredSegmentPeriod.toMillis())
                        .groupPersistPeriodSecond(groupPersist)
                        .capacityProtectedPercent(capacityProtectedPercent.getPercent())
                        .blockCacheCount(blockCacheCount);
        return builder.blockSize((int) blockSize)
                .segmentSize(segmentSize)
                .compressionType(compression)
                .topic(topic)
                .consumerGroups(groupSet)
                .appendSyncWait(appendSyncWait)
                .appendSyncWaitTimeoutMs(appendSyncWaitTimeout.toMillis())
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
