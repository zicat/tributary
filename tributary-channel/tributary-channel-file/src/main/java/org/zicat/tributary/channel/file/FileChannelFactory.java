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
import java.util.*;
import java.util.concurrent.TimeUnit;

import static org.zicat.tributary.channel.ChannelConfigOption.*;
import static org.zicat.tributary.channel.file.FileChannelConfigOption.*;
import static org.zicat.tributary.channel.memory.MemoryChannelFactory.SPLIT_STR;

/** FileChannelFactory. */
public class FileChannelFactory implements ChannelFactory {

    public static final String TYPE = "file";

    @Override
    public String type() {
        return TYPE;
    }

    @Override
    public Channel createChannel(String topic, ReadableConfig config) throws IOException {

        final String dirsStr = config.get(OPTION_DIR);
        final List<String> dirs = Arrays.asList(dirsStr.split(SPLIT_STR));

        final String groupIds = config.get(OPTION_GROUPS);
        final Set<String> groupSet = new HashSet<>(Arrays.asList(groupIds.split(SPLIT_STR)));

        final int blockSize = config.get(OPTION_BLOCK_SIZE);
        final long segmentSize = config.get(OPTION_SEGMENT_SIZE);
        final int flushPeriodMills = config.get(OPTION_FLUSH_PERIOD_MILLS);
        final long flushPageCacheSize = config.get(OPTION_FLUSH_PAGE_CACHE_SIZE);
        final String compression = config.get(OPTION_COMPRESSION);
        final long groupPersist = config.get(OPTION_GROUP_PERSIST_PERIOD_SECOND);
        final FileChannelBuilder builder =
                FileChannelBuilder.newBuilder()
                        .dirs(createDir(dirs))
                        .flushPeriod(flushPeriodMills, TimeUnit.MILLISECONDS)
                        .groupPersistPeriodSecond(groupPersist);
        builder.blockSize(blockSize)
                .segmentSize(segmentSize)
                .compressionType(CompressionType.getByName(compression))
                .flushPageCacheSize(flushPageCacheSize)
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
