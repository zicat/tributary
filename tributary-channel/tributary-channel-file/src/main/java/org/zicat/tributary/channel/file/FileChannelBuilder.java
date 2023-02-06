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

import org.zicat.tributary.channel.OnePartitionMemoryGroupManager;

import java.io.File;
import java.util.List;

/** FileChannelBuilder for {@link FileChannel}. */
public class FileChannelBuilder extends ChannelBuilder {

    private List<File> dirs;
    private long groupPersistPeriodSecond =
            OnePartitionMemoryGroupManager.DEFAULT_GROUP_PERSIST_PERIOD_SECOND;

    /**
     * set dir list.
     *
     * @param dirs dirs.
     * @return this
     */
    public FileChannelBuilder dirs(List<File> dirs) {
        this.dirs = dirs;
        return this;
    }

    /**
     * set group persist period second.
     *
     * @param groupPersistPeriodSecond groupPersistPeriodSecond.
     * @return this
     */
    public FileChannelBuilder groupPersistPeriodSecond(long groupPersistPeriodSecond) {
        this.groupPersistPeriodSecond = groupPersistPeriodSecond;
        return this;
    }

    /**
     * build to channel.
     *
     * @return PartitionFileChannel
     */
    public FileChannel build() {

        if (dirs == null || dirs.isEmpty()) {
            throw new IllegalStateException("dir list is null or empty");
        }
        if (consumerGroups == null || consumerGroups.isEmpty()) {
            throw new IllegalStateException("consumer group is null or empty");
        }

        final OnePartitionFileChannelBuilder builder = OnePartitionFileChannelBuilder.newBuilder();
        builder.segmentSize(segmentSize)
                .blockSize(blockSize)
                .compressionType(compressionType)
                .flushPeriod(0, flushTimeUnit)
                .flushPageCacheSize(flushPageCacheSize)
                .topic(topic)
                .consumerGroups(consumerGroups)
                .flushForce(flushForce);
        final OnePartitionFileChannel[] fileChannels = new OnePartitionFileChannel[dirs.size()];
        for (int i = 0; i < dirs.size(); i++) {
            builder.dir(dirs.get(i));
            builder.groupPersistPeriodSecond(groupPersistPeriodSecond);
            fileChannels[i] = builder.build();
        }
        return new FileChannel(fileChannels, flushPeriod, flushTimeUnit);
    }

    /**
     * create new builder.
     *
     * @return PartitionFileChannelBuilder
     */
    public static FileChannelBuilder newBuilder() {
        return new FileChannelBuilder();
    }
}
