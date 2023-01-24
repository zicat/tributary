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

import java.io.File;
import java.util.List;

import static org.zicat.tributary.channel.MemoryOnePartitionGroupManager.DEFAULT_GROUP_PERSIST_PERIOD_SECOND;

/** PartitionFileChannelBuilder for {@link PartitionFileChannel}. */
public class PartitionFileChannelBuilder extends ChannelBuilder {

    private List<File> dirs;
    private long groupPersistPeriodSecond = DEFAULT_GROUP_PERSIST_PERIOD_SECOND;

    /**
     * set dir list.
     *
     * @param dirs dirs.
     * @return this
     */
    public PartitionFileChannelBuilder dirs(List<File> dirs) {
        this.dirs = dirs;
        return this;
    }

    /**
     * set group persist period second.
     *
     * @param groupPersistPeriodSecond groupPersistPeriodSecond.
     * @return this
     */
    public PartitionFileChannelBuilder groupPersistPeriodSecond(long groupPersistPeriodSecond) {
        this.groupPersistPeriodSecond = groupPersistPeriodSecond;
        return this;
    }

    /**
     * build to channel.
     *
     * @return PartitionFileChannel
     */
    public PartitionFileChannel build() {
        if (dirs == null || dirs.isEmpty()) {
            throw new IllegalStateException("dir list is null or empty");
        }
        return new PartitionFileChannel(
                topic,
                consumerGroups,
                dirs,
                blockSize,
                segmentSize,
                compressionType,
                flushPeriod,
                flushTimeUnit,
                flushPageCacheSize,
                flushForce,
                groupPersistPeriodSecond);
    }

    /**
     * create new builder.
     *
     * @return PartitionFileChannelBuilder
     */
    public static PartitionFileChannelBuilder newBuilder() {
        return new PartitionFileChannelBuilder();
    }
}
