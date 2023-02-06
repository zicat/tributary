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

import org.zicat.tributary.channel.OnePartitionGroupManager;
import org.zicat.tributary.channel.OnePartitionMemoryGroupManager;
import org.zicat.tributary.common.IOUtils;
import org.zicat.tributary.common.TributaryRuntimeException;

import java.io.File;
import java.io.IOException;

/** FileChannelBuilder for {@link OnePartitionFileChannel}. */
public class OnePartitionFileChannelBuilder extends ChannelBuilder {

    private File dir;
    private long groupPersistPeriodSecond =
            OnePartitionMemoryGroupManager.DEFAULT_GROUP_PERSIST_PERIOD_SECOND;

    public OnePartitionFileChannelBuilder dir(File dir) {
        try {
            this.dir = dir.getCanonicalFile();
        } catch (IOException e) {
            throw new TributaryRuntimeException("get canonical file fail", e);
        }
        return this;
    }

    public OnePartitionFileChannelBuilder groupPersistPeriodSecond(long groupPersistPeriodSecond) {
        this.groupPersistPeriodSecond = groupPersistPeriodSecond;
        return this;
    }

    /**
     * build by customer groupManager.
     *
     * @param groupManager groupManager
     * @return FileChannel
     */
    public OnePartitionFileChannel build(OnePartitionGroupManager groupManager) {
        if (dir == null) {
            throw new IllegalStateException("dir is null");
        }
        if (!dir.exists() && !IOUtils.makeDir(dir)) {
            throw new IllegalStateException(
                    "dir not exist and try to create fail " + dir.getPath());
        }
        if (consumerGroups == null || consumerGroups.isEmpty()) {
            throw new IllegalStateException("file channel must has at least one consumer group");
        }
        return new OnePartitionFileChannel(
                topic,
                groupManager,
                dir,
                blockSize,
                segmentSize,
                compressionType,
                flushPeriod,
                flushTimeUnit,
                flushPageCacheSize,
                flushForce);
    }

    /**
     * build file channel.
     *
     * @return return
     */
    public OnePartitionFileChannel build() {
        return build(
                new OnePartitionFileGroupManager(
                        dir, topic, consumerGroups, groupPersistPeriodSecond));
    }

    /**
     * create new builder.
     *
     * @return FileChannelBuilder
     */
    public static OnePartitionFileChannelBuilder newBuilder() {
        return new OnePartitionFileChannelBuilder();
    }
}
