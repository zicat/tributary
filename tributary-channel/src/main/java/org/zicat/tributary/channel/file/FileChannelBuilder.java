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

import java.io.File;

import static org.zicat.tributary.channel.MemoryOnePartitionGroupManager.DEFAULT_GROUP_PERSIST_PERIOD_SECOND;
import static org.zicat.tributary.channel.utils.IOUtils.makeDir;

/** FileChannelBuilder for {@link FileChannel}. */
public class FileChannelBuilder extends ChannelBuilder {

    private File dir;
    private long groupPersistPeriodSecond = DEFAULT_GROUP_PERSIST_PERIOD_SECOND;

    public FileChannelBuilder dir(File dir) {
        this.dir = dir;
        return this;
    }

    public FileChannelBuilder groupPersistPeriodSecond(long groupPersistPeriodSecond) {
        this.groupPersistPeriodSecond = groupPersistPeriodSecond;
        return this;
    }

    /**
     * build by customer groupManager.
     *
     * @param groupManager groupManager
     * @return FileChannel
     */
    public FileChannel build(OnePartitionGroupManager groupManager) {
        if (dir == null) {
            throw new IllegalStateException("dir is null");
        }
        if (!dir.exists() && !makeDir(dir)) {
            throw new IllegalStateException(
                    "dir not exist and try to create fail " + dir.getPath());
        }
        if (consumerGroups == null || consumerGroups.isEmpty()) {
            throw new IllegalStateException("file channel must has at least one consumer group");
        }
        return new FileChannel(
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
    public FileChannel build() {
        return build(new FileGroupManager(dir, topic, consumerGroups, groupPersistPeriodSecond));
    }

    /**
     * create new builder.
     *
     * @return FileChannelBuilder
     */
    public static FileChannelBuilder newBuilder() {
        return new FileChannelBuilder();
    }
}
