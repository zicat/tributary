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

import org.zicat.tributary.channel.AbstractSingleChannel.SingleGroupManagerFactory;
import org.zicat.tributary.channel.CompressionType;
import org.zicat.tributary.channel.DefaultChannel;
import org.zicat.tributary.channel.DefaultChannel.AbstractChannelArrayFactory;
import static org.zicat.tributary.channel.file.FileChannelConfigOption.OPTION_CAPACITY_PROTECTED_PERCENT;
import org.zicat.tributary.channel.group.FileGroupManager;
import org.zicat.tributary.common.IOUtils;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileStore;
import java.nio.file.Files;
import java.util.List;
import java.util.Set;

import static org.zicat.tributary.channel.ChannelConfigOption.OPTION_BLOCK_CACHE_PER_PARTITION_SIZE;
import static org.zicat.tributary.channel.group.FileGroupManager.OPTION_GROUP_PERSIST_PERIOD;
import static org.zicat.tributary.channel.group.FileGroupManager.createFileName;
import org.zicat.tributary.common.PercentSize;

/** FileSingleChannelBuilder. */
public class FileSingleChannelBuilder {

    protected List<String> dirs;
    protected long groupPeriodSecond = OPTION_GROUP_PERSIST_PERIOD.defaultValue().getSeconds();
    protected String topic;
    protected Long segmentSize;
    protected Integer blockSize;
    protected CompressionType compressionType;
    protected Set<String> consumerGroups;
    protected long flushPeriodMills = 1000;
    protected long cleanupExpiredSegmentPeriodMills = 10000;
    protected PercentSize capacityProtectedPercent =
            OPTION_CAPACITY_PROTECTED_PERCENT.defaultValue();

    protected boolean appendSyncWait = false;
    protected long appendSyncWaitTimeoutMs = 0;
    protected int blockCacheCount = OPTION_BLOCK_CACHE_PER_PARTITION_SIZE.defaultValue();

    /**
     * set flush period .
     *
     * @param flushPeriodMills flushPeriodMills
     * @return this
     */
    public FileSingleChannelBuilder flushPeriodMills(long flushPeriodMills) {
        this.flushPeriodMills = flushPeriodMills;
        return this;
    }

    public FileSingleChannelBuilder capacityProtectedPercent(PercentSize capacityProtectedPercent) {
        this.capacityProtectedPercent = capacityProtectedPercent;
        return this;
    }

    /**
     * set cleanup expired segment period mills.
     *
     * @param cleanupExpiredSegmentPeriodMills cleanupExpiredSegmentPeriodMills
     * @return this
     */
    public FileSingleChannelBuilder cleanupExpiredSegmentPeriodMills(
            long cleanupExpiredSegmentPeriodMills) {
        this.cleanupExpiredSegmentPeriodMills = cleanupExpiredSegmentPeriodMills;
        return this;
    }

    /**
     * set block size.
     *
     * @param blockSize blockSize
     * @return this
     */
    public FileSingleChannelBuilder blockSize(int blockSize) {
        this.blockSize = blockSize;
        return this;
    }

    /**
     * set dir list.
     *
     * @param dirs dirs.
     * @return this
     */
    public FileSingleChannelBuilder dirs(List<String> dirs) {
        this.dirs = dirs;
        return this;
    }

    /**
     * set group persist period second.
     *
     * @param groupPeriodSecond groupPeriodSecond.
     * @return this
     */
    public FileSingleChannelBuilder groupPersistPeriodSecond(long groupPeriodSecond) {
        this.groupPeriodSecond = groupPeriodSecond;
        return this;
    }

    /**
     * set compression type.
     *
     * @param compressionType compressionType
     * @return this
     */
    public FileSingleChannelBuilder compressionType(CompressionType compressionType) {
        if (compressionType != null) {
            this.compressionType = compressionType;
        }
        return this;
    }

    /**
     * set segment size.
     *
     * @param segmentSize segmentSize
     * @return this
     */
    public FileSingleChannelBuilder segmentSize(Long segmentSize) {
        this.segmentSize = segmentSize;
        return this;
    }

    /**
     * set block size.
     *
     * @param blockCacheCount blockCacheCount
     * @return this
     */
    public FileSingleChannelBuilder blockCacheCount(Integer blockCacheCount) {
        this.blockCacheCount = blockCacheCount;
        return this;
    }

    /**
     * set topic.
     *
     * @param topic topic
     * @return this
     */
    public FileSingleChannelBuilder topic(String topic) {
        this.topic = topic;
        return this;
    }

    /**
     * set consumer groups.
     *
     * @param consumerGroups consumerGroups
     * @return this
     */
    public FileSingleChannelBuilder consumerGroups(Set<String> consumerGroups) {
        this.consumerGroups = consumerGroups;
        return this;
    }

    /**
     * set append sync wait.
     *
     * @param appendSyncWait appendSyncWait
     * @return this
     */
    public FileSingleChannelBuilder appendSyncWait(boolean appendSyncWait) {
        this.appendSyncWait = appendSyncWait;
        return this;
    }

    /**
     * set append sync wait timeout ms.
     *
     * @param appendSyncWaitTimeoutMs appendSyncWaitTimeoutMs
     * @return this
     */
    public FileSingleChannelBuilder appendSyncWaitTimeoutMs(long appendSyncWaitTimeoutMs) {
        this.appendSyncWaitTimeoutMs = appendSyncWaitTimeoutMs;
        return this;
    }

    /**
     * build to channel.
     *
     * @return PartitionFileChannel
     */
    public DefaultChannel<FileSingleChannel> build() throws IOException {
        if (dirs == null || dirs.isEmpty()) {
            throw new IllegalStateException("dir list is null or empty");
        }
        if (consumerGroups == null || consumerGroups.isEmpty()) {
            throw new IllegalStateException("file channel must has at least one consumer group");
        }
        final File[] canonicalDirs = new File[dirs.size()];
        final FileStore[] fileStores = new FileStore[dirs.size()];
        final SingleGroupManagerFactory[] factories = new SingleGroupManagerFactory[dirs.size()];
        for (int i = 0; i < dirs.size(); i++) {
            final File dir = new File(dirs.get(i)).getCanonicalFile();
            if (!dir.exists() && !IOUtils.makeDir(dir)) {
                throw new IllegalStateException("try to create fail " + dir.getPath());
            }
            canonicalDirs[i] = dir;
            fileStores[i] = Files.getFileStore(dir.toPath());
            final File groupFile = new File(dir, createFileName(topic));
            factories[i] = () -> new FileGroupManager(groupFile, consumerGroups, groupPeriodSecond);
        }
        return new DefaultChannel<>(
                new AbstractChannelArrayFactory<FileSingleChannel>(topic, consumerGroups) {
                    @SuppressWarnings("resource")
                    @Override
                    public FileSingleChannel[] create() {
                        final int partitions = canonicalDirs.length;
                        final FileSingleChannel[] fileChannels = new FileSingleChannel[partitions];
                        for (int i = 0; i < partitions; i++) {
                            fileChannels[i] =
                                    new FileSingleChannel(
                                            topic,
                                            factories[i],
                                            blockSize,
                                            segmentSize,
                                            compressionType,
                                            canonicalDirs[i],
                                            blockCacheCount,
                                            appendSyncWait,
                                            appendSyncWaitTimeoutMs,
                                            fileStores[i]);
                        }
                        return fileChannels;
                    }
                },
                flushPeriodMills,
                cleanupExpiredSegmentPeriodMills,
                capacityProtectedPercent);
    }
}
