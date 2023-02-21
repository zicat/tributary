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

import org.zicat.tributary.channel.AbstractChannel;
import org.zicat.tributary.channel.CompressionType;
import org.zicat.tributary.channel.DefaultChannel;
import org.zicat.tributary.channel.group.FileGroupManager;
import org.zicat.tributary.common.IOUtils;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Set;

import static org.zicat.tributary.channel.group.FileGroupManager.OPTION_GROUP_PERSIST_PERIOD_SECOND;
import static org.zicat.tributary.channel.group.FileGroupManager.createFileName;

/** FileChannelBuilder. */
public class FileChannelBuilder {

    private List<File> dirs;
    private long groupPersistPeriodSecond = OPTION_GROUP_PERSIST_PERIOD_SECOND.defaultValue();
    protected String topic;
    protected Long segmentSize;
    protected Integer blockSize;
    protected CompressionType compressionType;
    protected Set<String> consumerGroups;
    protected long flushPeriodMills = 1000;

    /**
     * set flush period .
     *
     * @param flushPeriodMills flushPeriodMills
     * @return this
     */
    public FileChannelBuilder flushPeriodMills(long flushPeriodMills) {
        this.flushPeriodMills = flushPeriodMills;
        return this;
    }

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
     * set compression type.
     *
     * @param compressionType compressionType
     * @return this
     */
    public FileChannelBuilder compressionType(CompressionType compressionType) {
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
    public FileChannelBuilder segmentSize(Long segmentSize) {
        this.segmentSize = segmentSize;
        return this;
    }

    /**
     * set block size.
     *
     * @param blockSize blockSize
     * @return this
     */
    public FileChannelBuilder blockSize(Integer blockSize) {
        this.blockSize = blockSize;
        return this;
    }

    /**
     * set topic.
     *
     * @param topic topic
     * @return this
     */
    public FileChannelBuilder topic(String topic) {
        this.topic = topic;
        return this;
    }

    /**
     * set consumer groups.
     *
     * @param consumerGroups consumerGroups
     * @return this
     */
    public FileChannelBuilder consumerGroups(Set<String> consumerGroups) {
        this.consumerGroups = consumerGroups;
        return this;
    }

    /**
     * build to channel.
     *
     * @return PartitionFileChannel
     */
    public DefaultChannel<FileChannel> build() throws IOException {

        return new DefaultChannel<>(
                new DefaultChannel.AbstractChannelArrayFactory<FileChannel>() {
                    @Override
                    public String topic() {
                        return topic;
                    }

                    @Override
                    public Set<String> groups() {
                        return consumerGroups;
                    }

                    @Override
                    public FileChannel[] create() throws IOException {
                        if (dirs == null || dirs.isEmpty()) {
                            throw new IllegalStateException("dir list is null or empty");
                        }
                        if (consumerGroups == null || consumerGroups.isEmpty()) {
                            throw new IllegalStateException(
                                    "file channel must has at least one consumer group");
                        }
                        final FileChannel[] fileChannels = new FileChannel[dirs.size()];
                        for (int i = 0; i < dirs.size(); i++) {
                            final File dir = dirs.get(i).getCanonicalFile();
                            if (!dir.exists() && !IOUtils.makeDir(dir)) {
                                throw new IllegalStateException(
                                        "try to create fail " + dir.getPath());
                            }
                            fileChannels[i] = createFileChannel(dir);
                        }
                        return fileChannels;
                    }
                },
                flushPeriodMills);
    }

    /**
     * create group manager.
     *
     * @param dir dir
     * @param topic topic
     * @param consumerGroups consumerGroups
     * @param groupPersistPeriodSecond groupPersistPeriodSecond
     * @return SingleGroupManager
     */
    private static AbstractChannel.MemoryGroupManagerFactory groupManagerFactory(
            File dir, String topic, Set<String> consumerGroups, long groupPersistPeriodSecond) {
        return () ->
                new FileGroupManager(
                        new File(dir, createFileName(topic)),
                        consumerGroups,
                        groupPersistPeriodSecond);
    }

    /**
     * file channel dir.
     *
     * @param dir dir
     * @return FileChannel
     */
    private FileChannel createFileChannel(File dir) {
        return new FileChannel(
                topic,
                groupManagerFactory(dir, topic, consumerGroups, groupPersistPeriodSecond),
                blockSize,
                segmentSize,
                compressionType,
                dir);
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
