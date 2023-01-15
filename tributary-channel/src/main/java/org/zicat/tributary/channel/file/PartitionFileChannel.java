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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zicat.tributary.channel.Channel;
import org.zicat.tributary.channel.CompressionType;
import org.zicat.tributary.channel.RecordsOffset;
import org.zicat.tributary.channel.RecordsResultSet;
import org.zicat.tributary.channel.utils.IOUtils;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.zicat.tributary.channel.utils.Functions.loopCloseableFunction;

/**
 * PartitionFileChannel implements {@link Channel} to Storage records by wrapper multi {@link
 * FileChannel}. Each {@link FileChannel} store one partition data
 *
 * <p>All public methods in PartitionFileChannel are @ThreadSafe.
 */
public class PartitionFileChannel implements Channel, Closeable {

    private static final Logger LOG = LoggerFactory.getLogger(PartitionFileChannel.class);
    private final FileChannel[] fileChannels;
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private Thread cleanUpThread;
    private Thread flushSegmentThread;
    private long cleanupMill;
    private long flushPeriodMill;
    private final boolean flushForce;
    private final String topic;
    private final Set<String> groups;

    protected PartitionFileChannel(
            String topic,
            List<String> consumerGroups,
            List<File> dirs,
            Integer blockSize,
            Long segmentSize,
            CompressionType compressionType,
            long cleanUpPeriod,
            TimeUnit cleanUpUnit,
            long flushPeriod,
            TimeUnit flushUnit,
            long flushPageSize,
            boolean flushForce) {

        this.topic = topic;
        this.flushForce = flushForce;
        this.groups = new HashSet<>(consumerGroups);
        fileChannels = new FileChannel[dirs.size()];
        FileChannelBuilder builder = FileChannelBuilder.newBuilder();

        builder.segmentSize(segmentSize)
                .blockSize(blockSize)
                .compressionType(compressionType)
                .flushPeriod(0, flushUnit)
                .flushPageCacheSize(flushPageSize)
                .topic(topic)
                .consumerGroups(consumerGroups)
                .flushForce(flushForce)
                .cleanUpPeriod(0, cleanUpUnit);
        for (int i = 0; i < dirs.size(); i++) {
            FileChannel fileChannel = builder.dir(dirs.get(i)).build();
            fileChannels[i] = fileChannel;
        }

        if (cleanUpPeriod > 0) {
            cleanupMill = cleanUpUnit.toMillis(cleanUpPeriod);
            cleanUpThread = new Thread(this::cleanUp, "cleanup_segment_thread");
            cleanUpThread.start();
        }

        if (flushPeriod > 0) {
            flushPeriodMill = flushUnit.toMillis(flushPeriod);
            flushSegmentThread = new Thread(this::periodForceSegment, "segment_flush_thread");
            flushSegmentThread.start();
        }
    }

    /** force segment to dish. */
    protected void periodForceSegment() {
        loopCloseableFunction(
                t -> {
                    try {
                        for (FileChannel fileChannel : fileChannels) {
                            fileChannel.flush(flushForce);
                        }
                    } catch (Exception e) {
                        LOG.warn("period flush file channel error", e);
                    }
                    return null;
                },
                flushPeriodMill,
                closed);
    }

    /** clean up task. */
    protected void cleanUp() {
        loopCloseableFunction(t -> cleanUpAll(), cleanupMill, closed);
    }

    /**
     * cleanup all file channel.
     *
     * @return null
     */
    private boolean cleanUpAll() {
        boolean cleanUp = false;
        for (FileChannel fileChannel : fileChannels) {
            cleanUp |= fileChannel.cleanUp();
        }
        return cleanUp;
    }

    @Override
    public void append(int partition, byte[] record, int offset, int length) throws IOException {
        final FileChannel fileChannel = getPartitionChannel(partition);
        fileChannel.append(record, offset, length);
    }

    @Override
    public RecordsResultSet poll(
            int partition, RecordsOffset recordsOffset, long time, TimeUnit unit)
            throws IOException, InterruptedException {
        final FileChannel fileChannel = getPartitionChannel(partition);
        return fileChannel.poll(recordsOffset, time, unit);
    }

    @Override
    public RecordsOffset getRecordsOffset(String groupId, int partition) {
        final FileChannel fileChannel = getPartitionChannel(partition);
        return fileChannel.getRecordsOffset(groupId);
    }

    @Override
    public void commit(String groupId, int partition, RecordsOffset recordsOffset)
            throws IOException {
        final FileChannel fileChannel = getPartitionChannel(partition);
        fileChannel.commit(groupId, recordsOffset);
    }

    @Override
    public RecordsOffset getMinRecordsOffset(int partition) {
        final FileChannel fileChannel = getPartitionChannel(partition);
        return fileChannel.getMinRecordsOffset();
    }

    @Override
    public long lag(int partition, RecordsOffset recordsOffset) {
        final FileChannel fileChannel = getPartitionChannel(partition);
        return fileChannel.lag(recordsOffset);
    }

    @Override
    public long lastSegmentId(int partition) {
        final FileChannel fileChannel = getPartitionChannel(partition);
        return fileChannel.lastSegmentId();
    }

    /**
     * check partition valid.
     *
     * @param partition partition
     */
    private FileChannel getPartitionChannel(int partition) {
        if (partition >= fileChannels.length) {
            throw new IllegalArgumentException(
                    "partition out of range, max partition "
                            + (fileChannels.length - 1)
                            + " input partition "
                            + partition);
        }
        return fileChannels[partition];
    }

    @Override
    public void flush() throws IOException {
        for (FileChannel fileChannel : fileChannels) {
            fileChannel.flush();
        }
    }

    @Override
    public String topic() {
        return topic;
    }

    @Override
    public Set<String> groups() {
        return groups;
    }

    @Override
    public int partition() {
        return fileChannels.length;
    }

    @Override
    public int activeSegment() {
        return Arrays.stream(fileChannels).mapToInt(Channel::activeSegment).sum();
    }

    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            for (Channel channel : fileChannels) {
                IOUtils.closeQuietly(channel);
            }
            if (cleanUpThread != null) {
                cleanUpThread.interrupt();
            }
            if (flushSegmentThread != null) {
                flushSegmentThread.interrupt();
            }
        }
    }

    @Override
    public long writeBytes() {
        return Arrays.stream(fileChannels).mapToLong(Channel::writeBytes).sum();
    }

    @Override
    public long readBytes() {
        return Arrays.stream(fileChannels).mapToLong(Channel::readBytes).sum();
    }

    @Override
    public long pageCache() {
        return Arrays.stream(fileChannels).mapToLong(Channel::pageCache).sum();
    }
}
