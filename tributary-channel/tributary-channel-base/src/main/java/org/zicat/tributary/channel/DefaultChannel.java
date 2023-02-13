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

package org.zicat.tributary.channel;

import org.zicat.tributary.common.Factory;
import org.zicat.tributary.common.Functions;
import org.zicat.tributary.common.IOUtils;

import java.io.IOException;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/** DefaultChannel. */
public class DefaultChannel<C extends AbstractChannel<?>> implements Channel {

    protected final AbstractChannelArrayFactory<C> factory;
    protected final C[] channels;
    protected final String topic;
    protected final AtomicBoolean closed = new AtomicBoolean(false);
    protected final Set<String> groups;
    private Thread flushSegmentThread;
    private long flushPeriodMill;

    public DefaultChannel(
            AbstractChannelArrayFactory<C> factory, long flushPeriod, TimeUnit flushUnit)
            throws IOException {

        final C[] channels = factory.create();
        if (channels == null || channels.length == 0) {
            throw new IllegalArgumentException("channels is null or empty");
        }
        this.factory = factory;
        this.topic = channels[0].topic();
        this.groups = channels[0].groups();
        this.channels = channels;
        if (flushPeriod > 0) {
            flushPeriodMill = flushUnit.toMillis(flushPeriod);
            flushSegmentThread = new Thread(this::periodForceSegment, "segment_flush_thread");
            flushSegmentThread.start();
        }
    }

    /**
     * AbstractChannelArrayFactory.
     *
     * @param <C> type channel
     */
    public interface AbstractChannelArrayFactory<C extends AbstractChannel<?>>
            extends Factory<C[]> {

        @Override
        default void destroy(C[] cs) {
            for (C c : cs) {
                IOUtils.closeQuietly(c);
            }
        }
    }

    @Override
    public RecordsOffset getRecordsOffset(String groupId, int partition) {
        final C channel = getPartitionChannel(partition);
        return channel.getRecordsOffset(groupId);
    }

    @Override
    public void flush() throws IOException {
        for (C channel : channels) {
            channel.flush();
        }
    }

    @Override
    public void append(int partition, byte[] record, int offset, int length) throws IOException {
        final C channel = getPartitionChannel(partition);
        channel.append(record, offset, length);
    }

    @Override
    public RecordsResultSet poll(
            int partition, RecordsOffset recordsOffset, long time, TimeUnit unit)
            throws IOException, InterruptedException {
        final C channel = getPartitionChannel(partition);
        return channel.poll(recordsOffset, time, unit);
    }

    @Override
    public void commit(String groupId, int partition, RecordsOffset recordsOffset)
            throws IOException {
        final C channel = getPartitionChannel(partition);
        channel.commit(groupId, recordsOffset);
    }

    @Override
    public String topic() {
        return topic;
    }

    @Override
    public int partition() {
        return channels.length;
    }

    @Override
    public int activeSegment() {
        int segments = 0;
        for (C channel : channels) {
            segments += channel.activeSegment();
        }
        return segments;
    }

    @Override
    public long lag(int partition, RecordsOffset recordsOffset) {
        final C channel = getPartitionChannel(partition);
        return channel.lag(recordsOffset);
    }

    @Override
    public long lastSegmentId(int partition) {
        final C fileChannel = getPartitionChannel(partition);
        return fileChannel.lastSegmentId();
    }

    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            try {
                factory.destroy(channels);
            } finally {
                if (flushSegmentThread != null) {
                    flushSegmentThread.interrupt();
                }
            }
        }
    }

    @Override
    public long writeBytes() {
        int writeBytes = 0;
        for (C channel : channels) {
            writeBytes += channel.writeBytes();
        }
        return writeBytes;
    }

    @Override
    public long readBytes() {
        int readBytes = 0;
        for (C channel : channels) {
            readBytes += channel.readBytes();
        }
        return readBytes;
    }

    @Override
    public long pageCache() {
        int pageCache = 0;
        for (C channel : channels) {
            pageCache += channel.pageCache();
        }
        return pageCache;
    }

    @Override
    public Set<String> groups() {
        return groups;
    }

    /**
     * check partition valid.
     *
     * @param partition partition
     */
    private C getPartitionChannel(int partition) {
        if (partition >= channels.length) {
            throw new IllegalArgumentException(
                    "partition over, partition is " + partition + ", size is " + channels.length);
        }
        return channels[partition];
    }

    /** force segment to dish. */
    protected void periodForceSegment() {

        Functions.loopCloseableFunction(
                t -> {
                    boolean success = true;
                    for (C channel : channels) {
                        success = success && channel.flushQuietly();
                    }
                    return success;
                },
                flushPeriodMill,
                closed);
    }
}
