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

import org.zicat.tributary.common.IOUtils;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/** AbstractChannel. */
public abstract class AbstractPartitionChannel<C extends OnePartitionChannel> implements Channel {

    protected final List<C> channels;
    protected final String topic;
    protected final AtomicBoolean closed = new AtomicBoolean(false);
    protected final Set<String> groups;

    public AbstractPartitionChannel(List<C> channels) {
        this.topic = channels.get(0).topic();
        this.groups = channels.get(0).groups();
        this.channels = Collections.unmodifiableList(channels);
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
        return channels.size();
    }

    @Override
    public int activeSegment() {
        return channels.stream().mapToInt(Channel::activeSegment).sum();
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
            for (Channel channel : channels) {
                IOUtils.closeQuietly(channel);
            }
            closeCallback();
        }
    }

    /** callback once when close. */
    public abstract void closeCallback();

    @Override
    public long writeBytes() {
        return channels.stream().mapToLong(Channel::writeBytes).sum();
    }

    @Override
    public long readBytes() {
        return channels.stream().mapToLong(Channel::readBytes).sum();
    }

    @Override
    public long pageCache() {
        return channels.stream().mapToLong(Channel::pageCache).sum();
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
        if (partition >= channels.size()) {
            throw new IllegalArgumentException(
                    "partition out of range, max partition "
                            + (channels.size() - 1)
                            + " input partition "
                            + partition);
        }
        return channels.get(partition);
    }
}
