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

import org.zicat.tributary.common.*;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/** DefaultChannel. */
public class DefaultChannel<C extends AbstractChannel<?>> implements Channel {

    protected final AbstractChannelArrayFactory<C> factory;
    protected final C[] channels;
    protected final AtomicBoolean closed = new AtomicBoolean(false);
    private final Thread flushSegmentThread;

    public DefaultChannel(AbstractChannelArrayFactory<C> factory, long flushPeriodMills)
            throws IOException {
        final C[] channels = factory.create();
        if (channels == null || channels.length == 0) {
            throw new IllegalArgumentException("channels is null or empty");
        }
        if (flushPeriodMills <= 0) {
            throw new IllegalArgumentException("flush period is less than 0");
        }
        this.factory = factory;
        this.channels = channels;
        this.flushSegmentThread = startFlushSegmentThread(channels, closed, flushPeriodMills);
    }

    /**
     * start flush segment thread.
     *
     * @param channels channels
     * @param closed closed
     * @param flushPeriodMillis flushPeriodMillis
     * @param <C> channel
     * @return Thread
     */
    private static <C extends AbstractChannel<?>> Thread startFlushSegmentThread(
            C[] channels, AtomicBoolean closed, long flushPeriodMillis) {
        final Runnable task =
                () ->
                        Functions.loopCloseableFunction(
                                t -> {
                                    boolean success = true;
                                    for (C channel : channels) {
                                        if (channel.flushIdleMillis() >= flushPeriodMillis) {
                                            success = success && channel.flushQuietly();
                                        }
                                    }
                                    return success;
                                },
                                flushPeriodMillis,
                                closed);
        final Thread thread = new Thread(task, "segment_flush_thread");
        thread.start();
        return thread;
    }

    @Override
    public Map<GaugeKey, GaugeFamily> gaugeFamily() {
        final Map<GaugeKey, GaugeFamily> result = new HashMap<>();
        for (Channel c : channels) {
            for (Map.Entry<GaugeKey, GaugeFamily> entry : c.gaugeFamily().entrySet()) {
                final GaugeFamily cache = result.get(entry.getKey());
                if (cache == null) {
                    result.put(entry.getKey(), entry.getValue());
                } else {
                    result.put(entry.getKey(), cache.merge(entry.getValue()));
                }
            }
        }
        return result;
    }

    @Override
    public Offset committedOffset(String groupId, int partition) {
        return getPartitionChannel(partition).committedOffset(groupId);
    }

    @Override
    public void flush() throws IOException {
        for (C channel : channels) {
            channel.flush();
        }
    }

    @Override
    public void append(int partition, ByteBuffer byteBuffer)
            throws IOException, InterruptedException {
        getPartitionChannel(partition).append(byteBuffer);
    }

    @Override
    public RecordsResultSet poll(int partition, Offset offset, long time, TimeUnit unit)
            throws IOException, InterruptedException {
        return getPartitionChannel(partition).poll(offset, time, unit);
    }

    @Override
    public void commit(int partition, String groupId, Offset offset) throws IOException {
        getPartitionChannel(partition).commit(groupId, offset);
    }

    @Override
    public String topic() {
        return factory.topic();
    }

    @Override
    public int partition() {
        return channels.length;
    }

    @Override
    public long lag(int partition, Offset offset) {
        return getPartitionChannel(partition).lag(offset);
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
    public Set<String> groups() {
        return Collections.unmodifiableSet(factory.groups());
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

    /**
     * AbstractChannelArrayFactory.
     *
     * @param <C> type channel
     */
    public interface AbstractChannelArrayFactory<C extends AbstractChannel<?>>
            extends Factory<C[]> {

        /**
         * return topic.
         *
         * @return string
         */
        String topic();

        /**
         * get groups.
         *
         * @return group set
         */
        Set<String> groups();

        @Override
        default void destroy(C[] cs) {
            IOUtils.concurrentCloseQuietly(cs);
        }
    }
}
