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
import static org.zicat.tributary.common.Threads.interruptQuietly;
import static org.zicat.tributary.common.Threads.joinQuietly;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/** DefaultChannel. */
public class DefaultChannel<C extends AbstractSingleChannel<?>> implements Channel {

    private static final String THREAD_NAME_SEGMENT_FLUSH = "segment_flush_thread";
    private static final String THREAD_NAME_CLEANUP_EXPIRED_SEGMENT =
            "cleanup_expired_segment_thread";
    public static final String LABEL_PARTITION = "partition";
    protected final ChannelArrayFactory<C> factory;
    protected final C[] channels;
    protected final AtomicBoolean closed = new AtomicBoolean(false);
    private final Thread flushSegmentThread;
    private final Thread cleanupExpiredSegmentThread;
    private final PercentSize capacityPercent;

    public DefaultChannel(
            ChannelArrayFactory<C> factory,
            long flushPeriodMills,
            long cleanupExpiredSegmentPeriodMills,
            PercentSize capacityPercent)
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
        this.capacityPercent = capacityPercent;
        this.flushSegmentThread =
                startLoopRunningThread(
                        closed, THREAD_NAME_SEGMENT_FLUSH, this::flushQuietly, flushPeriodMills);
        this.cleanupExpiredSegmentThread =
                startLoopRunningThread(
                        closed,
                        THREAD_NAME_CLEANUP_EXPIRED_SEGMENT,
                        this::cleanUpExpiredSegmentsQuietly,
                        cleanupExpiredSegmentPeriodMills);
    }

    /**
     * start loop running thread.
     *
     * @param closed closed
     * @param name name
     * @param runnable runnable
     * @param period period
     * @return Thread
     */
    private static Thread startLoopRunningThread(
            AtomicBoolean closed, String name, Runnable runnable, long period) {
        final Runnable task = () -> Functions.loopCloseableFunction(runnable, period, closed);
        final Thread thread = new Thread(task, name);
        thread.start();
        return thread;
    }

    @Override
    public Map<MetricKey, Double> gaugeFamily() {
        final Map<MetricKey, Double> result = new HashMap<>();
        for (int i = 0; i < channels.length; i++) {
            final Channel c = channels[i];
            for (Map.Entry<MetricKey, Double> entry : c.gaugeFamily().entrySet()) {
                final MetricKey newKey = entry.getKey().addLabel(LABEL_PARTITION, i);
                result.merge(newKey, entry.getValue(), Double::sum);
            }
        }
        return result;
    }

    @Override
    public Map<MetricKey, Double> counterFamily() {
        final Map<MetricKey, Double> result = new HashMap<>();
        for (int i = 0; i < channels.length; i++) {
            final Channel c = channels[i];
            for (Map.Entry<MetricKey, Double> entry : c.counterFamily().entrySet()) {
                final MetricKey newKey = entry.getKey().addLabel(LABEL_PARTITION, i);
                result.merge(newKey, entry.getValue(), Double::sum);
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
    public void commit(int partition, Offset offset) throws IOException {
        getPartitionChannel(partition).commit(offset);
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
                interruptQuietly(flushSegmentThread, cleanupExpiredSegmentThread);
                joinQuietly(flushSegmentThread, cleanupExpiredSegmentThread);
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
     * ChannelArrayFactory.
     *
     * @param <C> type channel
     */
    public interface ChannelArrayFactory<C extends AbstractSingleChannel<?>> extends Factory<C[]> {

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
    }

    /**
     * AbstractChannelArrayFactory.
     *
     * @param <C> C
     */
    public abstract static class AbstractChannelArrayFactory<C extends AbstractSingleChannel<?>>
            implements ChannelArrayFactory<C> {
        private final String topic;
        private final Set<String> groups;

        public AbstractChannelArrayFactory(String topic, Set<String> groups) {
            this.topic = topic;
            this.groups = groups;
        }

        @Override
        public Set<String> groups() {
            return groups;
        }

        @Override
        public String topic() {
            return topic;
        }

        @Override
        public void destroy(C[] cs) {
            IOUtils.concurrentCloseQuietly(cs);
        }
    }

    /** clean up expired segments quietly for test. */
    public void cleanUpExpiredSegmentsQuietly() {
        final boolean[] exceedChannel = new boolean[channels.length];
        // for some channel like memory, clean up segment may not free memory immediately
        int loopCount = 0;
        do {
            for (int i = 0; i < channels.length; i++) {
                final C channel = channels[i];
                channel.cleanUpExpiredSegmentsQuietly();
                try {
                    if (channel.checkCapacityExceed(capacityPercent)
                            && channel.cleanUpEarliestSegment()) {
                        exceedChannel[i] = channel.checkCapacityExceed(capacityPercent);
                    } else {
                        exceedChannel[i] = false;
                    }
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
            loopCount++;
        } while (Booleans.hasTrue(exceedChannel) && loopCount < channels.length);
    }

    /** flush segment quietly. */
    public void flushQuietly() {
        for (C channel : channels) {
            try {
                channel.flush();
            } catch (Throwable ignore) {
            }
        }
    }

    /**
     * get flush segment thread for test.
     *
     * @return thread
     */
    public Thread getFlushSegmentThread() {
        return flushSegmentThread;
    }

    /**
     * get cleanup expired segment thread for test.
     *
     * @return thread
     */
    public Thread getCleanupExpiredSegmentThread() {
        return cleanupExpiredSegmentThread;
    }
}
