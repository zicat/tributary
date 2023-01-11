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

package org.zicat.tributary.queue.file;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zicat.tributary.queue.CompressionType;
import org.zicat.tributary.queue.LogQueue;
import org.zicat.tributary.queue.RecordsOffset;
import org.zicat.tributary.queue.RecordsResultSet;
import org.zicat.tributary.queue.utils.IOUtils;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.zicat.tributary.queue.utils.Functions.loopCloseableFunction;

/**
 * PartitionFileLogQueue implements {@link LogQueue} to Storage records by wrapper multi {@link
 * FileLogQueue}. Each {@link FileLogQueue} store one partition data
 *
 * <p>All public methods in PartitionFileLogQueue are @ThreadSafe.
 */
public class PartitionFileLogQueue implements LogQueue, Closeable {

    private static final Logger LOG = LoggerFactory.getLogger(PartitionFileLogQueue.class);
    private final FileLogQueue[] logQueues;
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private Thread cleanUpThread;
    private Thread flushSegmentThread;
    private long cleanupMill;
    private long flushPeriodMill;
    private final boolean flushForce;
    private final String topic;

    protected PartitionFileLogQueue(
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
        logQueues = new FileLogQueue[dirs.size()];
        FileLogQueueBuilder builder = FileLogQueueBuilder.newBuilder();

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
            FileLogQueue logQueue = builder.dir(dirs.get(i)).build();
            logQueues[i] = logQueue;
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
                        for (FileLogQueue logQueue : logQueues) {
                            logQueue.flush(flushForce);
                        }
                    } catch (Exception e) {
                        LOG.warn("period flush log queue error", e);
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
     * cleanup all log queue.
     *
     * @return null
     */
    private boolean cleanUpAll() {
        boolean cleanUp = false;
        for (FileLogQueue logQueue : logQueues) {
            cleanUp |= logQueue.cleanUp();
        }
        return cleanUp;
    }

    @Override
    public void append(int partition, byte[] record, int offset, int length) throws IOException {
        final FileLogQueue queue = getPartitionQueue(partition);
        queue.append(record, offset, length);
    }

    @Override
    public RecordsResultSet poll(
            int partition, RecordsOffset recordsOffset, long time, TimeUnit unit)
            throws IOException, InterruptedException {
        final FileLogQueue queue = getPartitionQueue(partition);
        return queue.poll(recordsOffset, time, unit);
    }

    @Override
    public RecordsOffset getRecordsOffset(String groupId, int partition) {
        final FileLogQueue queue = getPartitionQueue(partition);
        return queue.getRecordsOffset(groupId);
    }

    @Override
    public void commit(String groupId, int partition, RecordsOffset recordsOffset)
            throws IOException {
        final FileLogQueue queue = getPartitionQueue(partition);
        queue.commit(groupId, recordsOffset);
    }

    @Override
    public RecordsOffset getMinRecordsOffset(int partition) {
        final FileLogQueue queue = getPartitionQueue(partition);
        return queue.getMinRecordsOffset();
    }

    @Override
    public long lag(int partition, RecordsOffset recordsOffset) {
        final FileLogQueue queue = getPartitionQueue(partition);
        return queue.lag(recordsOffset);
    }

    @Override
    public long lastSegmentId(int partition) {
        final FileLogQueue queue = getPartitionQueue(partition);
        return queue.lastSegmentId();
    }

    /**
     * check partition valid.
     *
     * @param partition partition
     */
    private FileLogQueue getPartitionQueue(int partition) {
        if (partition >= logQueues.length) {
            throw new IllegalArgumentException(
                    "partition out of range, max partition "
                            + (logQueues.length - 1)
                            + " input partition "
                            + partition);
        }
        return logQueues[partition];
    }

    @Override
    public void flush() throws IOException {
        for (FileLogQueue logQueue : logQueues) {
            logQueue.flush();
        }
    }

    @Override
    public String topic() {
        return topic;
    }

    @Override
    public int partition() {
        return logQueues.length;
    }

    @Override
    public int activeSegment() {
        return Arrays.stream(logQueues).mapToInt(LogQueue::activeSegment).sum();
    }

    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            for (LogQueue logQueue : logQueues) {
                IOUtils.closeQuietly(logQueue);
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
        return Arrays.stream(logQueues).mapToLong(LogQueue::writeBytes).sum();
    }

    @Override
    public long readBytes() {
        return Arrays.stream(logQueues).mapToLong(LogQueue::readBytes).sum();
    }

    @Override
    public long pageCache() {
        return Arrays.stream(logQueues).mapToLong(LogQueue::pageCache).sum();
    }
}
