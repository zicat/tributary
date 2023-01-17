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

package org.zicat.tributary.sink.handler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zicat.tributary.channel.Channel;
import org.zicat.tributary.channel.RecordsOffset;
import org.zicat.tributary.channel.RecordsResultSet;
import org.zicat.tributary.sink.SinkGroupConfig;
import org.zicat.tributary.sink.function.AbstractFunction;
import org.zicat.tributary.sink.function.Clock;
import org.zicat.tributary.sink.function.Function;
import org.zicat.tributary.sink.function.FunctionFactory;

import java.io.IOException;
import java.util.List;

import static org.zicat.tributary.sink.utils.Threads.sleepQuietly;

/**
 * AbstractPartitionHandler.
 *
 * <p>Each {@link AbstractPartitionHandler} bind one thread and consumer one topic partition data by
 * group Id.
 *
 * <p>{@link AbstractPartitionHandler} not support to consumer multi partition data.
 *
 * <p>{@link AbstractPartitionHandler} create at least one {@link Function} instance by {@link
 * FunctionFactory#createFunction()}.
 *
 * <p>The {@link Function} instance count depends on {@link AbstractPartitionHandler} implements.
 */
public abstract class AbstractPartitionHandler extends PartitionHandler {

    private static final Logger LOG = LoggerFactory.getLogger(AbstractPartitionHandler.class);
    public static final String KEY_MAX_RETAIN_SIZE = "maxRetainPerPartitionBytes";
    public static final String DEFAULT_MAX_RETAIN_SIZE = "";

    public static final String KEY_RETAIN_SIZE_CHECK_PERIOD_MILLI =
            "retainPerPartitionCheckPeriodMilli";
    public static final int DEFAULT_RETAIN_SIZE_CHECK_PERIOD_MILLI = 30 * 1000;

    private static final long DEFAULT_MIN_WAIT_TIME = 500;

    protected final Long maxRetainSize;
    protected final Clock clock;

    private volatile RecordsOffset fetchOffset;
    private RecordsOffset commitOffsetWaterMark;
    private long preTriggerMillis;

    public AbstractPartitionHandler(
            String groupId, Channel channel, int partitionId, SinkGroupConfig sinkGroupConfig) {

        super(groupId, channel, partitionId, sinkGroupConfig);
        this.commitOffsetWaterMark = startOffset;
        this.fetchOffset = startOffset;
        this.maxRetainSize = parseMaxRetainSize(sinkGroupConfig);
        this.clock = sinkGroupConfig.getOrCreateDefaultClock();
        this.preTriggerMillis = clock.currentTimeMillis();
    }

    @Override
    public void run() {

        while (true) {
            try {
                final long idleTimeMillis = idleTimeMillis();
                final RecordsResultSet result = poll(partitionId, idleTimeMillis);
                if (closed.get() && result.isEmpty()) {
                    break;
                }
                final RecordsOffset nextOffset = result.nexRecordsOffset();
                if (!result.isEmpty()) {
                    process(nextOffset, result);
                    commit();
                } else {
                    processIdleTrigger(idleTimeMillis);
                }
                fetchOffset = nextOffset;
            } catch (Throwable e) {
                LOG.error("poll data failed.", e);
                if (closed.get()) {
                    break;
                }
                fetchOffset = rollbackFetchOffset();
                // protect while true cause cpu high
                sleepQuietly(DEFAULT_WAIT_TIME_MILLIS);
            }
        }
    }

    /**
     * sync poll.
     *
     * @param partitionId partitionId
     * @param idleTimeMillis idleTimeMillis
     * @return RecordsResultSet
     * @throws IOException IOException
     * @throws InterruptedException InterruptedException
     */
    private synchronized RecordsResultSet poll(int partitionId, long idleTimeMillis)
            throws IOException, InterruptedException {
        checkFetchOffset();
        return super.poll(
                partitionId, fetchOffset, Math.min(DEFAULT_MIN_WAIT_TIME, idleTimeMillis));
    }

    /**
     * process idle trigger.
     *
     * @throws Throwable Throwable
     */
    private void processIdleTrigger(long idleTimeMillis) throws Throwable {
        if (idleTimeMillis <= 0) {
            return;
        }
        final long current = clock.currentTimeMillis();
        if (current - preTriggerMillis <= idleTimeMillis) {
            return;
        }
        try {
            idleTrigger();
            commit();
        } finally {
            preTriggerMillis = current;
        }
    }

    /** commit. */
    public synchronized void commit() {
        if (updateCommitOffsetWaterMark()) {
            commit(commitOffsetWaterMark);
        }
    }

    /** update commit offset watermark. */
    private boolean updateCommitOffsetWaterMark() {
        final RecordsOffset oldCommitOffsetWaterMark = commitOffsetWaterMark;
        commitOffsetWaterMark = RecordsOffset.max(committableOffset(), commitOffsetWaterMark);
        skipCommitOffsetWaterMarkByMaxRetainSize();
        checkFetchOffset();
        return this.commitOffsetWaterMark != oldCommitOffsetWaterMark;
    }

    /**
     * rollback fetch offset.
     *
     * @return RecordsOffset
     */
    private RecordsOffset rollbackFetchOffset() {
        commit();
        return fetchOffset.skip2Target(commitOffsetWaterMark);
    }

    /** check fetch offset. fetch offset must over commit offset watermark. */
    @SuppressWarnings("NonAtomicOperationOnVolatileField")
    private void checkFetchOffset() {
        if (fetchOffset.compareTo(commitOffsetWaterMark) < 0) {
            fetchOffset = fetchOffset.skip2Target(commitOffsetWaterMark);
        }
    }

    /** skip commit offset watermark by max retain size. */
    protected void skipCommitOffsetWaterMarkByMaxRetainSize() {

        RecordsOffset newRecordsOffset = this.commitOffsetWaterMark;
        while (maxRetainSize != null
                && newRecordsOffset.segmentId() < channel.lastSegmentId(partitionId)
                && channel.lag(partitionId, newRecordsOffset) > maxRetainSize) {
            newRecordsOffset = newRecordsOffset.skipNextSegmentHead();
        }

        if (newRecordsOffset == this.commitOffsetWaterMark) {
            return;
        }

        LOG.warn(
                "group {}, partition {}, lag over {}, current committed segment id = {}, skip to segment id = {}",
                groupId,
                partitionId,
                maxRetainSize,
                commitOffsetWaterMark.segmentId(),
                newRecordsOffset.segmentId());
        this.commitOffsetWaterMark = newRecordsOffset;
    }

    /**
     * get partition lag.
     *
     * @return lag
     */
    public final long lag() {
        return channel.lag(partitionId, fetchOffset);
    }

    /**
     * parse max retain size.
     *
     * @param sinkGroupConfig sinkGroupConfig
     * @return value
     */
    public static Long parseMaxRetainSize(SinkGroupConfig sinkGroupConfig) {
        final Object maxRetainSize = sinkGroupConfig.getCustomProperty(KEY_MAX_RETAIN_SIZE);
        if (maxRetainSize != null) {
            LOG.info("param {} value = {}", KEY_MAX_RETAIN_SIZE, maxRetainSize);
            return (Long) maxRetainSize;
        }
        return null;
    }

    @Override
    public final void close() throws IOException {
        try {
            // wait for function consumer data ending.
            super.close();
        } finally {
            closeCallback();
            // commit fetch offset
            commit(fetchOffset);
        }
    }

    /** callback close. */
    public abstract void closeCallback() throws IOException;

    /**
     * get all functions.
     *
     * @return function list
     */
    public abstract List<AbstractFunction> getFunctions();

    /**
     * get commit offset watermark. for unit test visitable.
     *
     * @return RecordsOffset
     */
    public RecordsOffset commitOffsetWaterMark() {
        return commitOffsetWaterMark;
    }
}
