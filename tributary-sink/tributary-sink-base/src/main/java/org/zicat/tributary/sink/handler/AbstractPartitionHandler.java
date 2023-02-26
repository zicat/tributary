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
import org.zicat.tributary.channel.GroupOffset;
import org.zicat.tributary.channel.RecordsResultSet;
import org.zicat.tributary.common.ConfigOption;
import org.zicat.tributary.common.ConfigOptions;
import org.zicat.tributary.sink.SinkGroupConfig;
import org.zicat.tributary.sink.function.AbstractFunction;
import org.zicat.tributary.sink.function.Function;
import org.zicat.tributary.sink.function.FunctionFactory;

import java.io.IOException;
import java.util.List;

import static org.zicat.tributary.common.Threads.sleepQuietly;

/**
 * AbstractPartitionHandler.
 *
 * <p>Each {@link AbstractPartitionHandler} bind one thread and consumer one topic partition data by
 * group Id.
 *
 * <p>{@link AbstractPartitionHandler} not support to consumer multi partition data.
 *
 * <p>{@link AbstractPartitionHandler} create at least one {@link Function} instance by {@link
 * FunctionFactory#create()}.
 *
 * <p>The {@link Function} instance count depends on {@link AbstractPartitionHandler} implements.
 */
public abstract class AbstractPartitionHandler extends PartitionHandler {

    private static final Logger LOG = LoggerFactory.getLogger(AbstractPartitionHandler.class);

    public static final ConfigOption<Long> OPTION_MAX_RETAIN_SIZE =
            ConfigOptions.key("maxRetainPerPartitionBytes")
                    .longType()
                    .description("delete oldest segment if one partition lag over this param")
                    .defaultValue(null);

    public static final ConfigOption<Integer> OPTION_RETAIN_SIZE_CHECK_PERIOD_MILLI =
            ConfigOptions.key("retainPerPartitionCheckPeriodMilli")
                    .integerType()
                    .description("check retain thread check period, default 30s")
                    .defaultValue(30 * 1000);

    protected final Long maxRetainSize;
    private volatile GroupOffset fetchOffset;

    public AbstractPartitionHandler(
            String groupId, Channel channel, int partitionId, SinkGroupConfig sinkGroupConfig) {
        super(groupId, channel, partitionId, sinkGroupConfig);
        this.fetchOffset = startOffset;
        this.maxRetainSize = parseMaxRetainSize(sinkGroupConfig);
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
                final GroupOffset nextOffset = result.nexGroupOffset();
                if (!result.isEmpty()) {
                    process(nextOffset, result);
                    updateCommitOffsetWaterMark();
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
        return super.poll(partitionId, fetchOffset, idleTimeMillis);
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
        idleTrigger();
        updateCommitOffsetWaterMark();
    }

    /** commit. */
    public synchronized void updateCommitOffsetWaterMark() {
        final GroupOffset oldWaterMark = commitOffsetWaterMark();
        final GroupOffset newWaterMark = GroupOffset.max(committableOffset(), oldWaterMark);
        final GroupOffset skipWaterMark = skipGroupOffsetByMaxRetainSize(newWaterMark);
        if (skipWaterMark.compareTo(oldWaterMark) > 0) {
            commit(skipWaterMark);
            checkFetchOffset();
        }
    }

    /**
     * rollback fetch offset.
     *
     * @return GroupOffset
     */
    private GroupOffset rollbackFetchOffset() {
        updateCommitOffsetWaterMark();
        return fetchOffset.skip2Target(commitOffsetWaterMark());
    }

    /** check fetch offset. fetch offset must over commit offset watermark. */
    private void checkFetchOffset() {
        final GroupOffset commitOffsetWaterMark = commitOffsetWaterMark();
        if (fetchOffset.compareTo(commitOffsetWaterMark) < 0) {
            //noinspection NonAtomicOperationOnVolatileField
            fetchOffset = fetchOffset.skip2Target(commitOffsetWaterMark);
        }
    }

    /** skip commit offset watermark by max retain size. */
    protected GroupOffset skipGroupOffsetByMaxRetainSize(GroupOffset groupOffset) {
        if (maxRetainSize == null) {
            return groupOffset;
        }
        GroupOffset newGroupOffset = groupOffset;
        GroupOffset preGroupOffset = groupOffset;
        while (true) {
            final long lag = lag(newGroupOffset);
            if (lag > maxRetainSize) {
                preGroupOffset = newGroupOffset;
                newGroupOffset = newGroupOffset.skipNextSegmentHead();
                continue;
            }
            if (lag <= 0) {
                newGroupOffset = preGroupOffset;
            }
            break;
        }

        if (preGroupOffset == groupOffset) {
            return groupOffset;
        }

        LOG.warn(
                "group {}, partition {}, lag over {}, current committed segment id = {}, skip to segment id = {}",
                groupId,
                partitionId,
                maxRetainSize,
                groupOffset.segmentId(),
                newGroupOffset.segmentId());
        return newGroupOffset;
    }

    /**
     * get partition lag.
     *
     * @return lag
     */
    public final long lag() {
        return lag(fetchOffset);
    }

    /**
     * parse max retain size.
     *
     * @param sinkGroupConfig sinkGroupConfig
     * @return value
     */
    public static Long parseMaxRetainSize(SinkGroupConfig sinkGroupConfig) {
        return sinkGroupConfig.get(OPTION_MAX_RETAIN_SIZE);
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
}
