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
import org.zicat.tributary.channel.Offset;
import org.zicat.tributary.channel.RecordsResultSet;
import org.zicat.tributary.common.ConfigOption;
import org.zicat.tributary.common.ConfigOptions;
import org.zicat.tributary.common.MemorySize;
import org.zicat.tributary.sink.SinkGroupConfig;
import org.zicat.tributary.sink.function.AbstractFunction;
import org.zicat.tributary.sink.function.Function;
import org.zicat.tributary.sink.function.FunctionFactory;

import java.io.IOException;
import java.time.Duration;
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

    public static final ConfigOption<MemorySize> OPTION_MAX_RETAIN_SIZE =
            ConfigOptions.key("partition.retain.max.bytes")
                    .memoryType()
                    .description("delete oldest segment if one partition lag over this param")
                    .defaultValue(null);

    public static final ConfigOption<Duration> OPTION_RETAIN_SIZE_CHECK_PERIOD =
            ConfigOptions.key("partition.group.commit.period")
                    .durationType()
                    .description("partition group commit period millisecond, default 30s")
                    .defaultValue(Duration.ofSeconds(30));

    protected final Long maxRetainSize;
    private Offset fetchOffset;

    public AbstractPartitionHandler(
            String groupId, Channel channel, int partitionId, SinkGroupConfig sinkGroupConfig) {
        super(groupId, channel, partitionId, sinkGroupConfig);
        this.fetchOffset = startOffset;
        this.maxRetainSize = parseMaxRetainSize(sinkGroupConfig);
    }

    @Override
    public void run() {

        final long idleTimeMillis = idleTimeMillis();
        while (true) {
            try {
                final RecordsResultSet result = poll(fetchOffset, idleTimeMillis);
                if (closed.get() && result.isEmpty()) {
                    break;
                }
                processRecords(result, idleTimeMillis);
                updateCommitOffsetWaterMark();
                fetchOffset = nextFetchOffset(result.nexOffset());
            } catch (InterruptedException interruptedException) {
                if (closed.get()) {
                    return;
                }
            } catch (Throwable e) {
                LOG.error("poll data failed.", e);
                if (closed.get()) {
                    break;
                }
                updateCommitOffsetWaterMark();
                fetchOffset = nextFetchOffset(null);
                // protect while true cause cpu high
                sleepQuietly(DEFAULT_WAIT_TIME_MILLIS);
            }
        }
    }

    /**
     * next fetch offset.
     *
     * @param nextOffset nextOffset
     * @return GroupOffset
     */
    private Offset nextFetchOffset(Offset nextOffset) {
        if (nextOffset == null || nextOffset.compareTo(commitOffsetWaterMark()) < 0) {
            return fetchOffset.skip2Target(commitOffsetWaterMark());
        } else {
            return nextOffset;
        }
    }

    /**
     * process records.
     *
     * @param result result
     * @param idleTimeMillis idleTimeMillis
     * @throws Throwable Throwable
     */
    private void processRecords(RecordsResultSet result, long idleTimeMillis) throws Throwable {
        if (!result.isEmpty()) {
            process(result.nexOffset(), result);
        } else if (idleTimeMillis > 0) {
            idleTrigger();
        }
    }

    /** update commit offset watermark. */
    public void updateCommitOffsetWaterMark() {
        final Offset oldWaterMark = commitOffsetWaterMark();
        final Offset newWaterMark = Offset.max(committableOffset(), oldWaterMark);
        final Offset skipWaterMark = skipGroupOffsetByMaxRetainSize(newWaterMark);
        if (skipWaterMark.compareTo(oldWaterMark) > 0) {
            commit(skipWaterMark);
        }
    }

    /** skip commit offset watermark by max retain size. */
    protected Offset skipGroupOffsetByMaxRetainSize(Offset offset) {

        if (maxRetainSize == null) {
            return offset;
        }

        Offset newOffset = offset;
        Offset preOffset = offset;
        while (true) {
            final long lag = lag(newOffset);
            if (lag > maxRetainSize) {
                preOffset = newOffset;
                newOffset = new Offset(newOffset.segmentId() + 1, 0L);
                continue;
            }
            if (lag <= 0) {
                newOffset = preOffset;
            }
            break;
        }

        if (preOffset == offset) {
            return offset;
        }

        LOG.warn("lag over {}, committed {}, skip target = {}", maxRetainSize, offset, newOffset);
        return newOffset;
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
        final MemorySize memorySize = sinkGroupConfig.get(OPTION_MAX_RETAIN_SIZE);
        return memorySize == null ? null : memorySize.getBytes();
    }

    @Override
    public final void close() throws IOException {
        try {
            // wait for function consumer data ending.
            super.close();
        } finally {
            closeCallback();
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
