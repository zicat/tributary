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

import io.prometheus.client.Gauge;
import static org.zicat.tributary.common.Threads.joinQuietly;
import static org.zicat.tributary.common.Threads.sleepQuietly;
import static org.zicat.tributary.sink.function.FunctionFactory.findFunctionFactory;
import static org.zicat.tributary.sink.handler.DefaultPartitionHandlerFactory.parseMaxRetainSize;
import static org.zicat.tributary.sink.handler.DefaultPartitionHandlerFactory.snapshotIntervalMills;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zicat.tributary.channel.Channel;
import org.zicat.tributary.channel.Offset;
import org.zicat.tributary.channel.RecordsResultSet;
import org.zicat.tributary.common.ConfigOption;
import org.zicat.tributary.common.ConfigOptions;
import org.zicat.tributary.common.IOUtils;
import org.zicat.tributary.sink.SinkGroupConfig;
import org.zicat.tributary.sink.function.*;
import org.zicat.tributary.sink.utils.HostUtils;

import java.io.Closeable;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/** PartitionHandler. */
public abstract class PartitionHandler extends Thread implements Closeable, CheckpointedFunction {

    public static final ConfigOption<String> OPTION_METRICS_HOST =
            ConfigOptions.key("metricsHost")
                    .stringType()
                    .description("the metric host")
                    .defaultValue(HostUtils.getHostName());

    public static final ConfigOption<Clock> OPTION_PARTITION_HANDLER_CLOCK =
            ConfigOptions.key("_partition_handler_clock")
                    .<Clock>objectType()
                    .defaultValue(new SystemClock());

    private static final Gauge SINK_SNAPSHOT_COST =
            Gauge.build()
                    .name("sink_snapshot_cost")
                    .help("sink snapshot cost")
                    .labelNames("host", "topic", "groupId", "partition_id")
                    .register();

    protected static final Logger LOG = LoggerFactory.getLogger(PartitionHandler.class);
    protected static final long DEFAULT_WAIT_TIME_MILLIS = 500;
    protected static final long DEFAULT_SLEEP_WHEN_EXCEPTION = 1000;

    protected final String groupId;
    protected final Channel channel;
    protected final Integer partitionId;
    protected final FunctionFactory functionFactory;
    protected final SinkGroupConfig config;
    protected final Offset startOffset;
    protected final Long maxRetainSize;
    protected final AtomicBoolean closed = new AtomicBoolean();
    protected final long snapshotIntervalMills;
    protected final Clock clock;

    protected Offset fetchOffset;
    protected Offset committedOffset;
    protected long preSnapshotTime;
    protected Gauge.Child sinkSnapshotCost;

    public PartitionHandler(
            String groupId, Channel channel, int partitionId, SinkGroupConfig config) {
        this.groupId = groupId;
        this.channel = channel;
        this.partitionId = partitionId;
        this.snapshotIntervalMills = snapshotIntervalMills(config);
        this.config = config;
        this.clock = config.get(OPTION_PARTITION_HANDLER_CLOCK);
        this.preSnapshotTime = clock.currentTimeMillis();
        this.functionFactory = findFunctionFactory(config.functionIdentity());
        this.startOffset = channel.committedOffset(groupId, partitionId);
        this.maxRetainSize = parseMaxRetainSize(config);
        this.committedOffset = startOffset;
        this.fetchOffset = startOffset;
        this.sinkSnapshotCost =
                SINK_SNAPSHOT_COST.labels(
                        config.get(OPTION_METRICS_HOST),
                        channel.topic(),
                        groupId,
                        String.valueOf(partitionId));
        setName(threadName());
    }

    @Override
    public void run() {

        while (true) {
            try {
                final RecordsResultSet result = poll(fetchOffset);
                if (closed.get() && result.isEmpty()) {
                    break;
                }
                if (!result.isEmpty()) {
                    process(result.nexOffset(), result);
                }
                checkTriggerCheckpoint();
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
                updateAndCommitOffset();
                fetchOffset = nextFetchOffset(null);
                sleepWhenException();
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
        if (nextOffset == null || nextOffset.compareTo(committedOffset()) < 0) {
            return fetchOffset.skip2Target(committedOffset());
        } else {
            return nextOffset;
        }
    }

    /**
     * get partitionId.
     *
     * @return int
     */
    public final int partitionId() {
        return partitionId;
    }

    /** check snapshot. */
    public void checkTriggerCheckpoint() throws Exception {
        final long currentTime = clock.currentTimeMillis();
        if (currentTime - preSnapshotTime >= snapshotIntervalMills) {
            snapshot();
            sinkSnapshotCost.set(clock.currentTimeMillis() - currentTime);
            preSnapshotTime = currentTime;
            updateAndCommitOffset();
        }
    }

    /** open sink handler. */
    public abstract void open();

    /**
     * committableOffset.
     *
     * @return GroupOffset
     */
    public abstract Offset committableOffset();

    /** callback close. */
    public abstract void closeCallback() throws IOException;

    /**
     * get all functions.
     *
     * @return function list
     */
    public abstract List<AbstractFunction> getFunctions();

    /**
     * create thread name.
     *
     * @return string
     */
    public String threadName() {
        return getClass().getSimpleName() + "-thread-" + getSinHandlerId().replace("_", "-");
    }

    /**
     * process.
     *
     * @param offset offset
     * @param iterator iterator
     */
    public abstract void process(Offset offset, Iterator<byte[]> iterator) throws Exception;

    /**
     * commit file offset.
     *
     * @param offset offset
     */
    protected void commit(Offset offset) {
        if (offset == null) {
            return;
        }
        try {
            channel.commit(partitionId, groupId, offset);
            committedOffset = offset;
        } catch (Throwable e) {
            LOG.warn("commit fail", e);
        }
    }

    /**
     * default poll data, subclass can override this function.
     *
     * @param groupOffset groupOffset
     * @return RecordsResultSet
     * @throws IOException IOException
     * @throws InterruptedException InterruptedException
     */
    protected RecordsResultSet poll(Offset groupOffset) throws IOException, InterruptedException {
        return channel.poll(
                partitionId, groupOffset, DEFAULT_WAIT_TIME_MILLIS, TimeUnit.MILLISECONDS);
    }

    /**
     * get sink handler identity.
     *
     * @return string
     */
    protected final String getSinHandlerId() {
        return channel.topic() + "_" + groupId + "_" + partitionId;
    }

    /**
     * create function.
     *
     * @return AbstractFunction
     */
    protected final AbstractFunction createFunction(String id) {
        final Function function = functionFactory.create();
        if (!(function instanceof AbstractFunction)) {
            throw new IllegalStateException(
                    function.getClass() + " not extends " + AbstractFunction.class);
        }
        final AbstractFunction abstractFunction = (AbstractFunction) function;
        final ContextBuilder builder =
                ContextBuilder.newBuilder()
                        .id(id == null ? getSinHandlerId() : id)
                        .partitionId(partitionId)
                        .startOffset(startOffset)
                        .groupId(groupId)
                        .topic(channel.topic());
        builder.addAll(config);
        try {
            abstractFunction.open(builder.build());
            return abstractFunction;
        } catch (Exception e) {
            IOUtils.closeQuietly(abstractFunction);
            throw new IllegalStateException("open function fail", e);
        }
    }

    @Override
    public void close() throws IOException {
        if (closed.compareAndSet(false, true)) {
            try {
                interrupt();
                joinQuietly(this);
            } finally {
                closeCallback();
                commit(fetchOffset);
            }
        }
    }

    /** sleep when exception, protect while true cause cpu high. */
    protected void sleepWhenException() {
        sleepQuietly(DEFAULT_SLEEP_WHEN_EXCEPTION);
    }

    /**
     * get lag by group offset.
     *
     * @param offset offset
     * @return long lag
     */
    public long lag(Offset offset) {
        return channel.lag(partitionId, offset);
    }

    /**
     * get partition lag.
     *
     * @return lag
     */
    public long lag() {
        return lag(fetchOffset);
    }

    /**
     * get commit offset watermark. for unit test visitable.
     *
     * @return GroupOffset
     */
    public Offset committedOffset() {
        return committedOffset;
    }

    /** update commit offset watermark. */
    protected void updateAndCommitOffset() {
        final Offset committedOffset = committedOffset();
        final Offset newCommittedOffset =
                skipGroupOffsetByMaxRetainSize(Offset.max(committableOffset(), committedOffset));
        if (newCommittedOffset.compareTo(committedOffset) > 0) {
            commit(newCommittedOffset);
        }
    }

    /** skip commit offset watermark by max retain size. */
    protected Offset skipGroupOffsetByMaxRetainSize(final Offset offset) {
        if (maxRetainSize == null) {
            return offset;
        }
        Offset newOffset = offset;
        Offset preOffset = offset;
        while (true) {
            final long lag = lag(newOffset);
            if (lag > maxRetainSize) {
                preOffset = newOffset;
                newOffset = newOffset.skipNextSegmentHead();
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
}
