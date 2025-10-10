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

import org.zicat.tributary.common.Clock;
import org.zicat.tributary.common.MetricCollector;
import org.zicat.tributary.common.MetricKey;
import org.zicat.tributary.common.SystemClock;
import static org.zicat.tributary.common.Threads.joinQuietly;
import static org.zicat.tributary.common.Threads.sleepQuietly;
import static org.zicat.tributary.sink.function.FunctionFactory.findFunctionFactory;
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

import java.io.Closeable;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/** PartitionHandler. */
public abstract class PartitionHandler extends Thread
        implements Closeable, CheckpointedFunction, MetricCollector {

    public static final MetricKey KEY_SINK_LAG = new MetricKey("tributary_sink_lag");

    public static final ConfigOption<Clock> OPTION_PARTITION_HANDLER_CLOCK =
            ConfigOptions.key("_partition_handler_clock")
                    .<Clock>objectType()
                    .defaultValue(new SystemClock());

    private static final MetricKey SINK_SNAPSHOT_COAST =
            new MetricKey("tributary_sink_snapshot_cost");

    protected static final Logger LOG = LoggerFactory.getLogger(PartitionHandler.class);
    protected static final long DEFAULT_WAIT_TIME_MILLIS = 500;
    protected static final long DEFAULT_SLEEP_WHEN_EXCEPTION = 1000;

    protected final String groupId;
    protected final Channel channel;
    protected final Integer partitionId;
    protected final FunctionFactory functionFactory;
    protected final SinkGroupConfig config;
    protected final Offset startOffset;
    protected final AtomicBoolean closed = new AtomicBoolean();
    protected final long snapshotIntervalMills;
    protected final Clock clock;

    protected Offset fetchOffset;
    protected Offset committedOffset;
    protected long preSnapshotTime;
    protected long sinkSnapshotCost;
    protected MetricKey partitionSinkSnapShortCountKey;

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
        this.committedOffset = startOffset;
        this.fetchOffset = startOffset;
        this.partitionSinkSnapShortCountKey =
                SINK_SNAPSHOT_COAST.addLabel("partitionId", partitionId);
        setName(threadName());
    }

    @Override
    public void run() {
        while (true) {
            if (runOneBatch()) {
                return;
            }
        }
    }

    public boolean runOneBatch() {
        try {
            final RecordsResultSet result = poll(fetchOffset);
            if (closed.get() && result.isEmpty()) {
                return true;
            }
            if (!result.isEmpty()) {
                process(result.nexOffset(), result);
            }
            checkTriggerCheckpoint();
            fetchOffset = nextFetchOffset(result.nexOffset());
        } catch (InterruptedException interruptedException) {
            if (closed.get()) {
                return true;
            }
        } catch (Throwable e) {
            LOG.error("poll data failed.", e);
            if (closed.get()) {
                return true;
            }
            updateAndCommitOffset();
            fetchOffset = nextFetchOffset(null);
            sleepWhenException();
        }
        return false;
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
            sinkSnapshotCost = clock.currentTimeMillis() - currentTime;
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
    public abstract List<Function> getFunctions();

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
     * @return Function
     */
    protected final Function createFunction(String id) {
        final Function function = functionFactory.create();
        final ContextBuilder builder =
                ContextBuilder.newBuilder()
                        .id(id == null ? getSinHandlerId() : id)
                        .partitionId(partitionId)
                        .startOffset(startOffset)
                        .groupId(groupId)
                        .topic(channel.topic());
        builder.addAll(config);
        try {
            function.open(builder.build());
            return function;
        } catch (Exception e) {
            IOUtils.closeQuietly(function);
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
    private long lag(Offset offset) {
        return channel.lag(partitionId, offset);
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
        final Offset committableOffset = committableOffset();
        if (committableOffset != null && committableOffset.compareTo(committedOffset) > 0) {
            commit(committableOffset);
        }
    }

    @Override
    public Map<MetricKey, Double> gaugeFamily() {
        final Map<MetricKey, Double> result = new HashMap<>();
        result.put(partitionSinkSnapShortCountKey, (double) sinkSnapshotCost);
        result.put(KEY_SINK_LAG, (double) lag(fetchOffset));
        return result;
    }

    @Override
    public Map<MetricKey, Double> counterFamily() {
        return Collections.emptyMap();
    }

    /**
     * fetch offset for test.
     *
     * @return offset
     */
    public Offset fetchOffset() {
        return fetchOffset;
    }
}
