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

import java.io.Closeable;
import java.io.IOException;
import java.util.Iterator;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/** PartitionHandler. */
public abstract class PartitionHandler extends Thread implements Closeable, CheckpointedFunction {

    public static final ConfigOption<Clock> OPTION_PARTITION_HANDLER_CLOCK =
            ConfigOptions.key("_partition_handler_clock")
                    .<Clock>objectType()
                    .defaultValue(new SystemClock());

    private static final Logger LOG = LoggerFactory.getLogger(PartitionHandler.class);
    private static final long DEFAULT_WAIT_TIME_MILLIS = 500;
    private static final long DEFAULT_SLEEP_WHEN_EXCEPTION = 1000;

    protected final String groupId;
    protected final Channel channel;
    protected final Integer partitionId;
    protected final FunctionFactory functionFactory;
    protected final SinkGroupConfig sinkGroupConfig;
    protected final Offset startOffset;
    protected final Long maxRetainSize;
    protected final AtomicBoolean closed;
    protected final long snapshotIntervalMills;
    protected final Clock clock;
    protected Offset committedOffset;
    private long preSnapshotTime;

    public PartitionHandler(
            String groupId, Channel channel, int partitionId, SinkGroupConfig sinkGroupConfig) {
        this.groupId = groupId;
        this.channel = channel;
        this.partitionId = partitionId;
        this.snapshotIntervalMills = snapshotIntervalMills(sinkGroupConfig);
        this.sinkGroupConfig = sinkGroupConfig;
        this.clock = sinkGroupConfig.get(OPTION_PARTITION_HANDLER_CLOCK);
        this.preSnapshotTime = clock.currentTimeMillis();
        this.functionFactory = findFunctionFactory(sinkGroupConfig.functionIdentity());
        this.startOffset = channel.committedOffset(groupId, partitionId);
        this.maxRetainSize = parseMaxRetainSize(sinkGroupConfig);
        this.closed = new AtomicBoolean(false);
        setName(threadName());
        this.committedOffset = startOffset;
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
            updateAndCommitOffset();
            preSnapshotTime = currentTime;
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
                    function.getClass().getName()
                            + " must extends "
                            + AbstractFunction.class.getName());
        }
        final AbstractFunction abstractFunction = (AbstractFunction) function;
        final ContextBuilder builder =
                ContextBuilder.newBuilder()
                        .id(id == null ? getSinHandlerId() : id)
                        .partitionId(partitionId)
                        .startOffset(startOffset)
                        .groupId(groupId)
                        .topic(channel.topic());
        builder.addAll(sinkGroupConfig);
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
            interrupt();
            joinQuietly(this);
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

    @Override
    public void snapshot() throws Exception {}
}
