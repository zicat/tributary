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
import org.zicat.tributary.common.IOUtils;
import org.zicat.tributary.sink.SinkGroupConfig;
import org.zicat.tributary.sink.function.*;

import java.io.Closeable;
import java.io.IOException;
import java.util.Iterator;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.zicat.tributary.common.SpiFactory.findFactory;
import static org.zicat.tributary.common.Threads.joinQuietly;

/** PartitionHandler. */
public abstract class PartitionHandler extends Thread implements Closeable, Trigger {

    private static final Logger LOG = LoggerFactory.getLogger(PartitionHandler.class);
    protected static final long DEFAULT_WAIT_TIME_MILLIS = 5000;

    protected final String groupId;
    private final Channel channel;
    protected final Integer partitionId;
    protected final FunctionFactory functionFactory;
    protected final SinkGroupConfig sinkGroupConfig;
    protected final Offset startOffset;
    protected final AtomicBoolean closed;
    private Offset commitOffsetWaterMark;

    public PartitionHandler(
            String groupId, Channel channel, int partitionId, SinkGroupConfig sinkGroupConfig) {
        this.groupId = groupId;
        this.channel = channel;
        this.partitionId = partitionId;
        this.sinkGroupConfig = sinkGroupConfig;
        this.functionFactory =
                findFactory(sinkGroupConfig.functionIdentity(), FunctionFactory.class);
        this.startOffset = channel.committedOffset(groupId, partitionId);
        this.commitOffsetWaterMark = startOffset;
        this.closed = new AtomicBoolean(false);
        setName(threadName());
    }

    /**
     * get partitionId.
     *
     * @return int
     */
    public final int partitionId() {
        return partitionId;
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
            commitOffsetWaterMark = channel.committedOffset(groupId, partitionId);
        } catch (Throwable e) {
            LOG.warn("commit fail", e);
        }
    }

    /**
     * default poll data, subclass can override this function.
     *
     * @param groupOffset groupOffset
     * @param idleTimeMillis idleTimeMillis
     * @return RecordsResultSet
     * @throws IOException IOException
     * @throws InterruptedException InterruptedException
     */
    protected RecordsResultSet poll(Offset groupOffset, long idleTimeMillis)
            throws IOException, InterruptedException {
        final long waitTime = idleTimeMillis <= 0 ? DEFAULT_WAIT_TIME_MILLIS : idleTimeMillis;
        return channel.poll(partitionId, groupOffset, waitTime, TimeUnit.MILLISECONDS);
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
        try {
            if (!(function instanceof AbstractFunction)) {
                throw new IllegalStateException(
                        function.getClass().getName()
                                + " must extends "
                                + AbstractFunction.class.getName());
            }
            final ContextBuilder builder =
                    ContextBuilder.newBuilder()
                            .id(id == null ? getSinHandlerId() : id)
                            .partitionId(partitionId)
                            .startOffset(startOffset)
                            .groupId(groupId)
                            .topic(channel.topic());
            builder.addAll(sinkGroupConfig);
            function.open(builder.build());
            return (AbstractFunction) function;
        } catch (Exception e) {
            IOUtils.closeQuietly(function);
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
    public Offset commitOffsetWaterMark() {
        return commitOffsetWaterMark;
    }
}
