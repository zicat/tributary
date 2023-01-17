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
import org.zicat.tributary.channel.utils.IOUtils;
import org.zicat.tributary.sink.SinkGroupConfig;
import org.zicat.tributary.sink.function.*;

import java.io.Closeable;
import java.io.IOException;
import java.util.Iterator;
import java.util.ServiceLoader;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.zicat.tributary.sink.utils.Threads.joinQuietly;

/** PartitionHandler. */
public abstract class PartitionHandler extends Thread implements Closeable, Trigger {

    private static final Logger LOG = LoggerFactory.getLogger(PartitionHandler.class);
    protected static final long DEFAULT_WAIT_TIME_MILLIS = 5000;

    protected final String groupId;
    protected final Channel channel;
    protected final Integer partitionId;
    protected final FunctionFactory functionFactory;
    protected final SinkGroupConfig sinkGroupConfig;
    protected final RecordsOffset startOffset;
    protected final AtomicBoolean closed;

    public PartitionHandler(
            String groupId, Channel channel, int partitionId, SinkGroupConfig sinkGroupConfig) {
        this.groupId = groupId;
        this.channel = channel;
        this.partitionId = partitionId;
        this.sinkGroupConfig = sinkGroupConfig;
        this.functionFactory = findFunctionFactory(sinkGroupConfig);
        this.startOffset = getRecordsOffset(groupId, channel, partitionId);
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

    /**
     * get records offset.
     *
     * @param groupId groupId
     * @param channel channel
     * @param partitionId partitionId
     * @return RecordsOffset
     */
    private RecordsOffset getRecordsOffset(String groupId, Channel channel, int partitionId) {
        final RecordsOffset recordsOffset = channel.getRecordsOffset(groupId, partitionId);
        return recordsOffset.segmentId() == -1
                ? recordsOffset.skip2TargetHead(channel.lastSegmentId(partitionId))
                : recordsOffset;
    }

    /** open sink handler. */
    public abstract void open();

    /**
     * committableOffset.
     *
     * @return RecordsOffset
     */
    public abstract RecordsOffset committableOffset();

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
     * @param recordsOffset recordsOffset
     * @param iterator iterator
     */
    public abstract void process(RecordsOffset recordsOffset, Iterator<byte[]> iterator)
            throws Exception;

    /**
     * commit file offset.
     *
     * @param recordsOffset recordsOffset
     */
    protected void commit(RecordsOffset recordsOffset) {
        if (recordsOffset == null) {
            return;
        }
        try {
            channel.commit(groupId, partitionId, recordsOffset);
        } catch (Throwable e) {
            LOG.warn("commit fail", e);
        }
    }

    /**
     * default poll data, subclass can override this function.
     *
     * @param partitionId partitionId
     * @param recordsOffset recordsOffset
     * @param idleTimeMillis idleTimeMillis
     * @return RecordsResultSet
     * @throws IOException IOException
     * @throws InterruptedException InterruptedException
     */
    protected RecordsResultSet poll(
            int partitionId, RecordsOffset recordsOffset, long idleTimeMillis)
            throws IOException, InterruptedException {
        final long waitTime = idleTimeMillis <= 0 ? DEFAULT_WAIT_TIME_MILLIS : idleTimeMillis;
        return channel.poll(partitionId, recordsOffset, waitTime, TimeUnit.MILLISECONDS);
    }

    /**
     * use java spi find SinkHandlerFactory by identity.
     *
     * @param sinkGroupConfig sinkGroupConfig
     * @return SinkHandlerFactory
     */
    private static FunctionFactory findFunctionFactory(SinkGroupConfig sinkGroupConfig) {
        final String identity = sinkGroupConfig.functionIdentity();
        final ServiceLoader<FunctionFactory> loader = ServiceLoader.load(FunctionFactory.class);
        for (FunctionFactory functionFactory : loader) {
            if (identity.equals(functionFactory.identity())) {
                return functionFactory;
            }
        }
        throw new RuntimeException("identity not found, " + identity);
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
        final Function function = functionFactory.createFunction();
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
                            .groupId(groupId)
                            .partitionId(partitionId)
                            .startRecordsOffset(startOffset)
                            .topic(channel.topic());
            builder.addAll(sinkGroupConfig.customConfig());
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
            joinQuietly(this);
        }
    }
}
