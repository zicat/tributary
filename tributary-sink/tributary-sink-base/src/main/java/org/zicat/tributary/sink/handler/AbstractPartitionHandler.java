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
import org.zicat.tributary.sink.SinkGroupConfig;
import org.zicat.tributary.sink.function.AbstractFunction;
import org.zicat.tributary.sink.function.Function;
import org.zicat.tributary.sink.function.FunctionFactory;

import java.io.IOException;
import java.util.List;

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

    private Offset fetchOffset;

    public AbstractPartitionHandler(
            String groupId, Channel channel, int partitionId, SinkGroupConfig sinkGroupConfig) {
        super(groupId, channel, partitionId, sinkGroupConfig);
        this.fetchOffset = startOffset;
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
     * get partition lag.
     *
     * @return lag
     */
    public final long lag() {
        return lag(fetchOffset);
    }

    @Override
    public void close() throws IOException {
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
