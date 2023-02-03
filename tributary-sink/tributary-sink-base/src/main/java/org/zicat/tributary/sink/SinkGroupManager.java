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

package org.zicat.tributary.sink;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zicat.tributary.channel.Channel;
import org.zicat.tributary.common.IOUtils;
import org.zicat.tributary.sink.function.AbstractFunction;
import org.zicat.tributary.sink.handler.AbstractPartitionHandler;
import org.zicat.tributary.sink.handler.PartitionHandlerFactory;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static org.zicat.tributary.sink.handler.AbstractPartitionHandler.*;
import static org.zicat.tributary.sink.handler.PartitionHandlerFactory.findPartitionHandlerFactory;

/**
 * One SinkGroupManager Instance maintain a group consumer one {@link Channel} with {@link
 * SinkGroupConfig}.
 */
public class SinkGroupManager implements Closeable {

    private static final Logger LOG = LoggerFactory.getLogger(SinkGroupManager.class);

    private final String groupId;
    private final Channel channel;
    private final SinkGroupConfig sinkGroupConfig;
    private final List<AbstractPartitionHandler> handlers = new ArrayList<>();
    private ScheduledExecutorService service;

    public SinkGroupManager(String groupId, Channel channel, SinkGroupConfig sinkGroupConfig) {
        this.groupId = groupId;
        this.channel = channel;
        this.sinkGroupConfig = sinkGroupConfig;
    }

    /** create sink handler. */
    public synchronized void createPartitionHandlesAndStart() {
        if (handlers.size() == channel.partition()) {
            return;
        }
        final PartitionHandlerFactory partitionHandlerFactory =
                findPartitionHandlerFactory(sinkGroupConfig.handlerIdentity());
        for (int partitionId = 0; partitionId < channel.partition(); partitionId++) {
            final AbstractPartitionHandler sinkHandler =
                    partitionHandlerFactory.createHandler(
                            groupId, channel, partitionId, sinkGroupConfig);
            handlers.add(sinkHandler);
        }
        handlers.forEach(AbstractPartitionHandler::open);
        handlers.forEach(Thread::start);
        supportMaxRetainSize();
    }

    /** support max retain size. */
    private void supportMaxRetainSize() {
        final Long maxRetainSize = parseMaxRetainSize(sinkGroupConfig);
        if (maxRetainSize != null) {
            final long periodMill =
                    sinkGroupConfig.getCustomProperty(
                            KEY_RETAIN_SIZE_CHECK_PERIOD_MILLI,
                            DEFAULT_RETAIN_SIZE_CHECK_PERIOD_MILLI);
            service = Executors.newSingleThreadScheduledExecutor();
            service.scheduleWithFixedDelay(
                    () -> {
                        for (AbstractPartitionHandler handler : handlers) {
                            try {
                                handler.commit();
                            } catch (Throwable e) {
                                LOG.warn("period commit handle error", e);
                            }
                        }
                    },
                    periodMill,
                    periodMill,
                    TimeUnit.MILLISECONDS);
        }
    }

    /**
     * get total consumer lag.
     *
     * @return lag
     */
    public long lag() {
        return handlers.stream().mapToLong(AbstractPartitionHandler::lag).sum();
    }

    /**
     * get functions.
     *
     * @return map
     */
    public Map<Integer, List<AbstractFunction>> getFunctions() {
        final Map<Integer, List<AbstractFunction>> result = new HashMap<>();
        for (AbstractPartitionHandler handler : handlers) {
            result.put(handler.partitionId(), handler.getFunctions());
        }
        return result;
    }

    @Override
    public void close() {
        try {
            if (service != null) {
                service.shutdown();
            }
        } finally {
            handlers.forEach(IOUtils::closeQuietly);
        }
    }

    /**
     * get topic.
     *
     * @return topic
     */
    public final String topic() {
        return channel.topic();
    }
}
