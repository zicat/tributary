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

import lombok.extern.slf4j.Slf4j;
import org.zicat.tributary.queue.LogQueue;
import org.zicat.tributary.queue.utils.IOUtils;
import org.zicat.tributary.sink.handler.AbstractPartitionHandler;
import org.zicat.tributary.sink.handler.factory.PartitionHandlerFactory;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.List;
import java.util.ServiceLoader;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static org.zicat.tributary.sink.handler.AbstractPartitionHandler.*;

/** 消费组模型，一个groupId对应一个SinkGroupManager . */
@Slf4j
public class SinkGroupManager implements Closeable {

    private final String groupId;
    private final LogQueue logQueue;
    private final SinkGroupConfig sinkGroupConfig;
    private final List<AbstractPartitionHandler> handlers = new ArrayList<>();
    private ScheduledExecutorService service;

    public SinkGroupManager(String groupId, LogQueue logQueue, SinkGroupConfig sinkGroupConfig) {
        this.groupId = groupId;
        this.logQueue = logQueue;
        this.sinkGroupConfig = sinkGroupConfig;
    }

    /** create sink handler. */
    public synchronized void createPartitionHandlesAndStart() {
        if (handlers.size() == logQueue.partition()) {
            return;
        }
        final PartitionHandlerFactory partitionHandlerFactory =
                findPartitionHandlerFactory(sinkGroupConfig);
        for (int partitionId = 0; partitionId < logQueue.partition(); partitionId++) {
            final AbstractPartitionHandler sinkHandler =
                    partitionHandlerFactory.createHandler(
                            groupId, logQueue, partitionId, sinkGroupConfig);
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
            service.scheduleAtFixedRate(
                    () -> handlers.forEach(AbstractPartitionHandler::commit),
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
     * use java spi find {@link PartitionHandlerFactory} by identify.
     *
     * @param sinkGroupConfig sinkGroupConfig
     * @return SinkHandlerFactory
     */
    private static PartitionHandlerFactory findPartitionHandlerFactory(
            SinkGroupConfig sinkGroupConfig) {
        final String identify = sinkGroupConfig.handlerIdentify();
        final ServiceLoader<PartitionHandlerFactory> loader =
                ServiceLoader.load(PartitionHandlerFactory.class);
        for (PartitionHandlerFactory partitionHandlerFactory : loader) {
            if (identify.equals(partitionHandlerFactory.identify())) {
                return partitionHandlerFactory;
            }
        }
        throw new RuntimeException("identify not found," + identify);
    }
}
