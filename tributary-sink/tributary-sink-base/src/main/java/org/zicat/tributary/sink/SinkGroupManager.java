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

import static org.zicat.tributary.common.SpiFactory.findFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zicat.tributary.channel.Channel;
import org.zicat.tributary.common.GaugeFamily;
import org.zicat.tributary.common.GaugeKey;
import org.zicat.tributary.common.IOUtils;
import org.zicat.tributary.common.ReadableConfig;
import org.zicat.tributary.sink.function.AbstractFunction;
import org.zicat.tributary.sink.handler.PartitionHandler;
import org.zicat.tributary.sink.handler.PartitionHandlerFactory;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * One SinkGroupManager Instance maintain a group consumer one {@link Channel} with {@link
 * SinkGroupConfig}.
 */
public class SinkGroupManager implements Closeable {

    public static final GaugeKey KEY_SINK_LAG = new GaugeKey("sink_lag", "sink lag");
    private static final Logger LOG = LoggerFactory.getLogger(SinkGroupManager.class);

    private final String groupId;
    private final Channel channel;
    private final SinkGroupConfig sinkGroupConfig;
    private final List<PartitionHandler> handlers = new ArrayList<>();

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
                findFactory(sinkGroupConfig.handlerIdentity(), PartitionHandlerFactory.class);
        for (int partitionId = 0; partitionId < channel.partition(); partitionId++) {
            final PartitionHandler sinkHandler =
                    partitionHandlerFactory.createHandler(
                            groupId, channel, partitionId, sinkGroupConfig);
            handlers.add(sinkHandler);
        }
        handlers.forEach(PartitionHandler::open);
        handlers.forEach(Thread::start);
    }

    public Map<GaugeKey, GaugeFamily> gaugeFamily() {
        final Map<GaugeKey, GaugeFamily> families = new HashMap<>();
        KEY_SINK_LAG
                .value(handlers.stream().mapToLong(PartitionHandler::lag).sum())
                .register(families);
        return families;
    }

    /**
     * get functions.
     *
     * @return map
     */
    public Map<Integer, List<AbstractFunction>> getFunctions() {
        final Map<Integer, List<AbstractFunction>> result = new HashMap<>();
        for (PartitionHandler handler : handlers) {
            result.put(handler.partitionId(), handler.getFunctions());
        }
        return result;
    }

    @Override
    public void close() {
        IOUtils.concurrentCloseQuietly(handlers);
        LOG.info("stop sink topic = {}, group = {}", channel.topic(), groupId);
    }

    public String id() {
        return channel.topic() + "_" + groupId;
    }

    /**
     * get topic.
     *
     * @return topic
     */
    public final String topic() {
        return channel.topic();
    }

    /**
     * get sink group config.
     *
     * @return ReadableConfig
     */
    public final ReadableConfig sinkGroupConfig() {
        return sinkGroupConfig;
    }
}
