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

import static org.zicat.tributary.common.util.Collections.sumDouble;
import org.zicat.tributary.common.metric.MetricCollector;
import static org.zicat.tributary.common.SpiFactory.findFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zicat.tributary.channel.Channel;
import org.zicat.tributary.common.metric.MetricKey;
import org.zicat.tributary.common.util.IOUtils;
import org.zicat.tributary.sink.handler.PartitionHandler;
import org.zicat.tributary.sink.handler.PartitionHandlerFactory;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * One SinkGroupManager Instance maintain a group consumer one {@link Channel} with {@link
 * SinkGroupConfig}.
 */
public class SinkGroupManager implements Closeable, MetricCollector {

    private static final Logger LOG = LoggerFactory.getLogger(SinkGroupManager.class);

    private final String groupId;
    private final Channel channel;
    private final SinkGroupConfig sinkGroupConfig;
    private final List<PartitionHandler> handlers;
    private final String id;

    public SinkGroupManager(String groupId, Channel channel, SinkGroupConfig sinkGroupConfig) {
        this.groupId = groupId;
        this.channel = channel;
        this.sinkGroupConfig = sinkGroupConfig;
        this.handlers = new ArrayList<>(channel.partition());
        this.id = channel.topic() + "_" + groupId;
    }

    /** create sink handler. */
    public void createPartitionHandlesAndStart() {
        if (handlers.size() == channel.partition()) {
            return;
        }
        final PartitionHandlerFactory factory =
                findFactory(sinkGroupConfig.handlerIdentity(), PartitionHandlerFactory.class);
        for (int partitionId = 0; partitionId < channel.partition(); partitionId++) {
            handlers.add(factory.create(groupId, channel, partitionId, sinkGroupConfig));
        }
        handlers.forEach(PartitionHandler::open);
        handlers.forEach(Thread::start);
    }

    @Override
    public Map<MetricKey, Double> gaugeFamily() {
        return sumDouble(handlers.stream().map(PartitionHandler::gaugeFamily));
    }

    @Override
    public Map<MetricKey, Double> counterFamily() {
        return sumDouble(handlers.stream().map(PartitionHandler::counterFamily));
    }

    @Override
    public void close() {
        IOUtils.concurrentCloseQuietly(handlers);
        LOG.info("stop sink group manager, id: {}", id);
    }

    /**
     * get id.
     *
     * @return id
     */
    public String id() {
        return id;
    }

    /**
     * get groupId.
     *
     * @return group id
     */
    public String groupId() {
        return groupId;
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
