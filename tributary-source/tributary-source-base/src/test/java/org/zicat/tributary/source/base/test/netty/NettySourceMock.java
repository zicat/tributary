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

package org.zicat.tributary.source.base.test.netty;

import org.zicat.tributary.source.base.netty.NettySource;

import org.zicat.tributary.channel.Channel;
import org.zicat.tributary.common.config.ReadableConfig;
import static org.zicat.tributary.source.base.netty.NettySourceFactory.OPTION_NETTY_THREADS_EVENT_LOOP;
import org.zicat.tributary.source.base.netty.pipeline.LengthPipelineInitialization;
import org.zicat.tributary.source.base.netty.pipeline.PipelineInitialization;

import java.util.Collections;

/** NettySourceMock. */
public class NettySourceMock extends NettySource {

    public NettySourceMock(
            String sourceId,
            ReadableConfig config,
            String host,
            int port,
            int eventThreads,
            Channel channel)
            throws Exception {
        super(
                sourceId,
                config,
                channel,
                host == null ? Collections.emptyList() : Collections.singletonList(host),
                port,
                eventThreads);
    }

    public NettySourceMock(ReadableConfig config, Channel channel) throws Exception {
        this(config, null, 0, channel);
    }

    public NettySourceMock(ReadableConfig config, String sourceId, Channel channel)
            throws Exception {
        this(sourceId, config, null, 0, OPTION_NETTY_THREADS_EVENT_LOOP.defaultValue(), channel);
    }

    public NettySourceMock(ReadableConfig config, String host, int port, Channel channel)
            throws Exception {
        this("", config, host, port, OPTION_NETTY_THREADS_EVENT_LOOP.defaultValue(), channel);
    }

    @Override
    protected PipelineInitialization createPipelineInitialization() throws Exception {
        return new LengthPipelineInitialization(this);
    }
}
