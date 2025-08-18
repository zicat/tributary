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

package org.zicat.tributary.source.base.netty;

import io.netty.channel.ChannelHandler;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.timeout.IdleStateHandler;

import org.zicat.tributary.channel.Channel;
import org.zicat.tributary.common.IOUtils;
import org.zicat.tributary.common.ReadableConfig;
import org.zicat.tributary.source.base.netty.pipeline.LengthPipelineInitialization;
import org.zicat.tributary.source.base.netty.pipeline.PipelineInitialization;

import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.TimeUnit;

/** DefaultNettySource. */
public class DefaultNettySource extends AbstractNettySource {

    protected final long idle;
    protected final PipelineInitialization pipelineInitialization;

    public DefaultNettySource(
            String sourceId,
            ReadableConfig config,
            String host,
            int port,
            int eventThreads,
            Channel channel,
            Duration idleDuration)
            throws Exception {
        super(sourceId, config, host, port, eventThreads, channel);
        this.idle = idleDuration.toMillis();
        this.pipelineInitialization = createPipelineInitialization();
    }

    /**
     * init channel.
     *
     * @param ch ch
     */
    @Override
    protected void initChannel(SocketChannel ch) {
        pipelineInitialization.init(ch);
    }

    /**
     * get netty decoder.
     *
     * @return NettyDecoder
     */
    protected PipelineInitialization createPipelineInitialization() throws Exception {
        return new LengthPipelineInitialization(this);
    }

    /**
     * idleStateHandler.
     *
     * @return ChannelHandler
     */
    public ChannelHandler idleStateHandler() {
        return new IdleStateHandler(idle, idle, idle, TimeUnit.MILLISECONDS);
    }

    @Override
    public void close() throws IOException {
        try {
            super.close();
        } finally {
            IOUtils.closeQuietly(pipelineInitialization);
        }
    }
}
