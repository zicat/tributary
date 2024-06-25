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

package org.zicat.tributary.source.netty;

import io.netty.channel.ChannelHandler;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.timeout.IdleStateHandler;
import org.zicat.tributary.common.DefaultReadableConfig;
import org.zicat.tributary.common.IOUtils;
import org.zicat.tributary.common.ReadableConfig;
import org.zicat.tributary.source.RecordsChannel;
import org.zicat.tributary.source.netty.pipeline.LengthPipelineInitialization;
import org.zicat.tributary.source.netty.pipeline.PipelineInitialization;

import static org.zicat.tributary.source.netty.AbstractNettySourceFactory.OPTION_NETTY_HOST;
import static org.zicat.tributary.source.netty.AbstractNettySourceFactory.OPTION_NETTY_THREADS;
import static org.zicat.tributary.source.netty.DefaultNettySourceFactory.OPTION_NETTY_IDLE_SECOND;

/** DefaultNettySource. */
public class DefaultNettySource extends AbstractNettySource {

    protected final int idleSecond;
    protected final PipelineInitialization pipelineInitialization;

    public DefaultNettySource(
            String sourceId,
            ReadableConfig config,
            String host,
            int port,
            int eventThreads,
            RecordsChannel channel,
            int idleSecond)
            throws Exception {
        super(sourceId, config, host, port, eventThreads, channel);
        this.idleSecond = idleSecond;
        this.pipelineInitialization = createPipelineInitialization();
    }

    public DefaultNettySource(int port, RecordsChannel channel) throws Exception {
        this(
                "",
                new DefaultReadableConfig(),
                OPTION_NETTY_HOST.defaultValue(),
                port,
                OPTION_NETTY_THREADS.defaultValue(),
                channel,
                OPTION_NETTY_IDLE_SECOND.defaultValue());
    }

    public DefaultNettySource(String host, RecordsChannel channel) throws Exception {
        this(host, 0, channel);
    }

    public DefaultNettySource(ReadableConfig config, String host, RecordsChannel channel)
            throws Exception {
        this(config, host, 0, channel);
    }

    public DefaultNettySource(ReadableConfig config, String host, int port, RecordsChannel channel)
            throws Exception {
        this(
                "",
                config,
                host,
                port,
                OPTION_NETTY_THREADS.defaultValue(),
                channel,
                OPTION_NETTY_IDLE_SECOND.defaultValue());
    }

    public DefaultNettySource(String host, int port, RecordsChannel channel) throws Exception {
        this(
                "",
                new DefaultReadableConfig(),
                host,
                port,
                OPTION_NETTY_THREADS.defaultValue(),
                channel,
                OPTION_NETTY_IDLE_SECOND.defaultValue());
    }

    public DefaultNettySource(RecordsChannel channel) throws Exception {
        this(0, channel);
    }

    /**
     * init channel.
     *
     * @param ch ch
     */
    @Override
    protected void initChannel(SocketChannel ch, RecordsChannel channel) {
        pipelineInitialization.init(ch.pipeline());
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
        return new IdleStateHandler(idleSecond, idleSecond, idleSecond);
    }

    @Override
    public void close() {
        try {
            super.close();
        } finally {
            IOUtils.closeQuietly(pipelineInitialization);
        }
    }
}
