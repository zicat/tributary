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

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zicat.tributary.channel.Channel;
import org.zicat.tributary.common.IOUtils;

import java.io.IOException;

/** ChannelHandler. */
public abstract class ChannelHandler extends SimpleChannelInboundHandler<byte[]> {

    private static final Logger LOG = LoggerFactory.getLogger(ChannelHandler.class);
    private final AbstractNettySource source;
    private final Channel channel;
    private final int partition;

    public ChannelHandler(AbstractNettySource source, int partition) {
        this.source = source;
        this.channel = source.getChannel();
        this.partition = partition;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, byte[] packet) throws IOException {
        try {
            channel.append(partition, packet);
            ackSuccess(packet, ctx);
        } catch (IOException e) {
            LOG.error("append data error, stop listen  {}:{}", source.host, source.port, e);
            IOUtils.closeQuietly(source);
            throw e;
        }
    }

    /**
     * ack success to client.
     *
     * @param receivedData receivedData
     * @param ctx ctx
     */
    public abstract void ackSuccess(byte[] receivedData, ChannelHandlerContext ctx);
}
