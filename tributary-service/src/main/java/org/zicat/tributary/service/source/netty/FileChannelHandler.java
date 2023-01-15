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

package org.zicat.tributary.service.source.netty;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.timeout.IdleState;
import io.netty.handler.timeout.IdleStateEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zicat.tributary.channel.Channel;

import java.util.Random;

/** FileChannelHandler. */
public class FileChannelHandler extends SimpleChannelInboundHandler<byte[]> {

    private static final Logger LOG = LoggerFactory.getLogger(FileChannelHandler.class);

    private final Random random = new Random(System.currentTimeMillis());
    private final Channel channel;

    public FileChannelHandler(Channel channel) {
        this.channel = channel;
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        LOG.error(ctx.channel().remoteAddress().toString(), cause);
    }

    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) {
        if (!(evt instanceof IdleStateEvent)) {
            return;
        }
        final IdleStateEvent e = (IdleStateEvent) evt;
        if (e.state() == IdleState.READER_IDLE || e.state() == IdleState.WRITER_IDLE) {
            LOG.info("channel idled, close it, {}", ctx.channel().remoteAddress().toString());
            ctx.channel().close();
        }
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, byte[] packet) {
        try {
            final int partition = random.nextInt(channel.partition());
            channel.append(partition, packet);
        } catch (Exception e) {
            LOG.error("append data error", e);
        }
    }
}
