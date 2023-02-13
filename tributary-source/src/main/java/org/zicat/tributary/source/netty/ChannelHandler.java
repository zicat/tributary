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

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.timeout.IdleState;
import io.netty.handler.timeout.IdleStateEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zicat.tributary.channel.Channel;

import java.net.InetSocketAddress;
import java.util.Random;

/** ChannelHandler. */
public class ChannelHandler extends SimpleChannelInboundHandler<byte[]> {

    private static final Logger LOG = LoggerFactory.getLogger(ChannelHandler.class);
    private static final String FAIL_RES_FORMAT =
            "failed response pong to host:{}, port:{}, cause:{}";
    private static final int CHANNEL_ERROR_CODE = -1;

    private final Random random = new Random(System.currentTimeMillis());
    private final Channel channel;
    private final boolean ack;

    public ChannelHandler(Channel channel, boolean ack) {
        this.channel = channel;
        this.ack = ack;
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

        final int partition = random.nextInt(channel.partition());
        try {
            channel.append(partition, packet);
        } catch (Throwable e) {
            LOG.error("append data error", e);
            responseChannelError(ctx);
            return;
        }
        response(packet.length, ctx);
    }

    /**
     * response received data.
     *
     * @param byteBuf byteBuf
     * @param ctx ctx
     */
    protected void response(ByteBuf byteBuf, ChannelHandlerContext ctx) {
        if (!ack) {
            return;
        }
        final ChannelFuture future = ctx.writeAndFlush(byteBuf);
        future.addListener(
                f -> {
                    if (!f.isSuccess()) {
                        final InetSocketAddress address =
                                (InetSocketAddress) ctx.channel().remoteAddress();
                        final String remoteHost = address.getAddress().getHostAddress();
                        final int remotePort = address.getPort();
                        LOG.warn(FAIL_RES_FORMAT, remoteHost, remotePort, f.cause());
                    }
                });
    }

    /**
     * response int data.
     *
     * @param length length
     * @param ctx ctx
     */
    protected void response(int length, ChannelHandlerContext ctx) {
        if (!ack) {
            return;
        }
        final ByteBuf byteBuf = ByteBufAllocator.DEFAULT.buffer(4);
        byteBuf.writeInt(length);
        response(byteBuf, ctx);
    }

    /**
     * response channel error ack.
     *
     * @param ctx ctx
     */
    protected void responseChannelError(ChannelHandlerContext ctx) {
        response(CHANNEL_ERROR_CODE, ctx);
    }
}
