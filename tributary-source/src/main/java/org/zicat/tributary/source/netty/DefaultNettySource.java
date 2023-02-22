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

import io.netty.channel.ChannelInboundHandler;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.timeout.IdleStateHandler;
import org.zicat.tributary.channel.Channel;
import org.zicat.tributary.source.netty.ack.AckHandlerFactory;

import java.util.concurrent.atomic.AtomicInteger;

import static org.zicat.tributary.source.netty.AbstractNettySourceFactory.OPTION_NETTY_HOST;
import static org.zicat.tributary.source.netty.AbstractNettySourceFactory.OPTION_NETTY_THREADS;
import static org.zicat.tributary.source.netty.DefaultNettySourceFactory.OPTION_NETTY_IDLE_SECOND;

/** DefaultNettySource. */
public class DefaultNettySource extends AbstractNettySource {

    protected static final NettyDecoder DEFAULT_DECODER = NettyDecoder.lengthDecoder;

    protected final int idleSecond;
    protected final int partitionCount;
    protected final AtomicInteger count = new AtomicInteger();

    public DefaultNettySource(
            String host, int port, int eventThreads, Channel channel, int idleSecond) {
        super(host, port, eventThreads, channel);
        this.idleSecond = idleSecond;
        this.partitionCount = channel.partition();
    }

    public DefaultNettySource(int port, Channel channel) {
        this(
                OPTION_NETTY_HOST.defaultValue(),
                port,
                OPTION_NETTY_THREADS.defaultValue(),
                channel,
                OPTION_NETTY_IDLE_SECOND.defaultValue());
    }

    /**
     * init channel.
     *
     * @param ch ch
     */
    @Override
    protected void initChannel(SocketChannel ch, Channel channel) {
        initChannel(ch, channel, nettyDecoder());
    }

    /**
     * get netty decoder.
     *
     * @return NettyDecoder
     */
    protected NettyDecoder nettyDecoder() {
        return DEFAULT_DECODER;
    }

    /**
     * init channel.
     *
     * @param ch ch
     * @param channel channel
     * @param decoder decoder
     */
    private void initChannel(SocketChannel ch, Channel channel, NettyDecoder decoder) {
        final ChannelInboundHandler sourceDecoder = decoder.createSourceDecoder();
        final AckHandlerFactory ackHandlerFactory = decoder.ackHandlerFactory();
        ch.pipeline()
                .addLast(new IdleStateHandler(idleSecond, idleSecond, idleSecond))
                .addLast(sourceDecoder)
                .addLast(new ChannelHandler(channel, selectPartition(), ackHandlerFactory));
    }

    /**
     * select partition id.
     *
     * @return partition id
     */
    protected int selectPartition() {
        return (count.getAndIncrement() & 0x7fffffff) % partitionCount;
    }
}
