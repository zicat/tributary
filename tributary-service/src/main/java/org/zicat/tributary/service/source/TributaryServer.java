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

package org.zicat.tributary.service.source;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.*;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollServerSocketChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.commons.lang3.SystemUtils.IS_OS_LINUX;

/** TributaryServer. */
public abstract class TributaryServer implements Closeable {

    private static final Logger LOG = LoggerFactory.getLogger(TributaryServer.class);
    private static final String HOST_SPLIT = ",";

    protected final String host;
    protected final int port;
    protected final int eventThreads;
    protected final org.zicat.tributary.channel.Channel channel;
    protected final EventLoopGroup bossGroup;
    protected final EventLoopGroup workGroup;
    protected final ServerBootstrap serverBootstrap;
    protected List<Channel> channelList;

    private final AtomicBoolean closed = new AtomicBoolean(false);

    public TributaryServer(
            String host, int port, int eventThreads, org.zicat.tributary.channel.Channel channel) {
        this.host = host;
        this.port = port;
        this.eventThreads = eventThreads;
        this.channel = channel;
        this.bossGroup = createBossGroup();
        this.workGroup = createWorkGroup();
        this.serverBootstrap = createServerBootstrap(bossGroup, workGroup);
    }

    public void start() throws InterruptedException {
        initOptions(serverBootstrap);
        initHandlers(serverBootstrap);
        channelList = createChannelList(host);
    }

    /** init server options. */
    protected void initOptions(ServerBootstrap serverBootstrap) {
        serverBootstrap
                .option(ChannelOption.SO_BACKLOG, 256)
                .option(ChannelOption.SO_RCVBUF, 1024 * 1024)
                .childOption(ChannelOption.SO_RCVBUF, 10 * 1024 * 1024)
                .childOption(ChannelOption.SO_KEEPALIVE, true)
                .childOption(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT);
    }

    /**
     * init channel.
     *
     * @param ch ch
     */
    protected abstract void initChannel(
            SocketChannel ch, org.zicat.tributary.channel.Channel channel);

    /** init handlers. */
    private void initHandlers(ServerBootstrap serverBootstrap) {
        serverBootstrap.childHandler(
                new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) {
                        TributaryServer.this.initChannel(ch, channel);
                    }
                });
    }

    /**
     * create channel by host.
     *
     * @param host host
     */
    private Channel createChannel(String host) throws InterruptedException {
        final ChannelFuture syncFuture =
                host == null
                        ? serverBootstrap.bind(port).sync()
                        : serverBootstrap.bind(host, port).sync();
        final ChannelFuture channelFuture =
                syncFuture.addListener(
                        (future) -> {
                            if (future.isSuccess()) {
                                final String realHost = host == null ? "*" : host;
                                LOG.info(">>> TcpServer started on ip {} port {} ", realHost, port);
                            } else {
                                final String message = "TcpServer started fail on port " + port;
                                throw new RuntimeException(message, future.cause());
                            }
                        });
        return channelFuture.channel();
    }

    /**
     * init server channel list.
     *
     * @param host host
     * @return channel list
     */
    private List<Channel> createChannelList(String host) throws InterruptedException {
        final List<Channel> channelList = new ArrayList<>();
        if (host == null || host.isEmpty()) {
            channelList.add(createChannel(null));
            return channelList;
        }
        final String[] hosts = host.split(HOST_SPLIT);
        for (String h : hosts) {
            channelList.add(createChannel(h));
        }
        return channelList;
    }

    /**
     * create server bootstrap.
     *
     * @param bossGroup bossGroup
     * @param workGroup workGroup
     * @return ServerBootstrap
     */
    protected ServerBootstrap createServerBootstrap(
            EventLoopGroup bossGroup, EventLoopGroup workGroup) {
        final ServerBootstrap serverBootstrap = new ServerBootstrap();
        final Class<? extends ServerChannel> channelClass =
                IS_OS_LINUX ? EpollServerSocketChannel.class : NioServerSocketChannel.class;
        serverBootstrap.group(bossGroup, workGroup).channel(channelClass);
        return serverBootstrap;
    }

    /**
     * create work group.
     *
     * @return EventLoopGroup
     */
    private EventLoopGroup createWorkGroup() {
        return IS_OS_LINUX
                ? new EpollEventLoopGroup(eventThreads)
                : new NioEventLoopGroup(eventThreads);
    }

    /**
     * create boss groups.
     *
     * @return EventLoopGroup
     */
    protected EventLoopGroup createBossGroup() {
        return IS_OS_LINUX ? new EpollEventLoopGroup(1) : new NioEventLoopGroup(1);
    }

    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            if (channelList != null) {
                channelList.forEach(
                        f -> {
                            try {
                                f.close().sync();
                            } catch (Exception e) {
                                LOG.info("wait close channel sync fail", e);
                            }
                        });
            }
            if (workGroup != null) {
                workGroup.shutdownGracefully();
            }
            if (bossGroup != null) {
                bossGroup.shutdownGracefully();
            }
        }
    }
}
