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

import static org.zicat.tributary.source.base.utils.HostUtils.realHostAddress;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.*;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollServerSocketChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;

import org.apache.commons.lang3.SystemUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zicat.tributary.common.ReadableConfig;
import org.zicat.tributary.common.TributaryRuntimeException;
import org.zicat.tributary.source.base.AbstractSource;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/** AbstractNettySource. */
public abstract class AbstractNettySource extends AbstractSource {

    private static final Logger LOG = LoggerFactory.getLogger(AbstractNettySource.class);

    protected final String host;
    protected int port;
    protected final int eventThreads;
    protected final EventLoopGroup bossGroup;
    protected final EventLoopGroup workGroup;
    protected final ServerBootstrap serverBootstrap;
    protected List<Channel> channelList;

    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final List<String> hostNames;

    public AbstractNettySource(
            String sourceId,
            ReadableConfig config,
            String host,
            int port,
            int eventThreads,
            org.zicat.tributary.channel.Channel channel) {
        super(sourceId, config, channel);
        this.host = host;
        this.port = port;
        this.eventThreads = eventThreads;
        this.hostNames = realHostAddress(host);
        this.bossGroup = createBossGroup(Math.max(1, eventThreads / 4));
        this.workGroup = createWorkGroup(eventThreads);
        this.serverBootstrap = createServerBootstrap(bossGroup, workGroup);
    }

    @Override
    public void start() throws InterruptedException {
        initHandlers();
        channelList = createChannelList();
    }

    /**
     * init channel.
     *
     * @param ch ch
     */
    protected abstract void initChannel(SocketChannel ch) throws IOException;

    /** init handlers. */
    private void initHandlers() {
        serverBootstrap.childHandler(
                new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) throws IOException {
                        AbstractNettySource.this.initChannel(ch);
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

        final Channel serverChannel = syncFuture.channel();
        port = ((InetSocketAddress) serverChannel.localAddress()).getPort();

        if (syncFuture.isSuccess()) {
            LOG.info("TcpServer started on {}:{}", prettyHost(host), port);
        } else {
            final String message = "TcpServer started fail on port " + port;
            throw new TributaryRuntimeException(message, syncFuture.cause());
        }
        return syncFuture.channel();
    }

    /**
     * init server channel list.
     *
     * @return channel list
     */
    private List<Channel> createChannelList() throws InterruptedException {
        final List<Channel> channelList = new ArrayList<>();
        if (hostNames.isEmpty()) {
            channelList.add(createChannel(null));
            return channelList;
        }
        for (String h : hostNames) {
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
    public static ServerBootstrap createServerBootstrap(
            EventLoopGroup bossGroup, EventLoopGroup workGroup) {
        final ServerBootstrap serverBootstrap = new ServerBootstrap();
        final Class<? extends ServerChannel> channelClass =
                SystemUtils.IS_OS_LINUX
                        ? EpollServerSocketChannel.class
                        : NioServerSocketChannel.class;
        serverBootstrap
                .group(bossGroup, workGroup)
                .channel(channelClass)
                .option(ChannelOption.SO_BACKLOG, 256)
                .option(ChannelOption.SO_RCVBUF, 1024 * 1024)
                .childOption(ChannelOption.SO_RCVBUF, 10 * 1024 * 1024)
                .childOption(ChannelOption.SO_KEEPALIVE, true)
                .childOption(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT);
        return serverBootstrap;
    }

    /**
     * create work group.
     *
     * @return EventLoopGroup
     */
    public static EventLoopGroup createWorkGroup(int eventThreads) {
        return SystemUtils.IS_OS_LINUX
                ? new EpollEventLoopGroup(eventThreads)
                : new NioEventLoopGroup(eventThreads);
    }

    /**
     * create boss groups.
     *
     * @return EventLoopGroup
     */
    public static EventLoopGroup createBossGroup(int nThreads) {
        return SystemUtils.IS_OS_LINUX
                ? new EpollEventLoopGroup(nThreads)
                : new NioEventLoopGroup(nThreads);
    }

    @Override
    public void close() throws IOException {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        if (channelList != null) {
            channelList.forEach(
                    f -> {
                        try {
                            f.close().sync();
                        } catch (Exception e) {
                            LOG.info("wait close channel sync fail", e);
                        }
                    });
            LOG.info("close netty source listen {}:{}", prettyHost(host), port);
        }
        if (workGroup != null) {
            workGroup.shutdownGracefully();
        }
        if (bossGroup != null) {
            bossGroup.shutdownGracefully();
        }
        super.close();
    }

    private String prettyHost(String host) {
        return host == null || host.isEmpty() ? "*" : host;
    }

    /**
     * get port.
     *
     * @return port
     */
    public int getPort() {
        return port;
    }

    /**
     * get hosts.
     *
     * @return hosts
     */
    public String getHost() {
        return host;
    }

    /**
     * get host names.
     *
     * @return host name.
     */
    public List<String> getHostNames() {
        return hostNames;
    }

    /**
     * get event threads.
     *
     * @return threads
     */
    public int getEventThreads() {
        return eventThreads;
    }
}
