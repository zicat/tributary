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

package org.zicat.tributary.service.server;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.*;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollServerSocketChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.timeout.IdleStateHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.zicat.tributary.channel.utils.IOUtils;
import org.zicat.tributary.service.configuration.DynamicChannel;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.commons.lang3.SystemUtils.IS_OS_LINUX;

/** TributaryServer. */
@Component
public class TributaryServer {

    private static final Logger LOG = LoggerFactory.getLogger(TributaryServer.class);
    private static final String HOST_SPLIT = ",";

    @Value("${server.netty.port:#{8200}}")
    protected int port;

    @Value("${server.netty.host:#{null}}")
    protected String host;

    @Value("${server.netty.idle.second:#{120}}")
    protected int idleSecond;

    @Value("${server.netty.threads:#{10}}")
    protected int eventThreads;

    @Value("${server.channel.topic}")
    protected String topic;

    @Autowired DynamicChannel dynamicChannel;

    protected EventLoopGroup bossGroup;
    protected EventLoopGroup workGroup;
    protected ServerBootstrap serverBootstrap;
    protected List<Channel> channelList;
    private final AtomicBoolean closed = new AtomicBoolean(false);

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
    protected void initChannel(SocketChannel ch) {
        ch.pipeline().addLast(new IdleStateHandler(idleSecond, 0, 0));
        ch.pipeline().addLast(new HeadBodyDecoder());
        ch.pipeline().addLast(new FileChannelHandler(dynamicChannel.getChannel(topic)));
    }

    /** init handlers. */
    private void initHandlers(ServerBootstrap serverBootstrap) {
        serverBootstrap.childHandler(
                new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) {
                        TributaryServer.this.initChannel(ch);
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

    @PostConstruct
    public void init() throws InterruptedException {

        this.bossGroup = createBossGroup();
        this.workGroup = createWorkGroup();
        this.serverBootstrap = createServerBootstrap(bossGroup, workGroup);
        initOptions(serverBootstrap);
        initHandlers(serverBootstrap);
        this.channelList = createChannelList(host);
    }

    /**
     * init server channel list.
     *
     * @param host host
     * @return channel list
     * @throws InterruptedException InterruptedException
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
    protected EventLoopGroup createWorkGroup() {
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

    @PreDestroy
    public void destroy() {
        if (closed.compareAndSet(false, true)) {
            try {
                if (channelList != null) {
                    channelList.forEach(
                            f -> {
                                try {
                                    f.close().sync();
                                } catch (Exception e) {
                                    LOG.info("wait close channel sync fail", e);
                                }
                            });
                    channelList = null;
                }
                if (workGroup != null) {
                    workGroup.shutdownGracefully();
                    workGroup = null;
                }
                if (bossGroup != null) {
                    bossGroup.shutdownGracefully();
                    bossGroup = null;
                }
            } finally {
                IOUtils.closeQuietly(dynamicChannel);
            }
        }
    }
}
