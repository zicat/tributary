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

import static org.zicat.tributary.common.HostUtils.getHostAddresses;
import org.zicat.tributary.common.IOUtils;
import org.zicat.tributary.source.base.netty.pipeline.PipelineInitialization;

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
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/** NettySource. */
public abstract class NettySource extends AbstractSource {

    private static final Logger LOG = LoggerFactory.getLogger(NettySource.class);

    protected final AtomicBoolean closed = new AtomicBoolean(false);
    protected final int port;
    protected final int eventThreads;
    protected final EventLoopGroup bossGroup;
    protected final EventLoopGroup workGroup;
    protected final ServerBootstrap serverBootstrap;
    protected final PipelineInitialization pipelineInitialization;
    protected final List<Channel> channelList;
    protected final List<String> hostNames;

    public NettySource(
            String sourceId,
            ReadableConfig config,
            org.zicat.tributary.channel.Channel channel,
            List<String> hosts,
            int port,
            int eventThreads)
            throws Exception {
        super(sourceId, config, channel);
        this.port = port;
        this.eventThreads = eventThreads;
        this.hostNames = getHostAddresses(hosts);
        try {
            this.bossGroup = createBossGroup(Math.max(1, eventThreads / 4));
            this.workGroup = createWorkGroup(eventThreads);
            this.serverBootstrap = createServerBootstrap(bossGroup, workGroup);
            this.pipelineInitialization = createPipelineInitialization();
            this.serverBootstrap.childHandler(
                    new ChannelInitializer<SocketChannel>() {
                        @Override
                        protected void initChannel(SocketChannel ch) throws Exception {
                            pipelineInitialization.init(ch);
                        }
                    });
            this.channelList = createChannelList();
        } catch (Exception e) {
            IOUtils.closeQuietly(this);
            throw e;
        }
    }

    /**
     * get netty decoder.
     *
     * @return NettyDecoder
     */
    protected abstract PipelineInitialization createPipelineInitialization() throws Exception;

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
        if (!syncFuture.isSuccess()) {
            throw new TributaryRuntimeException(
                    "TcpServer started fail on port " + port, syncFuture.cause());
        }
        LOG.info("TcpServer started on {}:{}", prettyHost(host), port);
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
        try {
            closeServerChannelsAndGroup();
        } finally {
            try {
                super.close();
            } finally {
                IOUtils.closeQuietly(pipelineInitialization);
            }
        }
    }

    private void closeServerChannelsAndGroup() {
        if (channelList != null) {
            channelList.forEach(NettySource::closeChannelSync);
            for (String host : hostNames) {
                LOG.info("close netty source listen {}:{}", prettyHost(host), port);
            }
        }
        if (workGroup != null) {
            workGroup.shutdownGracefully();
        }
        if (bossGroup != null) {
            bossGroup.shutdownGracefully();
        }
    }

    /**
     * pretty host.
     *
     * @param host host
     * @return String
     */
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

    /**
     * close channel sync.
     *
     * @param channel channel
     */
    private static void closeChannelSync(Channel channel) {
        if (channel == null) {
            return;
        }
        try {
            channel.close().sync();
        } catch (Exception e) {
            LOG.warn("wait close channel sync fail", e);
        }
    }
}
