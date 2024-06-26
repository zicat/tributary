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
import org.zicat.tributary.common.IOUtils;
import org.zicat.tributary.common.ReadableConfig;
import org.zicat.tributary.common.TributaryRuntimeException;
import org.zicat.tributary.common.records.Records;
import org.zicat.tributary.source.Source;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.zicat.tributary.source.utils.HostUtils.getInetAddress;

/** AbstractNettySource. */
public abstract class AbstractNettySource implements Source {

    private static final Logger LOG = LoggerFactory.getLogger(AbstractNettySource.class);
    private static final String HOST_SPLIT = ",";

    protected final String host;
    protected int port;
    protected final int eventThreads;
    protected final org.zicat.tributary.channel.Channel channel;
    protected final EventLoopGroup bossGroup;
    protected final EventLoopGroup workGroup;
    protected final ServerBootstrap serverBootstrap;
    protected List<Channel> channelList;
    protected final ReadableConfig config;

    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final List<String> hostNames;
    protected final String sourceId;

    public AbstractNettySource(
            String sourceId,
            ReadableConfig config,
            String host,
            int port,
            int eventThreads,
            org.zicat.tributary.channel.Channel channel) {
        this.sourceId = sourceId;
        this.config = config;
        this.host = host;
        this.port = port;
        this.eventThreads = eventThreads;
        this.channel = channel;
        this.hostNames = realHostName(host);
        this.bossGroup = createBossGroup();
        this.workGroup = createWorkGroup();
        this.serverBootstrap = createServerBootstrap(bossGroup, workGroup);
    }

    @Override
    public void start() throws InterruptedException {
        initOptions();
        initHandlers();
        channelList = createChannelList();
    }

    /** init server options. */
    protected void initOptions() {
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
    @SuppressWarnings("VulnerableCodeUsages")
    private Channel createChannel(String host) throws InterruptedException {
        final ChannelFuture syncFuture =
                host == null
                        ? serverBootstrap.bind(port).sync()
                        : serverBootstrap.bind(host, port).sync();

        final Channel serverChannel = syncFuture.channel();
        port = ((InetSocketAddress) serverChannel.localAddress()).getPort();

        if (syncFuture.isSuccess()) {
            final String realHost = host == null ? "*" : host;
            LOG.info(">>> TcpServer started on ip {}, port {} ", realHost, port);
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
     * parse host.
     *
     * @param host host
     * @return list.
     */
    private static List<String> realHostName(String host) {
        final List<String> hostName = new ArrayList<>();
        if (host == null || host.isEmpty()) {
            return hostName;
        }
        final String[] hosts = host.split(HOST_SPLIT);
        for (String h : hosts) {
            if (h != null && !h.trim().isEmpty()) {
                final InetAddress address = getInetAddress(h);
                hostName.add(address.getHostAddress());
            }
        }
        return hostName;
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
                SystemUtils.IS_OS_LINUX
                        ? EpollServerSocketChannel.class
                        : NioServerSocketChannel.class;
        serverBootstrap.group(bossGroup, workGroup).channel(channelClass);
        return serverBootstrap;
    }

    /**
     * create work group.
     *
     * @return EventLoopGroup
     */
    private EventLoopGroup createWorkGroup() {
        return SystemUtils.IS_OS_LINUX
                ? new EpollEventLoopGroup(eventThreads)
                : new NioEventLoopGroup(eventThreads);
    }

    /**
     * create boss groups.
     *
     * @return EventLoopGroup
     */
    protected EventLoopGroup createBossGroup() {
        return SystemUtils.IS_OS_LINUX ? new EpollEventLoopGroup(1) : new NioEventLoopGroup(1);
    }

    @Override
    public void append(int partition, Records records) throws IOException {
        try {
            channel.append(partition, records.toByteBuffer());
        } catch (IOException e) {
            LOG.error("append data error, close source", e);
            IOUtils.closeQuietly(this);
            throw new IOException(e);
        }
    }

    @Override
    public void flush() throws IOException {
        channel.flush();
    }

    @Override
    public String topic() {
        return channel.topic();
    }

    @Override
    public int partition() {
        return channel.partition();
    }

    @Override
    public void close() throws IOException {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        LOG.error("stop listen {}:{}", host, port);
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
        channel.flush();
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
     * get config.
     *
     * @return config
     */
    public ReadableConfig getConfig() {
        return config;
    }

    /**
     * get host names.
     *
     * @return host name.
     */
    public List<String> getHostNames() {
        return hostNames;
    }

    @Override
    public String sourceId() {
        return sourceId;
    }
}
