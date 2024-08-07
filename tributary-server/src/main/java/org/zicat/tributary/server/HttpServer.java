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

package org.zicat.tributary.server;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.Unpooled;
import io.netty.channel.*;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.http.*;
import io.netty.handler.stream.ChunkedWriteHandler;
import io.netty.util.CharsetUtil;
import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.exporter.common.TextFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zicat.tributary.common.*;

import java.io.Closeable;
import java.io.IOException;
import java.io.StringWriter;
import java.io.Writer;
import java.util.Collections;
import java.util.List;

import static org.zicat.tributary.source.netty.AbstractNettySource.*;
import static org.zicat.tributary.source.netty.handler.HttpMessageDecoder.addTextPlainUtf8Headers;
import static org.zicat.tributary.source.netty.handler.HttpMessageDecoder.notFoundResponse;
import static org.zicat.tributary.source.utils.HostUtils.realHostAddress;

/** HttpServer. */
public class HttpServer implements Closeable {
    private static final Logger LOG = LoggerFactory.getLogger(HttpServer.class);
    public static final int MAX_CONTENT_LENGTH = 10240;
    public static final ConfigOption<Integer> OPTION_PORT =
            ConfigOptions.key("port").integerType().defaultValue(8765);

    public static final ConfigOption<String> OPTION_HOST =
            ConfigOptions.key("host").stringType().defaultValue(".*");
    public static final ConfigOption<Integer> OPTION_THREADS =
            ConfigOptions.key("threads").integerType().defaultValue(5);
    public static final ConfigOption<String> OPTION_METRICS_PATH =
            ConfigOptions.key("metrics.path").stringType().defaultValue("/metrics");

    private final int port;
    private final String host;
    private final String metricsPath;
    private final CollectorRegistry registry;

    private transient Channel channel;
    private final EventLoopGroup bossGroup;
    private final EventLoopGroup workerGroup;

    public HttpServer(CollectorRegistry registry, ReadableConfig serverConfig) {
        this.registry = registry;
        this.port = serverConfig.get(OPTION_PORT);
        final String hostPattern = serverConfig.get(OPTION_HOST);
        final List<String> hosts = realHostAddress(hostPattern);
        if (hosts.isEmpty()) {
            throw new IllegalStateException("Host not found by config " + hostPattern);
        }
        this.host = hosts.get(0);
        if (hosts.size() > 1) {
            LOG.warn("Multiple hosts " + hosts + ", use the first " + host);
        }
        this.metricsPath = serverConfig.get(OPTION_METRICS_PATH);
        final int threads = serverConfig.get(OPTION_THREADS);
        this.bossGroup = createBossGroup(Math.max(1, threads / 4));
        this.workerGroup = createWorkGroup(threads);
    }

    /** start. */
    @SuppressWarnings("VulnerableCodeUsages")
    public void start() throws InterruptedException {
        final ServerBootstrap b = createServerBootstrap(bossGroup, workerGroup);
        b.childHandler(
                new ChannelInitializer<SocketChannel>() {
                    @Override
                    public void initChannel(SocketChannel ch) {
                        ch.pipeline().addLast(new HttpRequestDecoder());
                        ch.pipeline().addLast(new HttpObjectAggregator(MAX_CONTENT_LENGTH));
                        ch.pipeline().addLast(new HttpResponseEncoder());
                        ch.pipeline().addLast(new ChunkedWriteHandler());
                        ch.pipeline().addLast(new HttpServerHandler(registry, metricsPath));
                    }
                });
        this.channel = b.bind(host, port).sync().channel();
        LOG.info("HttpServer started on {}:{}", host, port);
    }

    @Override
    public void close() {
        try {
            if (channel != null) {
                channel.close().sync();
                LOG.info("close http server listen {}:{}", host, port);
            }
        } catch (InterruptedException e) {
            throw new TributaryRuntimeException(e);
        } finally {
            workerGroup.shutdownGracefully();
            bossGroup.shutdownGracefully();
        }
    }

    /** HttpServerHandler. */
    public static class HttpServerHandler extends SimpleChannelInboundHandler<FullHttpRequest> {

        private final CollectorRegistry registry;
        private final String path;

        public HttpServerHandler(CollectorRegistry registry, String path) {
            this.registry = registry;
            this.path = path;
        }

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, FullHttpRequest req)
                throws Exception {

            final PathParams pathParams = new PathParams(req.uri());
            if (!path.equalsIgnoreCase(pathParams.path())) {
                notFoundResponse(ctx, pathParams.path());
                return;
            }

            final FullHttpResponse response =
                    new DefaultFullHttpResponse(
                            HttpVersion.HTTP_1_1,
                            HttpResponseStatus.OK,
                            Unpooled.copiedBuffer(metrics(), CharsetUtil.UTF_8));
            ctx.writeAndFlush(addTextPlainUtf8Headers(response));
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            ctx.close();
        }

        /**
         * metrics.
         *
         * @return metrics
         * @throws IOException IOException
         */
        private String metrics() throws IOException {
            try (Writer writer = new StringWriter()) {
                TextFormat.write004(
                        writer, registry.filteredMetricFamilySamples(Collections.emptySet()));
                writer.flush();
                return writer.toString();
            }
        }
    }

    /**
     * get host.
     *
     * @return string
     */
    public String host() {
        return host;
    }

    /**
     * get port.
     *
     * @return int
     */
    public int port() {
        return port;
    }

    public String metricsPath() {
        return metricsPath;
    }
}
