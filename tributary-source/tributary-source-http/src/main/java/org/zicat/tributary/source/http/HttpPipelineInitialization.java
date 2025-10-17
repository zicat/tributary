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

package org.zicat.tributary.source.http;

import org.zicat.tributary.source.base.Source;
import static org.zicat.tributary.source.base.netty.EventExecutorGroups.createEventExecutorGroup;
import static org.zicat.tributary.source.http.HttpPipelineInitializationFactory.*;

import io.netty.channel.Channel;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpRequestDecoder;
import io.netty.handler.codec.http.HttpResponseEncoder;
import io.netty.util.concurrent.EventExecutorGroup;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zicat.tributary.common.config.ReadableConfig;
import org.zicat.tributary.common.util.Strings;
import org.zicat.tributary.source.base.netty.pipeline.AbstractPipelineInitialization;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Base64;

/** HttpPipelineInitialization. */
public class HttpPipelineInitialization extends AbstractPipelineInitialization {

    private static final Logger LOG = LoggerFactory.getLogger(HttpPipelineInitialization.class);
    protected final String path;
    protected final int maxContentLength;
    protected final EventExecutorGroup httpHandlerExecutorGroup;
    protected final String authToken;

    public HttpPipelineInitialization(Source source) {
        super(source);
        final ReadableConfig conf = source.config();
        this.path = formatPath(conf.get(OPTIONS_PATH));
        this.maxContentLength = maxContentLength(conf);
        this.authToken = authToken(conf, source.sourceId());
        this.httpHandlerExecutorGroup =
                createEventExecutorGroup(
                        source.sourceId() + "-httpHandler", httpWorkerThread(conf));
    }

    @Override
    public void init(Channel channel) {
        final ChannelPipeline pip = channel.pipeline();
        idleClosedChannelPipeline(pip.addLast(new HttpResponseEncoder()))
                .addLast(new HttpRequestDecoder())
                .addLast(new HttpObjectAggregator(maxContentLength))
                .addLast(httpHandlerExecutorGroup, createHttpMessageDecoder());
    }

    /**
     * authToken.
     *
     * @param username username
     * @param password password
     * @return token
     */
    protected String authToken(String username, String password) {
        if (username == null || password == null) {
            return null;
        }
        final String credentials = username + ":" + password;
        final byte[] credentialsBytes = credentials.getBytes(StandardCharsets.UTF_8);
        final String encodedCredentials = Base64.getEncoder().encodeToString(credentialsBytes);
        return "Basic " + encodedCredentials;
    }

    /**
     * username.
     *
     * @param config config
     * @return string
     */
    protected String username(ReadableConfig config) {
        return Strings.blank2Null(config.get(OPTION_HTTP_USER));
    }

    /**
     * password.
     *
     * @param config config
     * @return string
     */
    protected String password(ReadableConfig config) {
        return Strings.blank2Null(config.get(OPTION_HTTP_PASSWORD));
    }

    /**
     * maxContentLength.
     *
     * @param config config
     * @return int
     */
    protected int maxContentLength(ReadableConfig config) {
        return config.get(OPTION_MAX_CONTENT_LENGTH);
    }

    /**
     * getHttpWorkerThread.
     *
     * @param config config
     * @return int
     */
    protected int httpWorkerThread(ReadableConfig config) {
        return config.get(OPTION_HTTP_WORKER_THREADS);
    }

    /**
     * create http message decoder.
     *
     * @return HttpMessageDecoder
     */
    protected HttpMessageDecoder createHttpMessageDecoder() {
        return new HttpMessageDecoder(source, path, authToken);
    }

    /**
     * formatPath.
     *
     * @param path path
     * @return string
     */
    public static String formatPath(String path) {
        if (path == null) {
            return null;
        }
        final String trimPath = path.trim();
        return trimPath.endsWith("/") ? trimPath.substring(1, trimPath.length() - 1) : trimPath;
    }

    /**
     * authToken.
     *
     * @param conf conf
     * @param sourceId sourceId
     * @return String
     */
    private String authToken(ReadableConfig conf, String sourceId) {
        final String authToken = authToken(username(conf), password(conf));
        if (authToken == null) {
            LOG.info("Source {} not set http basic auth, will not check auth token", sourceId);
        }
        return authToken;
    }

    @Override
    public void close() throws IOException {
        if (httpHandlerExecutorGroup != null) {
            httpHandlerExecutorGroup.shutdownGracefully();
        }
    }
}
