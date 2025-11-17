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

package org.zicat.tributary.server.rest;

import io.netty.channel.ChannelHandler.Sharable;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.FullHttpRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zicat.tributary.common.PathParams;
import org.zicat.tributary.server.rest.handler.RestHandler;
import static org.zicat.tributary.source.http.HttpMessageDecoder.internalServerErrorResponse;
import static org.zicat.tributary.source.http.HttpMessageDecoder.notFoundResponse;

import java.util.Map;

/** DispatcherHttpHandler. */
@Sharable
public class DispatcherHttpHandler extends SimpleChannelInboundHandler<FullHttpRequest> {

    private static final Logger LOG = LoggerFactory.getLogger(DispatcherHttpHandler.class);

    private final Map<String, RestHandler> requestMapping;

    public DispatcherHttpHandler(Map<String, RestHandler> requestMapping) {
        this.requestMapping = requestMapping;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, FullHttpRequest req) throws Exception {
        final PathParams pathParams = new PathParams(req.uri());
        final RestHandler restHandler = requestMapping.get(pathParams.path());
        if (restHandler == null) {
            notFoundResponse(ctx, pathParams.path());
            return;
        }
        restHandler.handleHttpRequest(ctx, req);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        LOG.error("unexpected exception", cause);
        if (!ctx.channel().isActive()) {
            return;
        }
        internalServerErrorResponse(ctx, cause.getClass().getName());
    }
}
