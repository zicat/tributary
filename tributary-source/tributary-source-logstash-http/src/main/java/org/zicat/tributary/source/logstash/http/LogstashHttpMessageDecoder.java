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

package org.zicat.tributary.source.logstash.http;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.*;

import org.zicat.tributary.common.PathParams;
import org.zicat.tributary.common.records.*;
import org.zicat.tributary.source.base.netty.AbstractNettySource;
import org.zicat.tributary.source.http.HttpMessageDecoder;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** LogstashHttpMessageDecoder. */
public class LogstashHttpMessageDecoder extends HttpMessageDecoder {

    private static final String KEY_TAGS = "tags";
    private static final byte[] OK_RESPONSE = "ok".getBytes(StandardCharsets.UTF_8);
    private static final Map<String, String> EMPTY_HEADERS = new HashMap<>();
    private static final String DEFAULT_REMOTE_HOST = "0:0:0:0:0:0:0:1";
    protected final Codec codec;
    protected final String remoteHostTargetField;
    protected final String requestHeadersTargetField;
    protected final List<String> tags;

    public LogstashHttpMessageDecoder(
            AbstractNettySource source,
            int defaultPartition,
            String path,
            String authToken,
            Codec codec,
            String remoteHostTargetField,
            String requestHeadersTargetField,
            List<String> tags) {
        super(source, defaultPartition, path, authToken);
        this.codec = codec;
        this.remoteHostTargetField = remoteHostTargetField;
        this.requestHeadersTargetField = requestHeadersTargetField;
        this.tags = tags;
    }

    @Override
    protected Records parseRecords(
            ChannelHandlerContext ctx, String topic, Map<String, String> recordsHeader, byte[] body)
            throws IOException {
        final Map<String, Object> data = new HashMap<>();
        codec.encode(data, body);
        if (requestHeadersTargetField != null) {
            data.put(requestHeadersTargetField, recordsHeader);
        }
        if (remoteHostTargetField != null) {
            data.put(remoteHostTargetField, getRemoteHost(ctx));
        }
        if (tags != null && !tags.isEmpty()) {
            data.put(KEY_TAGS, tags);
        }
        final Record record = new DefaultRecord(MAPPER.writeValueAsBytes(data));
        return new SingleRecords(topic, record);
    }

    /**
     * Get remote host from channel handler context.
     *
     * @param ctx ctx
     * @return string
     */
    private String getRemoteHost(ChannelHandlerContext ctx) {
        SocketAddress remoteAddress = ctx.channel().remoteAddress();
        if (remoteAddress instanceof InetSocketAddress) {
            InetSocketAddress inetSocketAddress = (InetSocketAddress) remoteAddress;
            return inetSocketAddress.getAddress().getHostAddress();
        }
        return DEFAULT_REMOTE_HOST;
    }

    @Override
    protected void okResponse(ChannelHandlerContext ctx) {
        final byte[] content = OK_RESPONSE;
        final ByteBuf byteBuf = ctx.alloc().buffer(content.length);
        byteBuf.writeBytes(content);
        final FullHttpResponse response =
                new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK, byteBuf);
        ctx.writeAndFlush(addTextPlainUtf8Headers(response));
    }

    @Override
    protected String topic(PathParams pathParams) {
        return source.topic();
    }

    @Override
    protected String checkContentType(FullHttpRequest msg) {
        return null;
    }

    @Override
    protected boolean pathMatch(String path) {
        return true;
    }

    @Override
    protected Map<String, String> httpHeaders(FullHttpRequest msg) {
        return requestHeadersTargetField == null ? EMPTY_HEADERS : super.httpHeaders(msg);
    }
}
