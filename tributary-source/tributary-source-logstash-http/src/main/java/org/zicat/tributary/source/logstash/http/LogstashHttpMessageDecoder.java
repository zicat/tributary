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

import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.*;

import org.zicat.tributary.common.PathParams;
import org.zicat.tributary.common.records.*;
import org.zicat.tributary.source.base.netty.NettySource;
import org.zicat.tributary.source.http.HttpMessageDecoder;
import org.zicat.tributary.source.logstash.base.Message;
import org.zicat.tributary.source.logstash.base.MessageFilterFactory;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/** LogstashHttpMessageDecoder. */
public class LogstashHttpMessageDecoder extends HttpMessageDecoder {

    private static final String KEY_TAGS = "tags";
    private static final byte[] OK_RESPONSE = "ok".getBytes(StandardCharsets.UTF_8);
    private static final Map<String, String> EMPTY_HEADERS = new HashMap<>();
    private static final String DEFAULT_REMOTE_HOST = "0:0:0:0:0:0:0:1";
    protected final Codec defaultCodec;
    protected final String remoteHostTargetField;
    protected final String requestHeadersTargetField;
    protected final List<String> tags;
    protected final MessageFilterFactory messageFilterFactory;
    protected int offset;

    public LogstashHttpMessageDecoder(
            NettySource source,
            int defaultPartition,
            String path,
            String authToken,
            Codec defaultCodec,
            String remoteHostTargetField,
            String requestHeadersTargetField,
            List<String> tags,
            MessageFilterFactory messageFilterFactory) {
        super(source, defaultPartition, path, authToken);
        this.defaultCodec = defaultCodec;
        this.remoteHostTargetField = remoteHostTargetField;
        this.requestHeadersTargetField = requestHeadersTargetField;
        this.tags = tags;
        this.messageFilterFactory = messageFilterFactory;
    }

    @Override
    protected Iterable<Records> parseRecords(
            ChannelHandlerContext ctx,
            String topic,
            String contentType,
            Map<String, String> recordsHeader,
            byte[] body)
            throws Exception {
        final List<Map<String, Object>> dataList = findCodec(contentType).encode(body);
        final Iterator<Map<String, Object>> sourceIt = dataList.iterator();
        final Iterator<Message<Object>> targetIt =
                new Iterator<Message<Object>>() {
                    @Override
                    public boolean hasNext() {
                        return sourceIt.hasNext();
                    }

                    @Override
                    public Message<Object> next() {
                        Map<String, Object> data = sourceIt.next();
                        if (requestHeadersTargetField != null) {
                            data.put(requestHeadersTargetField, recordsHeader);
                        }
                        if (remoteHostTargetField != null) {
                            data.put(remoteHostTargetField, getRemoteHost(ctx));
                        }
                        if (tags != null && !tags.isEmpty()) {
                            data.put(KEY_TAGS, tags);
                        }
                        return new Message<>(offset++, data);
                    }
                };
        return messageFilterFactory.getMessageFilter().convert(topic, targetIt);
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

    /**
     * find codec.
     *
     * @param contentType contentType
     * @return Codec
     */
    private Codec findCodec(String contentType) {
        if (contentType == null || contentType.isEmpty()) {
            return defaultCodec;
        }
        if (contentType.contains(HttpHeaderValues.APPLICATION_JSON)) {
            return Codec.JSON;
        } else if (contentType.contains(HttpHeaderValues.TEXT_PLAIN)
                || contentType.contains(HttpHeaderValues.APPLICATION_X_WWW_FORM_URLENCODED)) {
            return Codec.PLAIN;
        } else {
            return defaultCodec;
        }
    }

    @Override
    protected void okResponse(ChannelHandlerContext ctx) {
        http1_1Response(ctx, HttpResponseStatus.OK, OK_RESPONSE);
    }

    @Override
    protected String topic(PathParams pathParams) {
        final String superTopic = super.topic(pathParams);
        return superTopic == null ? source.topic() : superTopic;
    }

    @Override
    protected String checkContentType(String contentType) {
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
