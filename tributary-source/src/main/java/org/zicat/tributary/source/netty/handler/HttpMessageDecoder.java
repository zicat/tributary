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

package org.zicat.tributary.source.netty.handler;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.*;
import org.zicat.tributary.common.PathParams;
import org.zicat.tributary.common.records.DefaultRecords;
import org.zicat.tributary.common.records.Record;
import org.zicat.tributary.source.netty.AbstractNettySource;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.zicat.tributary.source.utils.SourceHeaders.sourceHeaders;

/** HttpMessageDecoder. */
public class HttpMessageDecoder extends SimpleChannelInboundHandler<FullHttpRequest> {

    private static final String ENCODE = StandardCharsets.UTF_8.name();
    private static final byte[] EMPTY = new byte[0];
    private static final ObjectMapper MAPPER =
            new ObjectMapper().disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
    private static final TypeReference<List<Record>> BODY_TYPE =
            new TypeReference<List<Record>>() {};

    private static final String HTTP_HEADER_VALUE_UTF8 = "; charset=UTF-8";
    private static final String HTTP_HEADER_VALUE_APPLICATION_JSON_UTF8 =
            HttpHeaderValues.APPLICATION_JSON + HTTP_HEADER_VALUE_UTF8;
    private static final String HTTP_HEADER_VALUE_TEXT_PLAIN_UTF8 =
            HttpHeaderValues.TEXT_PLAIN + HTTP_HEADER_VALUE_UTF8;
    private static final String HTTP_HEADER_KEY_CONTENT_TYPE = "Content-Type";

    public static final String RESPONSE_BAD_CONTENT_TYPE =
            HTTP_HEADER_KEY_CONTENT_TYPE
                    + " only support "
                    + HTTP_HEADER_VALUE_APPLICATION_JSON_UTF8;

    public static final String RESPONSE_BAD_METHOD = "only support post request";
    private static final String HTTP_QUERY_KEY_TOPIC = "topic";
    public static final String RESPONSE_BAD_TOPIC_NOT_IN_PARAMS =
            HTTP_QUERY_KEY_TOPIC + " not found in params";

    public static final String RESPONSE_BAD_JSON_PARSE_FAIL = "json body parse to records fail";

    private final AbstractNettySource source;
    private final int partition;
    private final String path;

    public HttpMessageDecoder(AbstractNettySource source, int partition, String path) {
        this.source = source;
        this.partition = partition;
        this.path = path;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, FullHttpRequest msg)
            throws URISyntaxException, IOException {

        final ByteBuf byteBuf = msg.content();
        if (!HttpMethod.POST.equals(msg.method())) {
            badRequestResponse(ctx, RESPONSE_BAD_METHOD);
            return;
        }
        final PathParams pathParams = new PathParams(msg.uri());
        if (!pathMatch(pathParams.path())) {
            notFoundResponse(ctx, pathParams.path());
            return;
        }
        final Map<String, String> httpHeaders = httpHeaders(msg);
        final String contentType = httpHeaders.get(HTTP_HEADER_KEY_CONTENT_TYPE);
        if (!HTTP_HEADER_VALUE_APPLICATION_JSON_UTF8.equals(contentType)) {
            badRequestResponse(ctx, RESPONSE_BAD_CONTENT_TYPE);
            return;
        }
        final String topic = pathParams.params().get(HTTP_QUERY_KEY_TOPIC);
        if (topic == null || topic.trim().isEmpty()) {
            badRequestResponse(ctx, RESPONSE_BAD_TOPIC_NOT_IN_PARAMS);
            return;
        }

        final Map<String, byte[]> recordsHeader = recordsHeaders(httpHeaders);
        final byte[] bytes = new byte[byteBuf.readableBytes()];
        byteBuf.getBytes(byteBuf.readerIndex(), bytes).discardReadBytes();

        List<Record> records;
        try {
            records = MAPPER.readValue(bytes, BODY_TYPE);
        } catch (Exception e) {
            badRequestResponse(ctx, RESPONSE_BAD_JSON_PARSE_FAIL);
            return;
        }
        source.append(partition, new DefaultRecords(topic, recordsHeader, records));
        okResponse(ctx);
    }

    /**
     * http headers.
     *
     * @param msg msg
     * @return map
     */
    private static Map<String, String> httpHeaders(FullHttpRequest msg) {
        final Map<String, String> result = new HashMap<>();
        for (Map.Entry<String, String> entry : msg.headers()) {
            result.put(entry.getKey(), entry.getValue());
        }
        return result;
    }

    /**
     * records headers.
     *
     * @param httpHeaders http headers
     * @return map
     */
    private static Map<String, byte[]> recordsHeaders(Map<String, String> httpHeaders) {
        final long receivedTime = System.currentTimeMillis();
        final Map<String, byte[]> recordsHeader = new HashMap<>(sourceHeaders(receivedTime));
        httpHeaders.forEach(
                (k, v) -> {
                    if (k.equals(HTTP_HEADER_KEY_CONTENT_TYPE)) {
                        return;
                    }
                    if (k.equals(HttpHeaderNames.CONTENT_LENGTH.toString())) {
                        return;
                    }
                    recordsHeader.put(k, v.getBytes(StandardCharsets.UTF_8));
                });
        return recordsHeader;
    }

    /**
     * check path match.
     *
     * @param path path
     * @return if match
     */
    protected boolean pathMatch(String path) {
        if (this.path == null) {
            return true;
        }
        return this.path.equals(path);
    }

    @Override
    public void channelReadComplete(ChannelHandlerContext ctx) {
        ctx.flush();
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        ctx.close();
    }

    /**
     * response 200 response.
     *
     * @param ctx ctx
     */
    private static void okResponse(ChannelHandlerContext ctx) {
        final byte[] content = EMPTY;
        final ByteBuf byteBuf = ctx.alloc().buffer(content.length);
        byteBuf.writeBytes(content);
        final FullHttpResponse response =
                new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK, byteBuf);
        ctx.writeAndFlush(addTextPlainUtf8Headers(response));
    }

    /**
     * set bad request response.
     *
     * @param ctx ctx
     * @param message message
     */
    private static void badRequestResponse(ChannelHandlerContext ctx, String message) {
        final byte[] content = message.getBytes(StandardCharsets.UTF_8);
        final ByteBuf byteBuf = ctx.alloc().buffer(content.length);
        byteBuf.writeBytes(content);
        final FullHttpResponse response =
                new DefaultFullHttpResponse(
                        HttpVersion.HTTP_1_1, HttpResponseStatus.BAD_REQUEST, byteBuf);
        ctx.writeAndFlush(addTextPlainUtf8Headers(response));
    }

    /**
     * 404 not found.
     *
     * @param ctx ctx
     * @param path path
     */
    public static void notFoundResponse(ChannelHandlerContext ctx, String path) {
        final byte[] content = (path + " not found").getBytes(StandardCharsets.UTF_8);
        final ByteBuf byteBuf = ctx.alloc().buffer(content.length);
        byteBuf.writeBytes(content);
        final FullHttpResponse response =
                new DefaultFullHttpResponse(
                        HttpVersion.HTTP_1_1, HttpResponseStatus.NOT_FOUND, byteBuf);
        ctx.writeAndFlush(addTextPlainUtf8Headers(response));
    }

    /**
     * add text plain utf8 headers.
     *
     * @param response response
     * @return response
     */
    public static FullHttpResponse addTextPlainUtf8Headers(FullHttpResponse response) {
        response.headers().add(HttpHeaderNames.CONTENT_TYPE, HTTP_HEADER_VALUE_TEXT_PLAIN_UTF8);
        response.headers().add(HttpHeaderNames.CONNECTION, HttpHeaderValues.KEEP_ALIVE);
        response.headers().add(HttpHeaderNames.CONTENT_LENGTH, response.content().readableBytes());
        return response;
    }
}
