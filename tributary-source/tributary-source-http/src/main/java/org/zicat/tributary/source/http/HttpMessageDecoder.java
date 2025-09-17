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

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zicat.tributary.common.PathParams;
import org.zicat.tributary.common.records.DefaultRecords;
import org.zicat.tributary.common.records.Record;
import org.zicat.tributary.common.records.Records;
import org.zicat.tributary.source.base.netty.NettySource;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** HttpMessageDecoder. */
public class HttpMessageDecoder extends SimpleChannelInboundHandler<FullHttpRequest> {

    private static final Logger LOG = LoggerFactory.getLogger(HttpMessageDecoder.class);
    private static final byte[] EMPTY = new byte[0];
    public static final ObjectMapper MAPPER =
            new ObjectMapper().disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);

    protected static final TypeReference<List<Record>> BODY_TYPE =
            new TypeReference<List<Record>>() {};

    private static final byte[] UNAUTHORIZED =
            "Authentication required".getBytes(StandardCharsets.UTF_8);
    private static final String HTTP_HEADER_VALUE_UTF8 = "; charset=UTF-8";
    private static final String HTTP_HEADER_VALUE_APPLICATION_JSON_UTF8 =
            HttpHeaderValues.APPLICATION_JSON + HTTP_HEADER_VALUE_UTF8;
    private static final String HTTP_HEADER_VALUE_TEXT_PLAIN_UTF8 =
            HttpHeaderValues.TEXT_PLAIN + HTTP_HEADER_VALUE_UTF8;

    public static final String RESPONSE_BAD_CONTENT_TYPE =
            "content-type only support " + HTTP_HEADER_VALUE_APPLICATION_JSON_UTF8;

    public static final String RESPONSE_BAD_METHOD = "only support post request";
    private static final String HTTP_QUERY_KEY_TOPIC = "topic";
    private static final String HTTP_QUERY_KEY_PARTITION = "partition";
    public static final String RESPONSE_BAD_TOPIC_NOT_IN_PARAMS =
            HTTP_QUERY_KEY_TOPIC + " not found in params";

    protected final NettySource source;
    protected final String path;
    protected final int defaultPartition;
    protected final String authToken;

    public HttpMessageDecoder(
            NettySource source, int defaultPartition, String path, String authToken) {
        this.source = source;
        this.defaultPartition = defaultPartition;
        this.path = path;
        this.authToken = authToken;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, FullHttpRequest msg)
            throws URISyntaxException, IOException {

        final String authHeader = msg.headers().get(HttpHeaderNames.AUTHORIZATION);
        if (authToken != null && !authToken.equals(authHeader)) {
            unauthorizedResponse(ctx);
            return;
        }

        if (!HttpMethod.POST.equals(msg.method())) {
            badRequestResponse(ctx, RESPONSE_BAD_METHOD);
            return;
        }
        final PathParams pathParams = new PathParams(msg.uri());
        final String path = pathParams.path();
        if (!pathMatch(path)) {
            notFoundResponse(ctx, path);
            return;
        }
        final String contentType = msg.headers().get(HttpHeaderNames.CONTENT_TYPE);
        final String contentTypeCheck = checkContentType(contentType);
        if (contentTypeCheck != null) {
            badRequestResponse(ctx, contentTypeCheck);
            return;
        }
        final String topic = topic(pathParams);
        if (topic == null || topic.trim().isEmpty()) {
            badRequestResponse(ctx, RESPONSE_BAD_TOPIC_NOT_IN_PARAMS);
            return;
        }

        final Records records;
        try {
            final byte[] body = parseBody(msg);
            records = parseRecords(ctx, topic, contentType, httpHeaders(msg), body);
            if (records == null || records.count() == 0) {
                okResponse(ctx);
                return;
            }
            final int realPartition = realPartition(pathParams);
            source.append(realPartition, records);
            okResponse(ctx);
        } catch (Exception e) {
            LOG.error("process http request error, path: {}, topic: {}", path, topic, e);
            badRequestResponse(ctx, e.getClass().getName());
        }
    }

    /**
     * get http body.
     *
     * @param msg msg
     * @return bytes
     */
    protected byte[] parseBody(FullHttpRequest msg) {
        final ByteBuf byteBuf = msg.content();
        final byte[] body = new byte[byteBuf.readableBytes()];
        byteBuf.getBytes(byteBuf.readerIndex(), body).discardReadBytes();
        return body;
    }

    /**
     * get real partition.
     *
     * @param pathParams pathParams
     * @return int value
     */
    protected int realPartition(PathParams pathParams) {
        final String dataPartition = pathParams.params().get(HTTP_QUERY_KEY_PARTITION);
        if (dataPartition == null) {
            return defaultPartition;
        }
        try {
            final int partition = Integer.parseInt(dataPartition);
            return (partition & 0x7fffffff) % source.partition();
        } catch (NumberFormatException ignore) {
            return defaultPartition;
        }
    }

    /**
     * parse body to records.
     *
     * @param ctx ChannelHandlerContext
     * @param topic topic
     * @param contentType contentType
     * @param httpHeaders httpHeaders
     * @param body body
     * @return Records
     * @throws IOException IOException
     */
    protected Records parseRecords(
            ChannelHandlerContext ctx,
            String topic,
            String contentType,
            Map<String, String> httpHeaders,
            byte[] body)
            throws IOException {
        final List<Record> records = MAPPER.readValue(body, BODY_TYPE);
        return new DefaultRecords(topic, recordsHeaders(httpHeaders), records);
    }

    /**
     * get topic from path params.
     *
     * @param pathParams pathParams
     * @return topic
     */
    protected String topic(PathParams pathParams) {
        return pathParams.params().get(HTTP_QUERY_KEY_TOPIC);
    }

    /**
     * check content type.
     *
     * @param contentType contentType
     * @return if content type is not application/json; charset=UTF-8, return error message
     */
    protected String checkContentType(String contentType) {
        if (HTTP_HEADER_VALUE_APPLICATION_JSON_UTF8.equals(contentType)) {
            return null;
        }
        return RESPONSE_BAD_CONTENT_TYPE;
    }

    /**
     * http headers.
     *
     * @param msg msg
     * @return map
     */
    protected Map<String, String> httpHeaders(FullHttpRequest msg) {
        msg.headers().remove(HttpHeaderNames.CONTENT_TYPE);
        msg.headers().remove(HttpHeaderNames.CONTENT_LENGTH);
        msg.headers().remove(HttpHeaderNames.AUTHORIZATION);
        final Map<String, String> result = new HashMap<>();
        for (Map.Entry<String, String> entry : msg.headers()) {
            result.put(entry.getKey(), entry.getValue());
        }
        return result;
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

    /**
     * records headers.
     *
     * @param httpHeaders http headers
     * @return map
     */
    private static Map<String, byte[]> recordsHeaders(Map<String, String> httpHeaders) {
        final Map<String, byte[]> recordsHeader = new HashMap<>();
        httpHeaders.forEach((k, v) -> recordsHeader.put(k, v.getBytes(StandardCharsets.UTF_8)));
        return recordsHeader;
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
    protected void okResponse(ChannelHandlerContext ctx) {
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
                        HttpVersion.HTTP_1_1, HttpResponseStatus.INTERNAL_SERVER_ERROR, byteBuf);
        ctx.writeAndFlush(addTextPlainUtf8Headers(response));
    }

    /**
     * unauthorized response.
     *
     * @param ctx ctx
     */
    private static void unauthorizedResponse(ChannelHandlerContext ctx) {
        final ByteBuf byteBuf = ctx.alloc().buffer(UNAUTHORIZED.length);
        byteBuf.writeBytes(UNAUTHORIZED);
        final FullHttpResponse response =
                new DefaultFullHttpResponse(
                        HttpVersion.HTTP_1_1, HttpResponseStatus.UNAUTHORIZED, byteBuf);
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
