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
import org.zicat.tributary.source.base.Source;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
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

    public static final String RESPONSE_BAD_METHOD = "only support post or put request";
    private static final String HTTP_QUERY_KEY_TOPIC = "topic";
    private static final String HTTP_QUERY_KEY_PARTITION = "partition";
    public static final String RESPONSE_BAD_TOPIC_NOT_IN_PARAMS =
            HTTP_QUERY_KEY_TOPIC + " not found in params";

    protected final Source source;
    protected final String path;
    protected final String authToken;

    public HttpMessageDecoder(Source source, String path, String authToken) {
        this.source = source;
        this.path = path;
        this.authToken = authToken;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, FullHttpRequest msg) throws Exception {

        final String authHeader = msg.headers().get(HttpHeaderNames.AUTHORIZATION);
        if (authToken != null && !authToken.equals(authHeader)) {
            unauthorizedResponse(ctx);
            return;
        }

        if (!(HttpMethod.POST.equals(msg.method()) || HttpMethod.PUT.equals(msg.method()))) {
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

        final byte[] body = parseBody(msg);
        final Map<String, String> headers = httpHeaders(msg);
        final Iterable<Records> recordsIt = parseRecords(ctx, topic, contentType, headers, body);
        if (recordsIt == null) {
            okResponse(ctx);
            return;
        }

        final Integer partition = partition(pathParams);
        for (Records records : recordsIt) {
            source.append(partition, records);
        }
        okResponse(ctx);
    }

    /**
     * get http body.
     *
     * @param msg msg
     * @return bytes
     */
    protected byte[] parseBody(FullHttpRequest msg) throws IOException {
        final ByteBuf byteBuf = msg.content();
        final byte[] body = new byte[byteBuf.readableBytes()];
        byteBuf.getBytes(byteBuf.readerIndex(), body).discardReadBytes();
        String contentEncoding = msg.headers().get(HttpHeaderNames.CONTENT_ENCODING);
        return Extraction.getExtraction(contentEncoding).extract(body);
    }

    /**
     * get real partition.
     *
     * @param pathParams pathParams
     * @return int value
     */
    protected Integer partition(PathParams pathParams) {
        final String dataPartition = pathParams.params().get(HTTP_QUERY_KEY_PARTITION);
        if (dataPartition == null) {
            return null;
        }
        try {
            final long partition = Long.parseLong(dataPartition);
            return (((int) partition) & 0x7fffffff) % source.partition();
        } catch (NumberFormatException ignore) {
            return null;
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
    protected Iterable<Records> parseRecords(
            ChannelHandlerContext ctx,
            String topic,
            String contentType,
            Map<String, String> httpHeaders,
            byte[] body)
            throws Exception {
        final List<Record> records = MAPPER.readValue(body, BODY_TYPE);
        return Collections.singletonList(
                new DefaultRecords(topic, recordsHeaders(httpHeaders), records));
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
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        LOG.error("caught exception", cause);
        if (!ctx.channel().isActive()) {
            return;
        }
        internalServerErrorResponse(ctx, cause.getClass().getName());
    }

    /**
     * response 200 response.
     *
     * @param ctx ctx
     */
    protected void okResponse(ChannelHandlerContext ctx) {
        http1_1TextResponse(ctx, HttpResponseStatus.OK, EMPTY);
    }

    /**
     * set internal server error response.
     *
     * @param ctx ctx
     * @param message message
     */
    public static void internalServerErrorResponse(ChannelHandlerContext ctx, String message) {
        final byte[] content = message.getBytes(StandardCharsets.UTF_8);
        http1_1TextResponse(ctx, HttpResponseStatus.INTERNAL_SERVER_ERROR, content);
    }

    /**
     * set bad request response.
     *
     * @param ctx ctx
     * @param message message
     */
    public static void badRequestResponse(ChannelHandlerContext ctx, String message) {
        final byte[] content = message.getBytes(StandardCharsets.UTF_8);
        http1_1TextResponse(ctx, HttpResponseStatus.BAD_REQUEST, content);
    }

    /**
     * unauthorized response.
     *
     * @param ctx ctx
     */
    public static void unauthorizedResponse(ChannelHandlerContext ctx) {
        http1_1TextResponse(ctx, HttpResponseStatus.UNAUTHORIZED, UNAUTHORIZED);
    }

    /**
     * 404 not found.
     *
     * @param ctx ctx
     * @param path path
     */
    public static void notFoundResponse(ChannelHandlerContext ctx, String path) {
        final byte[] content = (path + " not found").getBytes(StandardCharsets.UTF_8);
        http1_1TextResponse(ctx, HttpResponseStatus.NOT_FOUND, content);
    }

    /**
     * http 1.1 response.
     *
     * @param ctx ctx
     * @param status status
     * @param headers headers
     * @param message message
     */
    public static void http1_1Response(
            ChannelHandlerContext ctx,
            HttpResponseStatus status,
            HttpHeaders headers,
            byte[] message) {
        final ByteBuf byteBuf = ctx.alloc().buffer(message.length);
        byteBuf.writeBytes(message);
        final FullHttpResponse response =
                new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, status, byteBuf);
        if (headers != null) {
            response.headers().add(headers);
        }
        ctx.writeAndFlush(response);
    }

    /**
     * http 1.1 response.
     *
     * @param ctx ctx
     * @param status status
     * @param message message
     */
    public static void http1_1TextResponse(
            ChannelHandlerContext ctx, HttpResponseStatus status, byte[] message) {
        http1_1Response(ctx, status, textPlainUtf8Headers(message.length), message);
    }

    /**
     * http 1.1 json response.
     *
     * @param ctx ctx
     * @param status status
     * @param message message
     */
    public static void http1_1JsonResponse(
            ChannelHandlerContext ctx, HttpResponseStatus status, byte[] message) {
        http1_1Response(ctx, status, applicationJsonUtf8Headers(message.length), message);
    }

    public static void http1_1JsonOkResponse(ChannelHandlerContext ctx, byte[] message) {
        http1_1JsonResponse(ctx, HttpResponseStatus.OK, message);
    }

    /**
     * add text plain utf8 headers.
     *
     * @param contentLength contentLength
     * @return response
     */
    public static HttpHeaders textPlainUtf8Headers(int contentLength) {
        return new DefaultHttpHeaders()
                .add(HttpHeaderNames.CONTENT_TYPE, HTTP_HEADER_VALUE_TEXT_PLAIN_UTF8)
                .add(HttpHeaderNames.CONNECTION, HttpHeaderValues.KEEP_ALIVE)
                .add(HttpHeaderNames.CONTENT_LENGTH, contentLength);
    }

    public static HttpHeaders applicationJsonUtf8Headers(int contentLength) {
        return new DefaultHttpHeaders()
                .add(HttpHeaderNames.CONTENT_TYPE, HTTP_HEADER_VALUE_APPLICATION_JSON_UTF8)
                .add(HttpHeaderNames.CONNECTION, HttpHeaderValues.KEEP_ALIVE)
                .add(HttpHeaderNames.CONTENT_LENGTH, contentLength);
    }
}
