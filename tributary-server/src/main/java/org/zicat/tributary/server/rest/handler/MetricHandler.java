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

package org.zicat.tributary.server.rest.handler;

import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.exporter.common.TextFormat;
import org.zicat.tributary.source.http.HttpMessageDecoder;

import java.io.IOException;
import java.io.StringWriter;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.util.Collections;

/** MetricRestHandler. */
public class MetricHandler implements RestHandler {

    private final CollectorRegistry registry;

    public MetricHandler(CollectorRegistry registry) {
        this.registry = registry;
    }

    @Override
    public void handleHttpRequest(ChannelHandlerContext ctx, FullHttpRequest req) throws Exception {
        HttpMessageDecoder.http1_1TextResponse(ctx, HttpResponseStatus.OK, metrics().getBytes(StandardCharsets.UTF_8));
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
