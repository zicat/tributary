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

package org.zicat.tributary.server.test;

import org.zicat.tributary.common.config.ReadableConfigConfigBuilder;
import org.zicat.tributary.common.config.ReadableConfig;
import static org.zicat.tributary.server.rest.DispatcherHttpHandlerBuilder.OPTION_METRICS_PATH;
import static org.zicat.tributary.server.test.HttpServerTest.availablePorts;

import org.junit.Assert;
import org.junit.Test;
import org.zicat.tributary.common.util.IOUtils;
import org.zicat.tributary.common.exception.TributaryRuntimeException;
import org.zicat.tributary.common.records.Record;
import org.zicat.tributary.server.rest.HttpServer;
import org.zicat.tributary.server.Starter;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.Socket;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

/** StarterTest. */
public class StarterTest {

    @Test
    public void test() throws IOException, InterruptedException {

        final List<Record> collection = Collections.synchronizedList(new ArrayList<>());
        final List<Integer> availablePorts = availablePorts(2);
        final ReadableConfig config =
                new ReadableConfigConfigBuilder()
                        .addConfig("server.port", availablePorts.get(0))
                        .addConfig("server.host-pattern", "127.0.0.1")
                        .addConfig("source.s1.channel", "c1")
                        .addConfig("source.s1.implement", "netty")
                        .addConfig("source.s1.netty.host-patterns", "127.0.0.1")
                        .addConfig("source.s1.netty.port", availablePorts.get(1))
                        .addConfig("source.s1.netty.decoder", "line")
                        .addConfig("channel.c1.type", "memory")
                        .addConfig("channel.c1.groups", "group_1")
                        .addConfig("channel.c1.flush.period", "100ms")
                        .addConfig("sink.group_1.function.id", "collection_mock")
                        .addConfig("sink.group_1.collection", collection)
                        .build();

        try (final StarterMock starter = new StarterMock(config.toProperties())) {
            starter.start();

            // check data in sink
            try (Socket socket = new Socket("127.0.0.1", availablePorts.get(1))) {
                socket.getOutputStream().write("aaa\n".getBytes(StandardCharsets.UTF_8));
                socket.getOutputStream().flush();
                starter.flushChannel();
                Thread.sleep(500);
                Assert.assertEquals("aaa", new String(collection.get(0).value()));
            }

            final HttpServer httpServer = starter.httpServer();
            // check metrics
            final URL url =
                    new URL(
                            "http://localhost:"
                                    + httpServer.port()
                                    + OPTION_METRICS_PATH.defaultValue());
            final HttpURLConnection connection = (HttpURLConnection) url.openConnection();
            try {
                final String v =
                        org.apache.commons.io.IOUtils.toString(
                                connection.getInputStream(), StandardCharsets.UTF_8);
                Assert.assertTrue(v.contains("tributary_sink_lag"));
                Assert.assertTrue(v.contains("tributary_channel_block_cache_query_total_count"));
            } finally {
                connection.disconnect();
            }
        } catch (Exception e) {
            throw new TributaryRuntimeException(e);
        }
    }

    /** StarterMock. */
    private static class StarterMock extends Starter {

        public StarterMock(Properties properties) {
            super(properties);
        }

        @Override
        public void start() throws Exception {
            initComponent();
            Runtime.getRuntime().addShutdownHook(new Thread(() -> IOUtils.closeQuietly(this)));
        }

        public void flushChannel() {
            channelComponent.flush();
        }

        public HttpServer httpServer() {
            return httpServer;
        }
    }
}
