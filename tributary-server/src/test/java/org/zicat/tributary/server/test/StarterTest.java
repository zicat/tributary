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

import org.junit.Assert;
import org.junit.Test;
import org.zicat.tributary.common.DefaultReadableConfig;
import org.zicat.tributary.common.IOUtils;
import org.zicat.tributary.common.TributaryRuntimeException;
import org.zicat.tributary.common.records.Record;
import org.zicat.tributary.server.HttpServer;
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

import static org.zicat.tributary.server.test.HttpServerTest.availablePorts;

/** StarterTest. */
public class StarterTest {

    @Test
    public void test() throws IOException, InterruptedException {

        final List<Record> collection = Collections.synchronizedList(new ArrayList<>());
        final List<Integer> availablePorts = availablePorts(2);
        final DefaultReadableConfig config = new DefaultReadableConfig();
        config.put("server.port", availablePorts.get(0));
        config.put("server.host", "127.0.0.1");
        config.put("source.s1.channel", "c1");
        config.put("source.s1.implement", "netty");
        config.put("source.s1.netty.host", "127.0.0.1");
        config.put("source.s1.netty.port", availablePorts.get(1));
        config.put("source.s1.netty.decoder", "lineDecoder");
        config.put("channel.c1.type", "memory");
        config.put("channel.c1.groups", "group_1");
        config.put("channel.c1.flush.period.mills", "100");
        config.put("sink.group_1.function.id", "collection_mock");
        config.put("sink.group_1.collection", collection);

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
                            "http://"
                                    + httpServer.host()
                                    + ":"
                                    + httpServer.port()
                                    + httpServer.metricsPath());
            final HttpURLConnection connection = (HttpURLConnection) url.openConnection();
            try {
                final String v =
                        org.apache.commons.io.IOUtils.toString(
                                connection.getInputStream(), StandardCharsets.UTF_8);
                Assert.assertTrue(v.contains("sink_lag{id=\"c1_group_1\""));
                Assert.assertTrue(v.contains("channel_block_cache_query_total_count{topic=\"c1\""));
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
        public void start() throws InterruptedException {
            initComponent();
            Runtime.getRuntime().addShutdownHook(new Thread(() -> IOUtils.closeQuietly(this)));
        }

        public void flushChannel() {
            channelComponent.flush();
        }

        public HttpServer httpServer() {
            return server;
        }
    }
}
