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

import io.prometheus.client.Collector;
import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.GaugeMetricFamily;

import org.apache.commons.io.IOUtils;
import org.junit.Assert;
import org.junit.Test;
import org.zicat.tributary.channel.AbstractChannel;
import org.zicat.tributary.common.DefaultReadableConfig;
import org.zicat.tributary.common.MetricKey;
import org.zicat.tributary.server.MetricsHttpServer;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.ServerSocket;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/** HttServerTest. */
public class MetricsHttpServerTest {

    @Test
    public void testMetricsPath() throws InterruptedException, IOException {
        final CollectorRegistry registry = new CollectorRegistry();
        new Collector() {
            @Override
            public List<MetricFamilySamples> collect() {
                final MetricKey key = AbstractChannel.KEY_READ_BYTES;
                final List<String> labels = Collections.singletonList("key");
                return Collections.singletonList(
                        new GaugeMetricFamily(key.getName(), key.getName(), labels)
                                .addMetric(Collections.singletonList("k1"), 1d));
            }
        }.register(registry);

        final DefaultReadableConfig config = new DefaultReadableConfig();
        config.put(MetricsHttpServer.OPTION_PORT, availablePort());
        try (MetricsHttpServer metricsHttpServer = new MetricsHttpServer(registry, config)) {
            metricsHttpServer.start();
            final URL url =
                    new URL(
                            "http://localhost:"
                                    + metricsHttpServer.port()
                                    + metricsHttpServer.metricsPath());
            final HttpURLConnection connection = (HttpURLConnection) url.openConnection();
            try {
                final String v =
                        IOUtils.toString(connection.getInputStream(), StandardCharsets.UTF_8);
                Assert.assertTrue(v.contains("channel_read_bytes{key=\"k1\",} 1.0"));
            } finally {
                connection.disconnect();
            }
        }
    }

    /**
     * get available port.
     *
     * @return int
     * @throws IOException IOException
     */
    public static int availablePort() throws IOException {
        return availablePorts(1).get(0);
    }

    /**
     * get available ports.
     *
     * @param count port count
     * @return list
     * @throws IOException IOException
     */
    public static List<Integer> availablePorts(int count) throws IOException {
        final List<ServerSocket> sockets = new ArrayList<>(count);
        try {
            for (int i = 0; i < count; i++) {
                sockets.add(new ServerSocket(0));
            }
            return sockets.stream().map(ServerSocket::getLocalPort).collect(Collectors.toList());
        } finally {
            sockets.forEach(org.zicat.tributary.common.IOUtils::closeQuietly);
        }
    }
}
