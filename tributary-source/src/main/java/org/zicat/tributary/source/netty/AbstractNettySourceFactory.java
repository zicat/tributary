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

package org.zicat.tributary.source.netty;

import org.zicat.tributary.channel.Channel;
import org.zicat.tributary.source.Source;
import org.zicat.tributary.source.SourceFactory;

import java.util.Map;

/** AbstractNettySourceFactory. */
public abstract class AbstractNettySourceFactory implements SourceFactory {

    private static final String KEY_NETTY_PORT = "netty.port";

    public static final String DEFAULT_NETTY_THREADS = "10";
    private static final String KEY_NETTY_THREADS = "netty.threads";

    private static final String DEFAULT_NETTY_HOST = "";
    private static final String KEY_NETTY_HOST = "netty.host";

    @Override
    public final Source createTributaryServer(Channel channel, Map<String, String> config) {
        final String host = config.getOrDefault(KEY_NETTY_HOST, DEFAULT_NETTY_HOST);
        final int port = Integer.parseInt(config.get(KEY_NETTY_PORT));
        final int threads =
                Integer.parseInt(config.getOrDefault(KEY_NETTY_THREADS, DEFAULT_NETTY_THREADS));
        return createAbstractTributaryServer(host, port, threads, channel, config);
    }

    /**
     * create abstract tributary server instance.
     *
     * @param host host
     * @param port port
     * @param eventThreads eventThreads.
     * @param channel channel
     * @param config config
     * @return AbstractTributaryServer
     */
    public abstract AbstractNettySource createAbstractTributaryServer(
            String host, int port, int eventThreads, Channel channel, Map<String, String> config);
}
