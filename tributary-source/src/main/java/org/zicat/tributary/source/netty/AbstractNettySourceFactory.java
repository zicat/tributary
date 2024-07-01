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
import org.zicat.tributary.common.ConfigOption;
import org.zicat.tributary.common.ConfigOptions;
import org.zicat.tributary.common.ReadableConfig;
import org.zicat.tributary.source.Source;
import org.zicat.tributary.source.SourceFactory;

/** AbstractNettySourceFactory. */
public abstract class AbstractNettySourceFactory implements SourceFactory {

    public static final ConfigOption<Integer> OPTION_NETTY_PORT =
            ConfigOptions.key("netty.port")
                    .integerType()
                    .description("netty port")
                    .noDefaultValue();

    public static final ConfigOption<Integer> OPTION_NETTY_THREADS =
            ConfigOptions.key("netty.threads")
                    .integerType()
                    .description("netty event loop threads count")
                    .defaultValue(10);

    public static final ConfigOption<String> OPTION_NETTY_HOST =
            ConfigOptions.key("netty.host")
                    .stringType()
                    .description("netty host to register")
                    .defaultValue("");

    @Override
    public final Source createSource(String sourceId, Channel channel, ReadableConfig config)
            throws Exception {
        final String host = config.get(OPTION_NETTY_HOST);
        final int port = config.get(OPTION_NETTY_PORT);
        final int threads = config.get(OPTION_NETTY_THREADS);
        return createNettySource(sourceId, host, port, threads, channel, config);
    }

    /**
     * create netty source instance.
     *
     * @param sourceId sourceId
     * @param host host
     * @param port port
     * @param eventThreads eventThreads.
     * @param channel channel
     * @param config config
     * @return AbstractNettySource
     */
    public abstract AbstractNettySource createNettySource(
            String sourceId,
            String host,
            int port,
            int eventThreads,
            Channel channel,
            ReadableConfig config)
            throws Exception;
}
