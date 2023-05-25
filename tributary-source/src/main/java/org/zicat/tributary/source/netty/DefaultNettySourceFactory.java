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

/** DefaultNettySourceFactory. */
public class DefaultNettySourceFactory extends AbstractNettySourceFactory {

    public static final ConfigOption<Integer> OPTION_NETTY_IDLE_SECOND =
            ConfigOptions.key("netty.idle.second")
                    .integerType()
                    .description("max wait to close when channel idle over this param")
                    .defaultValue(120);

    public static final ConfigOption<String> OPTION_NETTY_DECODER =
            ConfigOptions.key("netty.decoder")
                    .stringType()
                    .description("set netty streaming decoder, values[lengthDecoder,lineDecoder]")
                    .defaultValue("lengthDecoder");

    @Override
    public String identity() {
        return "netty";
    }

    @Override
    public AbstractNettySource createNettySource(
            String host, int port, int eventThreads, Channel channel, ReadableConfig config) {

        final int idleSecond = config.get(OPTION_NETTY_IDLE_SECOND);
        final NettyDecoder nettyDecoder = NettyDecoder.valueOf(config.get(OPTION_NETTY_DECODER));
        return new DefaultNettySource(host, port, eventThreads, channel, idleSecond) {
            @Override
            protected NettyDecoder nettyDecoder() {
                return nettyDecoder;
            }
        };
    }
}
