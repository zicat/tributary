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

package org.zicat.tributary.source.base.netty;

import org.zicat.tributary.channel.Channel;
import org.zicat.tributary.common.config.ConfigOption;
import org.zicat.tributary.common.config.ConfigOptions;
import static org.zicat.tributary.common.config.ConfigOptions.COMMA_SPLIT_HANDLER;
import org.zicat.tributary.common.config.ReadableConfig;
import org.zicat.tributary.common.SpiFactory;
import org.zicat.tributary.source.base.Source;
import org.zicat.tributary.source.base.SourceFactory;
import org.zicat.tributary.source.base.netty.pipeline.PipelineInitialization;
import org.zicat.tributary.source.base.netty.pipeline.PipelineInitializationFactory;

import java.util.List;

/** NettySourceFactory. */
public class NettySourceFactory implements SourceFactory {

    public static final ConfigOption<Integer> OPTION_NETTY_PORT =
            ConfigOptions.key("netty.port")
                    .integerType()
                    .description("netty port")
                    .noDefaultValue();

    public static final ConfigOption<Integer> OPTION_NETTY_THREADS_EVENT_LOOP =
            ConfigOptions.key("netty.threads.event-loop")
                    .integerType()
                    .description("netty event loop threads count")
                    .defaultValue(10);

    public static final ConfigOption<List<String>> OPTION_NETTY_HOSTS =
            ConfigOptions.key("netty.host-patterns")
                    .listType(COMMA_SPLIT_HANDLER)
                    .description("netty host to register, split by ,")
                    .defaultValue(null);

    public static final ConfigOption<String> OPTION_NETTY_DECODER =
            ConfigOptions.key("netty.decoder")
                    .stringType()
                    .description(
                            "set netty streaming decoder, values[length,line,kafka,http,logstash-http,logstash-beats]")
                    .noDefaultValue();

    @Override
    public String identity() {
        return "netty";
    }

    @Override
    public Source createSource(String sourceId, Channel channel, ReadableConfig config)
            throws Exception {

        final List<String> hosts = config.get(OPTION_NETTY_HOSTS);
        final int port = config.get(OPTION_NETTY_PORT);
        final int eventLoopThreads = config.get(OPTION_NETTY_THREADS_EVENT_LOOP);
        final String decoder = config.get(OPTION_NETTY_DECODER);
        final PipelineInitializationFactory initializationFactory =
                SpiFactory.findFactory(decoder, PipelineInitializationFactory.class);

        return new NettySource(sourceId, config, channel, hosts, port, eventLoopThreads) {
            @Override
            protected PipelineInitialization createPipelineInitialization() throws Exception {
                return initializationFactory.createPipelineInitialization(this);
            }
        };
    }
}
