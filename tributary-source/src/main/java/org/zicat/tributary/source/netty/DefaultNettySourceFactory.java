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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zicat.tributary.channel.Channel;
import org.zicat.tributary.common.ConfigOption;
import org.zicat.tributary.common.ConfigOptions;
import org.zicat.tributary.common.ReadableConfig;
import org.zicat.tributary.common.SpiFactory;
import org.zicat.tributary.source.netty.pipeline.PipelineInitialization;
import org.zicat.tributary.source.netty.pipeline.PipelineInitializationFactory;

/** DefaultNettySourceFactory. */
public class DefaultNettySourceFactory extends AbstractNettySourceFactory {

    private static final Logger LOG = LoggerFactory.getLogger(DefaultNettySourceFactory.class);

    public static final ConfigOption<Integer> OPTION_NETTY_IDLE_SECOND =
            ConfigOptions.key("netty.idle.second")
                    .integerType()
                    .description("max wait to close when channel idle over this param")
                    .defaultValue(120);

    public static final ConfigOption<String> OPTION_NETTY_DECODER =
            ConfigOptions.key("netty.decoder")
                    .stringType()
                    .description(
                            "set netty streaming decoder, values[lengthDecoder,lineDecoder,kafkaDecoder,httpDecoder]")
                    .defaultValue("lengthDecoder");

    @Override
    public String identity() {
        return "netty";
    }

    @Override
    public AbstractNettySource createNettySource(
            String sourceId,
            String host,
            int port,
            int eventThreads,
            Channel channel,
            ReadableConfig config)
            throws Exception {
        final int idleSecond = config.get(OPTION_NETTY_IDLE_SECOND);
        final String decode = config.get(OPTION_NETTY_DECODER);
        final PipelineInitializationFactory initializationFactory =
                SpiFactory.findFactory(decode, PipelineInitializationFactory.class);
        LOG.info("netty decode register success, identity = {}", decode);
        return new DefaultNettySource(
                sourceId, config, host, port, eventThreads, channel, idleSecond) {
            @Override
            protected PipelineInitialization createPipelineInitialization() throws Exception {
                return initializationFactory.createPipelineInitialization(this);
            }
        };
    }
}
