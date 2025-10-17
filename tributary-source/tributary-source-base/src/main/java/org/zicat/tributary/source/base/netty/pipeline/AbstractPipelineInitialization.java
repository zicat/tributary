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

package org.zicat.tributary.source.base.netty.pipeline;

import io.netty.channel.ChannelPipeline;
import io.netty.handler.timeout.IdleStateHandler;
import org.zicat.tributary.common.config.ConfigOption;
import org.zicat.tributary.common.config.ConfigOptions;
import org.zicat.tributary.source.base.Source;
import org.zicat.tributary.source.base.netty.handler.IdleCloseHandler;

import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/** AbstractPipelineInitialization. */
public abstract class AbstractPipelineInitialization implements PipelineInitialization {

    public static final ConfigOption<Duration> OPTION_NETTY_IDLE =
            ConfigOptions.key("netty.idle")
                    .durationType()
                    .description("max wait to close when channel idle over this param")
                    .defaultValue(Duration.ofSeconds(120));

    protected final AtomicInteger count = new AtomicInteger();
    protected final Source source;
    protected final long idle;

    public AbstractPipelineInitialization(Source source) {
        this.source = source;
        this.idle = source.config().get(OPTION_NETTY_IDLE).toMillis();
    }

    /**
     * idle closed channel pipeline.
     *
     * @param pipeline pipeline
     * @return ChannelPipeline
     */
    protected ChannelPipeline idleClosedChannelPipeline(ChannelPipeline pipeline) {
        return pipeline.addLast(new IdleStateHandler(idle, idle, idle, TimeUnit.MILLISECONDS))
                .addLast(new IdleCloseHandler());
    }

    @Override
    public void close() throws IOException {}
}
