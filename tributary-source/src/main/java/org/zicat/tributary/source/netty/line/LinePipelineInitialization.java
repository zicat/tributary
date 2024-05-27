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

package org.zicat.tributary.source.netty.line;

import io.netty.channel.ChannelPipeline;
import org.zicat.tributary.source.netty.ChannelPipelineInitialization;
import org.zicat.tributary.source.netty.DefaultNettySource;
import org.zicat.tributary.source.netty.IdleCloseHandler;

/** LinePipelineInitialization. */
public class LinePipelineInitialization extends ChannelPipelineInitialization {

    protected final DefaultNettySource source;

    public LinePipelineInitialization(DefaultNettySource source) {
        super(source);
        this.source = source;
    }

    @Override
    public void init(ChannelPipeline pipeline) {
        pipeline.addLast(source.idleStateHandler());
        pipeline.addLast(new IdleCloseHandler());
        pipeline.addLast(new LineDecoder());
        pipeline.addLast(createMuteChannelHandler());
    }
}
