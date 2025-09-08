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

import io.netty.channel.Channel;
import io.netty.channel.ChannelPipeline;

import org.zicat.tributary.source.base.netty.NettySource;
import org.zicat.tributary.source.base.netty.handler.BytesChannelHandler.LengthResponseBytesChannelHandler;
import org.zicat.tributary.source.base.netty.handler.LengthDecoder;

/** LengthPipelineInitialization. */
public class LengthPipelineInitialization extends AbstractPipelineInitialization {

    public LengthPipelineInitialization(NettySource source) {
        super(source);
    }

    @Override
    public void init(Channel channel) {
        final ChannelPipeline pipeline = channel.pipeline();
        idleClosedChannelPipeline(pipeline)
                .addLast(new LengthDecoder())
                .addLast(new LengthResponseBytesChannelHandler(source, selectPartition()));
    }
}
