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
import org.zicat.tributary.source.base.netty.NettySource;
import org.zicat.tributary.source.base.netty.handler.IdleCloseHandler;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

/** AbstractPipelineInitialization. */
public abstract class AbstractPipelineInitialization implements PipelineInitialization {

    protected final AtomicInteger count = new AtomicInteger();
    protected final NettySource source;

    public AbstractPipelineInitialization(NettySource source) {
        this.source = source;
    }

    /**
     * select partition id.
     *
     * @return partition id
     */
    protected int selectPartition() {
        return (count.getAndIncrement() & 0x7fffffff) % source.partition();
    }

    /**
     * idle closed channel pipeline.
     * @param pipeline pipeline
     * @return ChannelPipeline
     */
    protected ChannelPipeline idleClosedChannelPipeline(ChannelPipeline pipeline) {
        return pipeline.addLast(source.idleStateHandler()).addLast(new IdleCloseHandler());
    }

    @Override
    public void close() throws IOException {}
}
