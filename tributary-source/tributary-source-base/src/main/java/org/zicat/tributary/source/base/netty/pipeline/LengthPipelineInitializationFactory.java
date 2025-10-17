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

import org.zicat.tributary.common.config.ConfigOption;
import org.zicat.tributary.common.config.ConfigOptions;
import org.zicat.tributary.source.base.netty.NettySource;

/** LengthPipelineInitializationFactory. */
public class LengthPipelineInitializationFactory implements PipelineInitializationFactory {

    public static final String IDENTITY = "length";

    public static final ConfigOption<Integer> OPTION_LENGTH_WORKER_THREADS =
            ConfigOptions.key("netty.decoder.length.worker-threads")
                    .integerType()
                    .description("The number of worker threads for the line handler, default 10")
                    .defaultValue(10);

    @Override
    public String identity() {
        return IDENTITY;
    }

    @Override
    public PipelineInitialization createPipelineInitialization(NettySource source) {
        return new LengthPipelineInitialization(source);
    }
}
