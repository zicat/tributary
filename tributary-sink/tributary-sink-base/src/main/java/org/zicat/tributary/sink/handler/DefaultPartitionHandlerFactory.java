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

package org.zicat.tributary.sink.handler;

import org.zicat.tributary.channel.Channel;
import org.zicat.tributary.common.ConfigOption;
import org.zicat.tributary.common.ConfigOptions;
import org.zicat.tributary.sink.SinkGroupConfig;

/** DefaultPartitionHandlerFactory. */
public class DefaultPartitionHandlerFactory implements PartitionHandlerFactory {

    public static final String IDENTITY = "default";
    public static final ConfigOption<Integer> OPTION_THREADS =
            ConfigOptions.key("partition.concurrent")
                    .integerType()
                    .description("consume threads per partition")
                    .defaultValue(1);

    @Override
    public String identity() {
        return IDENTITY;
    }

    @Override
    public AbstractPartitionHandler createHandler(
            String groupId, Channel channel, int partitionId, SinkGroupConfig sinkGroupConfig) {
        final int concurrent = sinkGroupConfig.get(OPTION_THREADS);
        if (concurrent > 1) {
            return new MultiThreadPartitionHandler(
                    groupId, channel, partitionId, concurrent, sinkGroupConfig);
        } else {
            return new DirectPartitionHandler(groupId, channel, partitionId, sinkGroupConfig);
        }
    }
}
