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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zicat.tributary.channel.Channel;
import org.zicat.tributary.common.Clock;
import org.zicat.tributary.common.SystemClock;
import org.zicat.tributary.common.config.ConfigOption;
import org.zicat.tributary.common.config.ConfigOptions;
import org.zicat.tributary.common.config.ReadableConfig;
import org.zicat.tributary.sink.SinkGroupConfig;

import java.time.Duration;

/** DefaultPartitionHandlerFactory. */
public class DefaultPartitionHandlerFactory implements PartitionHandlerFactory {

    private static final Logger LOG = LoggerFactory.getLogger(DefaultPartitionHandlerFactory.class);
    public static final String IDENTITY = "default";

    public static final ConfigOption<Integer> OPTION_THREADS =
            ConfigOptions.key("partition.concurrent")
                    .integerType()
                    .description("consume threads per partition")
                    .defaultValue(1);
    public static final ConfigOption<Duration> OPTION_CHECKPOINT_INTERVAL =
            ConfigOptions.key("partition.checkpoint.interval")
                    .durationType()
                    .description("snapshot interval, default 30 seconds")
                    .defaultValue(Duration.ofSeconds(30));
    public static final ConfigOption<Boolean> OPTION_PARTITION_GRACEFUL_CLOSE_QUICK_EXIT =
            ConfigOptions.key("partition.graceful-close.quick-exit")
                    .booleanType()
                    .defaultValue(true);
    public static final ConfigOption<Clock> OPTION_PARTITION_CLOCK =
            ConfigOptions.key("partition._clock")
                    .<Clock>objectType()
                    .defaultValue(new SystemClock());

    @Override
    public String identity() {
        return IDENTITY;
    }

    @Override
    public PartitionHandler create(
            String groupId, Channel channel, int partitionId, SinkGroupConfig config) {
        final int concurrent = config.get(OPTION_THREADS);
        if (concurrent > 1) {
            return new MultiThreadPartitionHandler(
                    groupId, channel, partitionId, concurrent, config);
        } else {
            return new DirectPartitionHandler(groupId, channel, partitionId, config);
        }
    }

    /**
     * snapshot interval mills.
     *
     * @param config config.
     * @return value
     */
    public static long snapshotIntervalMills(ReadableConfig config) {
        final long snapshot = config.get(OPTION_CHECKPOINT_INTERVAL).toMillis();
        final long min = OPTION_CHECKPOINT_INTERVAL.defaultValue().toMillis();
        if (snapshot < min) {
            LOG.warn("snapshot interval {}ms may affect performance, reset to {}ms", snapshot, min);
            return min;
        }
        return snapshot;
    }
}
