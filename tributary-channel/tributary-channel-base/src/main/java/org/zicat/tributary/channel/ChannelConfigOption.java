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

package org.zicat.tributary.channel;

import org.zicat.tributary.common.config.ConfigOption;
import org.zicat.tributary.common.config.ConfigOptions;
import org.zicat.tributary.common.config.MemorySize;

import java.time.Duration;
import java.util.List;

import static org.zicat.tributary.common.config.ConfigOptions.COMMA_SPLIT_HANDLER;

/** ChannelConfigOption. */
public class ChannelConfigOption {

    public static final ConfigOption<MemorySize> OPTION_BLOCK_SIZE =
            ConfigOptions.key("block.size")
                    .memoryType()
                    .description("block size")
                    .defaultValue(new MemorySize(32 * 1024));

    public static final ConfigOption<MemorySize> OPTION_SEGMENT_SIZE =
            ConfigOptions.key("segment.size")
                    .memoryType()
                    .description("segment size")
                    .defaultValue(new MemorySize(4L * 1024L * 1024L * 1024L));

    public static final ConfigOption<CompressionType> OPTION_COMPRESSION =
            ConfigOptions.key("compression")
                    .enumType(CompressionType.class)
                    .description("compression type [none,snappy,zstd]")
                    .defaultValue(CompressionType.NONE);

    public static final ConfigOption<Integer> OPTION_PARTITION_COUNT =
            ConfigOptions.key("partitions")
                    .integerType()
                    .description("partition count")
                    .defaultValue(1);

    public static final ConfigOption<List<String>> OPTION_GROUPS =
            ConfigOptions.key("groups")
                    .listType(COMMA_SPLIT_HANDLER)
                    .description("set groups, split by ','")
                    .noDefaultValue();

    public static final ConfigOption<Duration> OPTION_FLUSH_PERIOD =
            ConfigOptions.key("flush.period")
                    .durationType()
                    .description("async flush page cache to disk period, default 500ms")
                    .defaultValue(Duration.ofMillis(500));

    public static final ConfigOption<Duration> OPTION_CLEANUP_EXPIRED_SEGMENT_PERIOD =
            ConfigOptions.key("segment.expired.cleanup.period")
                    .durationType()
                    .description("cleanup expired segment period, default 30s")
                    .defaultValue(Duration.ofSeconds(30));

    public static final ConfigOption<Integer> OPTION_BLOCK_CACHE_PER_PARTITION_SIZE =
            ConfigOptions.key("block.cache.per.partition.size")
                    .integerType()
                    .description("block cache size per partition, default 1024")
                    .defaultValue(1024);
}
