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

import org.zicat.tributary.common.ConfigOption;
import org.zicat.tributary.common.ConfigOptions;

/** ChannelConfigOption. */
public class ChannelConfigOption {

    public static final ConfigOption<Integer> OPTION_BLOCK_SIZE =
            ConfigOptions.key("blockSize")
                    .integerType()
                    .description("block size")
                    .defaultValue(32 * 1024);

    public static final ConfigOption<Long> OPTION_SEGMENT_SIZE =
            ConfigOptions.key("segmentSize")
                    .longType()
                    .description("segment size")
                    .defaultValue(4L * 1024L * 1024L * 1024L);

    public static final ConfigOption<String> OPTION_COMPRESSION =
            ConfigOptions.key("compression")
                    .stringType()
                    .description("compression type [none,snappy,zstd]")
                    .defaultValue("none");

    public static final ConfigOption<Integer> OPTION_PARTITIONS =
            ConfigOptions.key("partitions")
                    .integerType()
                    .description("partition count")
                    .defaultValue(1);

    public static final ConfigOption<String> OPTION_GROUPS =
            ConfigOptions.key("groups")
                    .stringType()
                    .description("set groups, split by ','")
                    .noDefaultValue();

    public static final ConfigOption<Integer> OPTION_FLUSH_PERIOD_MILLS =
            ConfigOptions.key("flushPeriodMills")
                    .integerType()
                    .description("async flush page cache to disk period millis, default 500")
                    .defaultValue(500);
}
