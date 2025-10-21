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

package org.zicat.tributary.channel.file;

import static org.zicat.tributary.common.config.ConfigOptions.COMMA_SPLIT_HANDLER;

import org.zicat.tributary.channel.ChannelConfigOption;
import org.zicat.tributary.common.config.ConfigOption;
import org.zicat.tributary.common.config.ConfigOptions;
import org.zicat.tributary.common.config.PercentSize;

import java.util.List;

/** FileChannelConfigOption. */
public class FileChannelConfigOption extends ChannelConfigOption {

    public static final ConfigOption<List<String>> OPTION_PARTITION_PATHS =
            ConfigOptions.key("partitions")
                    .listType(COMMA_SPLIT_HANDLER)
                    .description("partition paths, must allow read and write, split by ','")
                    .noDefaultValue();

    public static final ConfigOption<PercentSize> OPTION_CAPACITY_PROTECTED_PERCENT =
            ConfigOptions.key("capacity.protected.percent")
                    .percentType()
                    .description("the capacity protected percent")
                    .defaultValue(PercentSize.parse("90%"));
}
