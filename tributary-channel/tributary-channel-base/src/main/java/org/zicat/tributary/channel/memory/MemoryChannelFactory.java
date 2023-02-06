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

package org.zicat.tributary.channel.memory;

import org.zicat.tributary.channel.Channel;
import org.zicat.tributary.channel.ChannelFactory;

import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;

/** MemoryChannelFactory. */
public class MemoryChannelFactory implements ChannelFactory {

    public static final String TYPE = "memory";

    public static final String KEY_PARTITIONS = "partitions";
    public static final String DEFAULT_PARTITIONS = "1";

    public static final String KEY_GROUPS = "groupIds";

    @Override
    public String type() {
        return TYPE;
    }

    @Override
    public Channel createChannel(String topic, Map<String, String> params) {
        final int partitionCounts =
                Integer.parseInt(params.getOrDefault(KEY_PARTITIONS, DEFAULT_PARTITIONS));
        final String groups = params.get(KEY_GROUPS);
        return new MemoryChannel(
                topic,
                partitionCounts,
                Arrays.stream(groups.split(",")).collect(Collectors.toSet()));
    }
}
