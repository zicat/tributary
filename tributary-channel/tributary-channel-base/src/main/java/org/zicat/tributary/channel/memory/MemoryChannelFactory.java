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
import org.zicat.tributary.channel.CompressionType;

import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;

/** MemoryChannelFactory. */
public class MemoryChannelFactory implements ChannelFactory {

    public static final String KEY_FILE_BLOCK_SIZE = "blockSize";
    public static final String DEFAULT_FILE_BLOCK_SIZE = String.valueOf(32 * 1024);

    public static final String KEY_FILE_SEGMENT_SIZE = "segmentSize";
    public static final String DEFAULT_FILE_SEGMENT_SIZE =
            String.valueOf(4L * 1024L * 1024L * 1024L);

    public static final String KEY_FILE_FLUSH_FORCE = "flushForce";
    public static final String DEFAULT_FILE_FLUSH_FORCE = "false";

    public static final String KEY_FILE_COMPRESSION = "compression";
    public static final String DEFAULT_FILE_COMPRESSION = "none";

    public static final String TYPE = "memory";

    public static final String KEY_PARTITIONS = "partitions";
    public static final String DEFAULT_PARTITIONS = "1";

    public static final String KEY_GROUPS = "groups";

    @Override
    public String type() {
        return TYPE;
    }

    @Override
    public Channel createChannel(String topic, Map<String, String> params) {

        final int partitionCounts =
                Integer.parseInt(params.getOrDefault(KEY_PARTITIONS, DEFAULT_PARTITIONS));
        final String groups = params.get(KEY_GROUPS);
        final int blockSize =
                Integer.parseInt(params.getOrDefault(KEY_FILE_BLOCK_SIZE, DEFAULT_FILE_BLOCK_SIZE));
        final long segmentSize =
                Long.parseLong(
                        params.getOrDefault(KEY_FILE_SEGMENT_SIZE, DEFAULT_FILE_SEGMENT_SIZE));
        final String compression =
                params.getOrDefault(KEY_FILE_COMPRESSION, DEFAULT_FILE_COMPRESSION);
        final boolean flushForce =
                Boolean.parseBoolean(
                        params.getOrDefault(KEY_FILE_FLUSH_FORCE, DEFAULT_FILE_FLUSH_FORCE));
        return new MemoryChannel(
                topic,
                partitionCounts,
                Arrays.stream(groups.split(",")).collect(Collectors.toSet()),
                blockSize,
                segmentSize,
                CompressionType.getByName(compression),
                flushForce);
    }
}
