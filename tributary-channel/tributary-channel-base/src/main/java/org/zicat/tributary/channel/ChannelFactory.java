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

import org.zicat.tributary.common.ReadableConfig;
import org.zicat.tributary.common.TributaryRuntimeException;

import java.util.Arrays;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.stream.Collectors;

import static org.zicat.tributary.channel.ChannelConfigOption.OPTION_GROUPS;
import static org.zicat.tributary.channel.memory.MemoryChannelFactory.SPLIT_STR;

/** ChannelFactory. */
public interface ChannelFactory {

    /**
     * get channel type.
     *
     * @return type
     */
    String type();

    /**
     * create channel.
     *
     * @param topic topic
     * @param readableConfig readableConfig
     * @throws Exception exception
     * @return Channel
     */
    Channel createChannel(String topic, ReadableConfig readableConfig) throws Exception;

    /**
     * find channel factory.
     *
     * @param type type
     * @return ChannelFactory
     */
    static ChannelFactory findChannelFactory(String type) {
        final ServiceLoader<ChannelFactory> loader = ServiceLoader.load(ChannelFactory.class);
        for (ChannelFactory channelFactory : loader) {
            if (type.equals(channelFactory.type())) {
                return channelFactory;
            }
        }
        throw new TributaryRuntimeException("channel type not found," + type);
    }

    /**
     * get group set by config.
     *
     * @param config config
     * @return group set
     */
    default Set<String> groupSet(ReadableConfig config) {
        return Arrays.stream(config.get(OPTION_GROUPS).split(SPLIT_STR))
                .map(String::trim)
                .collect(Collectors.toSet());
    }
}
