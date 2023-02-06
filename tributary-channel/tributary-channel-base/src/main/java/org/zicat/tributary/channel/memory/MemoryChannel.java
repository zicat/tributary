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

import org.zicat.tributary.channel.AbstractChannel;

import java.util.Set;

/** MemoryChannel. */
public class MemoryChannel extends AbstractChannel<OnePartitionMemoryChannel> {

    public MemoryChannel(String topic, Set<String> groups) {
        this(topic, 1, groups);
    }

    public MemoryChannel(String topic, int partitionCount, Set<String> groups) {
        super(createChannels(topic, partitionCount, groups));
    }

    /**
     * create channels.
     *
     * @param topic topic
     * @param partitionCount partitionCount
     * @param groups groups
     * @return list
     */
    private static OnePartitionMemoryChannel[] createChannels(
            String topic, int partitionCount, Set<String> groups) {
        final OnePartitionMemoryChannel[] channels = new OnePartitionMemoryChannel[partitionCount];
        for (int i = 0; i < partitionCount; i++) {
            channels[i] = new OnePartitionMemoryChannel(topic, groups);
        }
        return channels;
    }

    @Override
    public void closeCallback() {}
}
