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

package org.zicat.tributary.channle.file.test;

import org.zicat.tributary.channel.Channel;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/** Sink. */
public class SinkGroup extends Thread {

    private final int partitionCount;
    private final Channel channel;
    private final String groupName;
    private final long totalSize;
    private final AtomicLong consumerCount = new AtomicLong();

    public SinkGroup(int partitionCount, Channel channel, String groupName, long totalSize) {
        this.partitionCount = partitionCount;
        this.channel = channel;
        this.groupName = groupName;
        this.totalSize = totalSize;
    }

    @Override
    public void run() {

        List<Thread> sinkThreads = new ArrayList<>(partitionCount);
        for (int j = 0; j < partitionCount; j++) {
            // register group id to channel and start consumer each partition
            sinkThreads.add(new SinkThread(channel, j, groupName, consumerCount, totalSize));
        }
        sinkThreads.forEach(Thread::start);
        sinkThreads.forEach(FileChannelTest::join);
    }

    public final long getConsumerCount() {
        return consumerCount.get();
    }
}
