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

package org.zicat.tributary.channel.test;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zicat.tributary.channel.Channel;
import org.zicat.tributary.channel.Offset;
import org.zicat.tributary.channel.RecordsResultSet;
import org.zicat.tributary.common.exception.TributaryRuntimeException;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/** SinkThread. */
public class SinkThread extends Thread {

    private static final Logger LOG = LoggerFactory.getLogger(SinkThread.class);

    private final Channel channel;
    private final int partitionId;
    private final AtomicLong readSize;
    private final long totalSize;
    private final Offset startOffset;
    private final String groupId;

    public SinkThread(
            Channel channel, int partitionId, String groupId, AtomicLong readSize, long totalSize) {
        this.channel = channel;
        this.partitionId = partitionId;
        this.readSize = readSize;
        this.totalSize = totalSize;
        this.groupId = groupId;
        this.startOffset = channel.committedOffset(groupId, partitionId);
    }

    @Override
    public void run() {

        try {
            Offset offset = startOffset;
            RecordsResultSet result;
            int peakEmptyCount = 0;
            while (readSize.get() < totalSize) {
                result = channel.poll(partitionId, offset, 10000, TimeUnit.MILLISECONDS);
                if (result.isEmpty()) {
                    peakEmptyCount++;
                    if (peakEmptyCount >= 100) {
                        throw new RuntimeException(
                                "read empty for 100 times, maybe channel is closed or no data, partitionId: "
                                        + partitionId
                                        + ", groupId: "
                                        + groupId);
                    }
                } else {
                    peakEmptyCount = 0;
                }
                while (result.hasNext()) {
                    result.next();
                    readSize.incrementAndGet();
                }
                offset = result.nexOffset();
            }
            LOG.info(
                    "read finished, partitionId:{}, groupId:{}, readSize:{}, expectSize:{}",
                    partitionId,
                    groupId,
                    readSize.get(),
                    totalSize);
            channel.commit(partitionId, groupId, offset);
        } catch (Exception e) {
            throw new TributaryRuntimeException(e);
        }
    }
}
