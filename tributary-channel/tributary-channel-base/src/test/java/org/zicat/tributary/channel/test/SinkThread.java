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
import org.zicat.tributary.common.TributaryRuntimeException;

import java.text.DecimalFormat;
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
    private final int sleep;

    public SinkThread(
            Channel channel,
            int partitionId,
            String groupId,
            AtomicLong readSize,
            long totalSize,
            int sleep) {
        this.channel = channel;
        this.partitionId = partitionId;
        this.readSize = readSize;
        this.totalSize = totalSize;
        this.groupId = groupId;
        this.startOffset = channel.committedOffset(groupId, partitionId);
        this.sleep = sleep;
    }

    @SuppressWarnings("BusyWait")
    @Override
    public void run() {

        try {
            Offset offset = startOffset;
            RecordsResultSet result = null;
            long readLength = 0;
            long start = System.currentTimeMillis();
            DecimalFormat df = new DecimalFormat("######0.00");
            Long preFileId = null;
            boolean firstThread = false;
            boolean middleThread = false;
            while (readSize.get() < totalSize || (result != null && result.hasNext())) {
                result = channel.poll(partitionId, offset, 10, TimeUnit.MILLISECONDS);
                while (result.hasNext()) {
                    readLength += result.next().length;
                    readSize.incrementAndGet();
                    if (preFileId == null) {
                        preFileId = result.nexOffset().segmentId();
                    } else if (preFileId != result.nexOffset().segmentId()) {
                        channel.commit(partitionId, groupId, offset);
                        preFileId = result.nexOffset().segmentId();
                    }
                }
                long spend = System.currentTimeMillis() - start;
                if (readLength >= 1024 * 1024 * 1024 && spend > 0) {
                    LOG.info(
                            "read spend:"
                                    + df.format(readLength / 1024.0 / 1024.0 / (spend / 1000.0))
                                    + "(mb/s), file id:"
                                    + result.nexOffset().segmentId()
                                    + ", lag:"
                                    + channel.lag(partitionId, result.nexOffset()));
                    readLength = 0;
                    start = System.currentTimeMillis();
                }
                offset = result.nexOffset();
                if (sleep > 0) {
                    if (!firstThread) {
                        firstThread = true;
                        Thread.sleep(sleep);
                    }
                    if (!middleThread && readSize.get() > totalSize / 2) {
                        middleThread = true;
                        Thread.sleep(sleep);
                    }
                }
            }
            channel.commit(partitionId, groupId, offset);
        } catch (Exception e) {
            throw new TributaryRuntimeException(e);
        }
    }
}
