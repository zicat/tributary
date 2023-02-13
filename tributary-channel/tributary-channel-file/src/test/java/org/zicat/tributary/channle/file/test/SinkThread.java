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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zicat.tributary.channel.Channel;
import org.zicat.tributary.channel.GroupOffset;
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
    private final GroupOffset startOffset;

    public SinkThread(
            Channel channel,
            int partitionId,
            String groupName,
            AtomicLong readSize,
            long totalSize) {
        this.channel = channel;
        this.partitionId = partitionId;
        this.readSize = readSize;
        this.totalSize = totalSize;
        this.startOffset = channel.committedGroupOffset(groupName, partitionId);
    }

    @Override
    public void run() {

        try {
            GroupOffset groupOffset = startOffset;
            RecordsResultSet result = null;
            long readLength = 0;
            long start = System.currentTimeMillis();
            DecimalFormat df = new DecimalFormat("######0.00");
            Long preFileId = null;
            while (readSize.get() < totalSize || (result != null && result.hasNext())) {
                result = channel.poll(partitionId, groupOffset, 10, TimeUnit.MILLISECONDS);
                while (result.hasNext()) {
                    readLength += result.next().length;
                    readSize.incrementAndGet();
                    if (preFileId == null) {
                        preFileId = result.nexGroupOffset().segmentId();
                    } else if (preFileId != result.nexGroupOffset().segmentId()) {
                        channel.commit(partitionId, groupOffset);
                        preFileId = result.nexGroupOffset().segmentId();
                    }
                }
                long spend = System.currentTimeMillis() - start;
                if (readLength >= 1024 * 1024 * 1024 && spend > 0) {
                    LOG.info(
                            "read spend:"
                                    + df.format(readLength / 1024.0 / 1024.0 / (spend / 1000.0))
                                    + "(mb/s), file id:"
                                    + result.nexGroupOffset().segmentId()
                                    + ", lag:"
                                    + channel.lag(partitionId, result.nexGroupOffset()));
                    readLength = 0;
                    start = System.currentTimeMillis();
                }
                groupOffset = result.nexGroupOffset();
            }
            channel.commit(partitionId, groupOffset);
        } catch (Exception e) {
            throw new TributaryRuntimeException(e);
        }
    }
}
