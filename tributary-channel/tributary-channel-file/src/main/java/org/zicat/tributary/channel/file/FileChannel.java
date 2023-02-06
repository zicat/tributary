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

import org.zicat.tributary.channel.AbstractChannel;
import org.zicat.tributary.channel.Channel;
import org.zicat.tributary.common.Functions;

import java.util.concurrent.TimeUnit;

/**
 * FileChannel implements {@link Channel} to Storage records by wrapper multi {@link
 * OnePartitionFileChannel}. Each {@link OnePartitionFileChannel} store one partition data
 *
 * <p>All public methods in PartitionFileChannel are @ThreadSafe.
 */
public class FileChannel extends AbstractChannel<OnePartitionFileChannel> {

    private Thread flushSegmentThread;
    private long flushPeriodMill;

    protected FileChannel(
            OnePartitionFileChannel[] fileChannels, long flushPeriod, TimeUnit flushUnit) {
        super(fileChannels);
        if (flushPeriod > 0) {
            flushPeriodMill = flushUnit.toMillis(flushPeriod);
            flushSegmentThread = new Thread(this::periodForceSegment, "segment_flush_thread");
            flushSegmentThread.start();
        }
    }

    /** force segment to dish. */
    protected void periodForceSegment() {
        Functions.loopCloseableFunction(
                t -> {
                    boolean success = true;
                    for (OnePartitionFileChannel fileChannel : channels) {
                        success = success && fileChannel.periodFlush();
                    }
                    return success;
                },
                flushPeriodMill,
                closed);
    }

    @Override
    public void closeCallback() {
        if (flushSegmentThread != null) {
            flushSegmentThread.interrupt();
        }
    }
}
