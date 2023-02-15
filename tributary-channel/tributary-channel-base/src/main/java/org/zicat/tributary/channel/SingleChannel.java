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

import org.zicat.tributary.channel.group.SingleGroupManager;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

/** SingleChannel for {@link Channel} without partition. @ThreadSafe */
public interface SingleChannel extends Channel, SingleGroupManager {

    @Override
    default int partition() {
        return 1;
    }

    @Override
    default long lag(int partition, RecordsOffset recordsOffset) {
        return lag(recordsOffset);
    }

    /**
     * estimate the lag between records offset and write position without partition.
     *
     * @param recordsOffset recordsOffset
     * @return long lag return 0 if records offset over
     */
    long lag(RecordsOffset recordsOffset);

    @Override
    default void append(int partition, byte[] record, int offset, int length) throws IOException {
        append(record, offset, length);
    }

    /**
     * append record to channel without partition.
     *
     * <p>append operator only make sure put record to memory block or page cache.
     *
     * <p>invoke {@link Channel#flush()} will flush logs from memory block and page cache to disk.
     *
     * @param record record
     * @param offset offset the record offset
     * @param length length the record length to append
     * @throws IOException IOException
     */
    void append(byte[] record, int offset, int length) throws IOException;

    @Override
    default RecordsResultSet poll(
            int partition, RecordsOffset recordsOffset, long time, TimeUnit unit)
            throws IOException, InterruptedException {
        return poll(recordsOffset, time, unit);
    }

    /**
     * poll records without partition.
     *
     * @param recordsOffset recordsOffset
     * @param time time
     * @param unit unit
     * @return RecordsResultSet
     * @throws IOException IOException
     * @throws InterruptedException InterruptedException
     */
    RecordsResultSet poll(RecordsOffset recordsOffset, long time, TimeUnit unit)
            throws IOException, InterruptedException;

    /**
     * get records offset without partition. if group id is new, return the latest offset in channel
     *
     * @param groupId groupId
     * @return RecordsOffset
     */
    RecordsOffset getRecordsOffset(String groupId);

    @Override
    default RecordsOffset getRecordsOffset(String groupId, int partition) {
        return getRecordsOffset(groupId);
    }
}
