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

import org.zicat.tributary.channel.group.GroupManager;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 * Channel.
 *
 * <p>Channel support to append record to channel, poll or take records, flush data and commit
 * consumer {@link RecordsOffset}
 *
 * <p>All methods in Channel are @ThreadSafe.
 */
public interface Channel extends Closeable, ChannelMeta, GroupManager {

    /**
     * append record to channel.
     *
     * <p>append operator only make sure put record to memory block or page cache.
     *
     * <p>invoke {@link Channel#flush()} will flush logs from memory block and page cache to disk.
     *
     * @param partition partition
     * @param record record
     * @param offset offset the record offset
     * @param length length the record length to append
     * @throws IOException IOException
     */
    void append(int partition, byte[] record, int offset, int length) throws IOException;

    /**
     * append record to channel.
     *
     * <p>append operator only make sure put record to memory block or page cache.
     *
     * <p>invoke {@link Channel#flush()} will flush logs from memory block and page cache to disk.
     *
     * @param partition partition
     * @param record record
     * @throws IOException IOException
     */
    default void append(int partition, byte[] record) throws IOException {
        if (record != null) {
            append(partition, record, 0, record.length);
        }
    }

    /**
     * poll records.
     *
     * @param partition partition
     * @param recordsOffset recordsOffset
     * @param time time
     * @param unit unit
     * @return RecordsResultSet
     * @throws IOException IOException
     * @throws InterruptedException InterruptedException
     */
    RecordsResultSet poll(int partition, RecordsOffset recordsOffset, long time, TimeUnit unit)
            throws IOException, InterruptedException;

    /**
     * take records. waiting if necessary * until an element becomes available.
     *
     * @param partition the partition to read
     * @param recordsOffset recordsOffset
     * @return RecordsResultSet
     * @throws IOException IOException
     * @throws InterruptedException InterruptedException
     */
    default RecordsResultSet take(int partition, RecordsOffset recordsOffset)
            throws IOException, InterruptedException {
        return poll(partition, recordsOffset, 0, TimeUnit.MILLISECONDS);
    }

    /** flush block data and page cache data to disk. */
    void flush() throws IOException;

    /**
     * get records offset without partition. if group id is new, return the latest offset in channel
     *
     * @param groupId groupId
     * @return RecordsOffset
     */
    RecordsOffset getRecordsOffset(String groupId, int partition);
}
