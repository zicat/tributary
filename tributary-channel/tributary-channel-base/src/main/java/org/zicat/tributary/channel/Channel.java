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
import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;

/**
 * Channel.
 *
 * <p>Channel support to append record to channel, poll or take records, flush data and commit
 * consumer {@link Offset}
 *
 * <p>All methods in Channel are @ThreadSafe.
 */
public interface Channel extends Closeable, ChannelMetric, GroupManager {

    /**
     * append record to channel.
     *
     * <p>append operator only make sure put record to memory block or page cache.
     *
     * <p>invoke {@link Channel#flush()} will flush logs from memory block and page cache to disk.
     *
     * @param partition partition
     * @param byteBuffer byteBuffer
     * @throws IOException IOException
     */
    void append(int partition, ByteBuffer byteBuffer) throws IOException, InterruptedException;

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
    default void append(int partition, byte[] record, int offset, int length)
            throws IOException, InterruptedException {
        if (record != null) {
            append(partition, ByteBuffer.wrap(record, offset, length));
        }
    }

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
    default void append(int partition, byte[] record) throws IOException, InterruptedException {
        if (record != null) {
            append(partition, ByteBuffer.wrap(record));
        }
    }

    /**
     * poll records.
     *
     * @param partition partition
     * @param groupOffset groupOffset
     * @param time time
     * @param unit unit
     * @return RecordsResultSet
     * @throws IOException IOException
     * @throws InterruptedException InterruptedException
     */
    RecordsResultSet poll(int partition, Offset groupOffset, long time, TimeUnit unit)
            throws IOException, InterruptedException;

    /**
     * take records. waiting if necessary * until an element becomes available.
     *
     * @param partition the partition to read
     * @param offset offset
     * @return RecordsResultSet
     * @throws IOException IOException
     * @throws InterruptedException InterruptedException
     */
    default RecordsResultSet take(int partition, Offset offset)
            throws IOException, InterruptedException {
        return poll(partition, offset, 0, TimeUnit.MILLISECONDS);
    }

    /** flush block data and page cache data to disk. */
    void flush() throws IOException;

    /**
     * get group offset without partition. if group id is new, return the latest offset in channel
     *
     * @param groupId groupId
     * @return GroupOffset
     */
    Offset committedOffset(String groupId, int partition);

    /**
     * estimate the lag between group offset and write position in one partition.
     *
     * @param partition partition
     * @param offset offset
     * @return long lag return 0 if group offset over latest offset
     */
    long lag(int partition, Offset offset);

    /**
     * get topic.
     *
     * @return topic
     */
    String topic();

    /**
     * get partition count.
     *
     * @return int
     */
    int partition();
}
