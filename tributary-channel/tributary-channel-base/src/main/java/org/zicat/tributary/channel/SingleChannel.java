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
import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;

/** SingleChannel for {@link Channel} without partition. @ThreadSafe */
public interface SingleChannel extends Channel, SingleGroupManager {

    @Override
    default int partition() {
        return 0;
    }

    @Override
    default long lag(int partition, Offset offset) {
        return lag(offset);
    }

    /**
     * estimate the lag between group offset and write position without partition.
     *
     * @param offset offset
     * @return long lag return 0 if group offset over
     */
    long lag(Offset offset);

    @Override
    default void append(int partition, byte[] record, int offset, int length)
            throws IOException, InterruptedException {
        append(record, offset, length);
    }

    @Override
    default void append(int partition, ByteBuffer byteBuffer)
            throws IOException, InterruptedException {
        append(byteBuffer);
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
     * @throws InterruptedException InterruptedException
     */
    default void append(byte[] record, int offset, int length)
            throws IOException, InterruptedException {
        append(ByteBuffer.wrap(record, offset, length));
    }

    /**
     * append record to channel without partition.
     *
     * <p>append operator only make sure put record to memory block.
     *
     * <p>invoke {@link Channel#flush()} will flush logs from memory block and page cache to disk.
     *
     * @param byteBuffer byteBuffer
     * @throws IOException IOException
     */
    void append(ByteBuffer byteBuffer) throws IOException, InterruptedException;

    @Override
    default RecordsResultSet poll(int partition, Offset offset, long time, TimeUnit unit)
            throws IOException, InterruptedException {
        return poll(offset, time, unit);
    }

    /**
     * poll records without partition.
     *
     * @param offset offset
     * @param time time
     * @param unit unit
     * @return RecordsResultSet
     * @throws IOException IOException
     * @throws InterruptedException InterruptedException
     */
    RecordsResultSet poll(Offset offset, long time, TimeUnit unit)
            throws IOException, InterruptedException;

    @Override
    default Offset committedOffset(String groupId, int partition) {
        return committedOffset(groupId);
    }

    /**
     * get flush idle time in ms.
     *
     * @return the flush idle time
     */
    long flushIdleMillis();
}
