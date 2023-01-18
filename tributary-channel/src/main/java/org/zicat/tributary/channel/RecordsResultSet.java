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

import java.util.Iterator;
import java.util.concurrent.TimeUnit;

/**
 * RecordsResultSet.
 *
 * <p>RecordsResultSet is a interface to read block data and get the next block data by invoke
 * function {@link RecordsResultSet#nexRecordsOffset()}
 *
 * <p>When invoke {@link Channel#poll(int, org.zicat.tributary.channel.RecordsOffset, long,
 * TimeUnit)} no data, Method {@link RecordsResultSet#isEmpty()} return true.
 *
 * <p>Method {@link RecordsResultSet#readBytes()} can got the total size of this block,
 */
public interface RecordsResultSet extends Iterator<byte[]> {

    /**
     * next records offset.
     *
     * @return RecordsOffset
     */
    RecordsOffset nexRecordsOffset();

    /**
     * read bytes size of the block. if block compression, return compression size
     *
     * @return long
     */
    long readBytes();

    /**
     * if empty.
     *
     * @return boolean
     */
    default boolean isEmpty() {
        return readBytes() == 0;
    }
}
