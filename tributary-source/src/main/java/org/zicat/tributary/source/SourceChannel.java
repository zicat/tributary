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

package org.zicat.tributary.source;

import org.zicat.tributary.channel.Channel;
import org.zicat.tributary.common.records.Records;

import java.io.IOException;

/** SourceChannel. */
public interface SourceChannel {

    /**
     * append records to channel.
     *
     * <p>append operator only make sure put record to memory block or page cache.
     *
     * <p>invoke {@link Channel#flush()} will flush logs from memory block and page cache to disk.
     *
     * @param partition partition
     * @throws IOException IOException
     */
    void append(int partition, Records records) throws IOException;

    /** flush block data and page cache data to disk. */
    void flush() throws IOException;

    /**
     * get topic.
     *
     * @return topic
     */
    String topic();

    /**
     * get partition.
     *
     * @return partition
     */
    int partition();
}
