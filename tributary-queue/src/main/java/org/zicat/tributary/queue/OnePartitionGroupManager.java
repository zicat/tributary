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

package org.zicat.tributary.queue;

import java.io.IOException;

/** OnePartitionGroupManager. */
public interface OnePartitionGroupManager extends GroupManager {

    /**
     * get records offset without partition.
     *
     * @param groupId groupId
     * @return RecordsOffset
     */
    RecordsOffset getRecordsOffset(String groupId);

    @Override
    default RecordsOffset getRecordsOffset(String groupId, int partition) {
        return getRecordsOffset(groupId);
    }

    /**
     * commit records offset without partition.
     *
     * @param groupId groupId
     * @param recordsOffset recordsOffset
     * @throws IOException IOException
     */
    void commit(String groupId, RecordsOffset recordsOffset) throws IOException;

    @Override
    default void commit(String groupId, int partition, RecordsOffset recordsOffset)
            throws IOException {
        commit(groupId, recordsOffset);
    }

    /**
     * get min records offset without partition.
     *
     * @return RecordsOffset
     */
    RecordsOffset getMinRecordsOffset();

    @Override
    default RecordsOffset getMinRecordsOffset(int partition) {
        return getMinRecordsOffset();
    }
}
