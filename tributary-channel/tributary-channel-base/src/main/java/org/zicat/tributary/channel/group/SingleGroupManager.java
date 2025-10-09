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

package org.zicat.tributary.channel.group;

import org.zicat.tributary.channel.Offset;

import java.io.IOException;

/** SingleGroupManager for {@link GroupManager} offset without partition. */
public interface SingleGroupManager extends GroupManager {

    /**
     * get committed group offset without partition. return (-1, -1) if first consume
     *
     * @param groupId groupId
     * @return GroupOffset
     */
    Offset committedOffset(String groupId);

    @Override
    default Offset committedOffset(String groupId, int partition) {
        return committedOffset(groupId);
    }

    /**
     * commit group offset without partition.
     *
     * @param groupId groupId
     * @param offset offset
     * @throws IOException IOException
     */
    void commit(String groupId, Offset offset) throws IOException;

    /**
     * commit all group offset without partition.
     *
     * @param offset offset
     * @throws IOException IOException
     */
    void commit(Offset offset) throws IOException;

    @Override
    default void commit(int partition, String groupId, Offset offset) throws IOException {
        commit(groupId, offset);
    }

    @Override
    default void commit(int partition, Offset offset) throws IOException {
        commit(offset);
    }
}
