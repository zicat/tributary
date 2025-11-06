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

import java.io.Closeable;
import java.util.Set;

/** GroupManager. @ThreadSafe */
public interface GroupManager extends Closeable {

    /**
     * get all group consume this topic.
     *
     * @return groups
     */
    Set<String> groups();

    /**
     * get committed group offset by group id & partition. return (-1, -1) if first consume
     *
     * @param groupId groupId
     * @param partition partition
     * @return Offset offset
     */
    Offset committedOffset(String groupId, int partition);

    /**
     * commit group offset.
     *
     * @param partition partition
     * @param groupId groupId
     * @param offset offset
     */
    void commit(int partition, String groupId, Offset offset);

    /**
     * commit group offset without group id.
     *
     * @param partition partition
     * @param offset offset
     */
    void commit(int partition, Offset offset);
}
