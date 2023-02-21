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

package org.zicat.tributary.source.netty;

/** PartitionSelect. */
public interface PartitionSelect {

    /**
     * select partition by bs.
     *
     * @param bs bs
     * @return partition
     */
    int partition(byte[] bs, int partitionCount);

    /** RoundRobin. */
    class RoundRobin implements PartitionSelect {

        private int nextPartition = 0;

        @Override
        public int partition(byte[] bs, int partitionCount) {
            nextPartition++;
            return (nextPartition & 0x7fffffff) % partitionCount;
        }
    }
}
