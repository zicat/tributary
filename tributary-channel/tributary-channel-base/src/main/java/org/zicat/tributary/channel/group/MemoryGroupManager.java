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

import org.zicat.tributary.channel.AbstractChannel;
import org.zicat.tributary.channel.Offset;

import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * MemoryGroupManager.
 *
 * <p>Store commit offset in memory cache, Override flush function to storage.
 */
public class MemoryGroupManager implements SingleGroupManager {

    private final Map<String, Offset> cache = new ConcurrentHashMap<>();
    private final Set<String> groups = new HashSet<>();
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private GroupOffset minGroupOffset;

    public MemoryGroupManager(Map<String, Offset> groupOffsets) {
        for (Map.Entry<String, Offset> entry : groupOffsets.entrySet()) {
            this.cache.put(entry.getKey(), entry.getValue());
            this.groups.add(entry.getKey());
        }
        minGroupOffset = minOffset(cache);
    }

    /**
     * foreach group.
     *
     * @param action callback function
     */
    protected void foreachGroup(Consumer<String, Offset> action) throws IOException {
        isOpen();
        for (Map.Entry<String, Offset> entry : cache.entrySet()) {
            action.accept(entry.getKey(), entry.getValue());
        }
    }

    @Override
    public synchronized void commit(String groupId, Offset offset) {

        isOpen();
        final Offset cachedOffset = cache.get(groupId);
        if (cachedOffset == null) {
            throw new IllegalStateException("group id " + groupId + " not found in cache");
        }
        if (cachedOffset.compareTo(offset) >= 0) {
            return;
        }
        cache.put(groupId, offset);
        minGroupOffset = minOffset(cache);
    }

    @Override
    public Set<String> groups() {
        return groups;
    }

    @Override
    public Offset committedOffset(String groupId) {
        isOpen();
        return cache.get(groupId);
    }

    /**
     * get min group offset.
     *
     * @param cache cache
     * @return GroupOffset
     */
    private static GroupOffset minOffset(Map<String, Offset> cache) {
        GroupOffset min = null;
        for (Map.Entry<String, Offset> entry : cache.entrySet()) {
            final Offset offset = entry.getValue();
            if (min == null || min.compareTo(offset) > 0) {
                min = new GroupOffset(entry.getKey(), entry.getValue());
            }
        }
        return min;
    }

    /**
     * get min group offset.
     *
     * @return GroupOffset
     */
    public GroupOffset getMinGroupOffset() {
        return minGroupOffset;
    }

    /** check whether closed. */
    protected void isOpen() {
        if (closed.get()) {
            throw new IllegalStateException("GroupManager is closed");
        }
    }

    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            cache.clear();
        }
    }

    /**
     * Consumer.
     *
     * @param <T>
     * @param <U>
     */
    public interface Consumer<T, U> {

        /**
         * accept t u.
         *
         * @param t t
         * @param u u
         * @throws IOException IOException
         */
        void accept(T t, U u) throws IOException;
    }

    /**
     * create not persist memory group manager.
     *
     * @param groupOffsets groupOffsets
     * @return MemoryOnePartitionGroupManager
     */
    public static AbstractChannel.MemoryGroupManagerFactory createMemoryGroupManagerFactory(
            Map<String, Offset> groupOffsets) {
        return () -> new MemoryGroupManager(groupOffsets);
    }

    /** GroupOffset. */
    public static class GroupOffset extends Offset {

        private final String groupId;

        public GroupOffset(String groupId, long segmentId, long offset) {
            super(segmentId, offset);
            this.groupId = groupId;
        }

        public GroupOffset(String groupId, Offset offset) {
            this(groupId, offset.segmentId(), offset.offset());
        }

        public String groupId() {
            return groupId;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof GroupOffset)) {
                return false;
            }
            if (!super.equals(o)) {
                return false;
            }
            GroupOffset that = (GroupOffset) o;
            return Objects.equals(groupId, that.groupId);
        }

        @Override
        public int hashCode() {
            return Objects.hash(super.hashCode(), groupId);
        }
    }
}
