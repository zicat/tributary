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

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

/** MemoryOnePartitionGroupManager. */
public abstract class MemoryOnePartitionGroupManager implements OnePartitionGroupManager {

    private final Map<String, RecordsOffset> cache = new ConcurrentHashMap<>();
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final String topic;
    private final Set<String> groups;

    public MemoryOnePartitionGroupManager(String topic, List<String> groups) {
        this.topic = topic;
        this.groups = new HashSet<>(groups);
    }

    /**
     * load topic group offset 2 cache.
     *
     * @param map map
     */
    public void loadTopicGroupOffset2Cache(Map<String, RecordsOffset> map) {
        cache.putAll(map);
    }

    /**
     * flush to storage.
     *
     * @param groupId groupId
     * @param recordsOffset recordsOffset
     * @throws IOException IOException
     */
    public abstract void flush(String groupId, RecordsOffset recordsOffset) throws IOException;

    @Override
    public synchronized void commit(String groupId, RecordsOffset recordsOffset)
            throws IOException {

        isOpen();
        RecordsOffset cachedRecordsOffset = cache.get(groupId);
        if (cachedRecordsOffset != null && cachedRecordsOffset.compareTo(recordsOffset) >= 0) {
            return;
        }
        flush(groupId, recordsOffset);
        cache.put(groupId, recordsOffset);
    }

    @Override
    public String topic() {
        return topic;
    }

    @Override
    public Set<String> groups() {
        return groups;
    }

    @Override
    public RecordsOffset getRecordsOffset(String groupId) {
        isOpen();
        return cache.get(groupId);
    }

    @Override
    public RecordsOffset getMinRecordsOffset() {
        isOpen();
        return cache.values().stream().min(RecordsOffset::compareTo).orElse(null);
    }

    /**
     * create new group records offset.
     *
     * @return RecordsOffset
     */
    public static RecordsOffset createNewGroupRecordsOffset() {
        return new RecordsOffset(-1, 0);
    }

    /** check whether closed. */
    protected void isOpen() {
        if (closed.get()) {
            throw new IllegalStateException("GroupManager is closed");
        }
    }

    @Override
    public void close() throws IOException {
        if (closed.compareAndSet(false, true)) {
            try {
                closeCallback();
            } finally {
                cache.clear();
            }
        }
    }

    /** call back close. */
    public abstract void closeCallback() throws IOException;
}
