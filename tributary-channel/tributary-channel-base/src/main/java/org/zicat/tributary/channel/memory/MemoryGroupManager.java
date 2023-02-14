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

package org.zicat.tributary.channel.memory;

import org.zicat.tributary.channel.AbstractChannel;
import org.zicat.tributary.channel.RecordsOffset;
import org.zicat.tributary.channel.SingleGroupManager;
import org.zicat.tributary.common.Threads;

import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * MemoryGroupManager.
 *
 * <p>Store commit offset in memory cache, Override flush(String groupId, RecordsOffset
 * recordsOffset) to storage.
 */
public abstract class MemoryGroupManager implements SingleGroupManager {

    public static final RecordsOffset DEFAULT_RECORDS_OFFSET = new RecordsOffset(-1, -1);
    private final Map<String, RecordsOffset> cache = new ConcurrentHashMap<>();
    private final Set<String> groups = new HashSet<>();
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final long periodSecond;
    protected ScheduledExecutorService schedule;

    public MemoryGroupManager(Map<String, RecordsOffset> groupOffsets, long periodSecond) {
        this.cache.putAll(groupOffsets);
        this.groups.addAll(groupOffsets.keySet());
        this.periodSecond = periodSecond;
        if (periodSecond > 0) {
            this.schedule =
                    Executors.newSingleThreadScheduledExecutor(
                            Threads.createThreadFactoryByName("group_persist", true));
            schedule();
        }
    }

    /** schedule. */
    protected void schedule() {
        schedule.scheduleWithFixedDelay(
                this::persist, periodSecond, periodSecond, TimeUnit.SECONDS);
    }

    /** persist. */
    public abstract void persist();

    /**
     * foreach group.
     *
     * @param action callback function
     */
    protected void foreachGroup(Consumer<String, RecordsOffset> action) throws IOException {
        for (Map.Entry<String, RecordsOffset> entry : cache.entrySet()) {
            action.accept(entry.getKey(), entry.getValue());
        }
    }

    @Override
    public synchronized void commit(String groupId, RecordsOffset recordsOffset) {

        isOpen();
        final RecordsOffset cachedRecordsOffset = cache.get(groupId);
        if (cachedRecordsOffset == null) {
            throw new IllegalStateException("group id " + groupId + " not found in cache");
        }
        if (cachedRecordsOffset.compareTo(recordsOffset) >= 0) {
            return;
        }
        cache.put(groupId, recordsOffset);
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
        RecordsOffset min = null;
        for (Map.Entry<String, RecordsOffset> entry : cache.entrySet()) {
            final RecordsOffset recordsOffset = entry.getValue();
            min = min == null ? recordsOffset : RecordsOffset.min(min, recordsOffset);
        }
        return min;
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
            try {
                if (schedule != null) {
                    schedule.shutdownNow();
                }
                persist();
            } finally {
                cache.clear();
            }
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
    public static AbstractChannel.SingleGroupManagerFactory createUnPersistGroupManagerFactory(
            Map<String, RecordsOffset> groupOffsets) {
        return () ->
                new MemoryGroupManager(groupOffsets, -1) {
                    @Override
                    public void persist() {}
                };
    }
}
