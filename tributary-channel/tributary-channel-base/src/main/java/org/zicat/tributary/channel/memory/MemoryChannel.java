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

import org.zicat.tributary.channel.MemoryOnePartitionGroupManager;
import org.zicat.tributary.channel.OnePartitionChannel;
import org.zicat.tributary.channel.RecordsOffset;
import org.zicat.tributary.channel.RecordsResultSet;
import org.zicat.tributary.common.IOUtils;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import static org.zicat.tributary.channel.MemoryOnePartitionGroupManager.createUnPersistGroupManager;

/** MemoryChannel. */
public class MemoryChannel implements OnePartitionChannel {

    private final LinkedList<Element> elements = new LinkedList<>();
    private final String topic;
    private final AtomicBoolean closed = new AtomicBoolean();
    private final ReentrantLock lock = new ReentrantLock();
    private final Condition notEmpty = lock.newCondition();

    private final AtomicLong nextSegmentId = new AtomicLong();
    private final AtomicLong minCommitSegmentId = new AtomicLong();

    private final AtomicLong writeBytes = new AtomicLong();
    private final AtomicLong readBytes = new AtomicLong();
    private final MemoryOnePartitionGroupManager groupManager;

    public MemoryChannel(String topic, Set<String> groups) {
        this.topic = topic;
        this.groupManager = createUnPersistGroupManager(topic, initGroupRecordsOffset(groups));
    }

    /**
     * init groups record offset.
     *
     * @param groups groups
     * @return map
     */
    private static Map<String, RecordsOffset> initGroupRecordsOffset(Set<String> groups) {
        final Map<String, RecordsOffset> result = new HashMap<>();
        groups.forEach(group -> result.put(group, RecordsOffset.startRecordOffset()));
        return result;
    }

    @Override
    public RecordsOffset getRecordsOffset(String groupId) {
        return groupManager.getRecordsOffset(groupId);
    }

    @Override
    public void commit(String groupId, RecordsOffset recordsOffset) {
        groupManager.commit(groupId, recordsOffset);
        clear();
    }

    @Override
    public RecordsOffset getMinRecordsOffset() {
        return groupManager.getMinRecordsOffset();
    }

    /** clear expired elements. */
    private void clear() {
        final RecordsOffset min = getMinRecordsOffset();
        if (min == null) {
            return;
        }
        final ReentrantLock lock = this.lock;
        lock.lock();
        try {
            minCommitSegmentId.set(min.segmentId());
            final Iterator<Element> it = elements.iterator();
            while (it.hasNext()) {
                final Element element = it.next();
                if (element.recordsOffset().segmentId() >= min.segmentId()) {
                    break;
                }
                it.remove();
            }
        } finally {
            lock.unlock();
        }
    }

    /**
     * read elements.
     *
     * @param recordsOffset recordsOffset
     * @param elements elements
     * @return RecordsResultSet
     */
    private RecordsResultSet readElements(
            RecordsOffset recordsOffset, LinkedList<Element> elements) {
        final Iterator<Element> it = elements.descendingIterator();
        while (it.hasNext()) {
            final Element element = it.next();
            if (element.recordsOffset().segmentId() == recordsOffset.segmentId()) {
                final MemoryRecordsResultSet recordsResultSet =
                        new MemoryRecordsResultSet(
                                Collections.singletonList(element),
                                recordsOffset.skipNextSegmentHead());
                readBytes.addAndGet(recordsResultSet.readBytes());
                return recordsResultSet;
            }
        }
        return new MemoryRecordsResultSet(recordsOffset);
    }

    @Override
    public void append(byte[] record, int offset, int length) {
        final ReentrantLock lock = this.lock;
        lock.lock();
        try {
            final Element element =
                    new Element(
                            record,
                            offset,
                            length,
                            new RecordsOffset(nextSegmentId.getAndIncrement(), 0));
            elements.add(element);
            writeBytes.addAndGet(length);
            notEmpty.signalAll();
        } finally {
            lock.unlock();
        }
    }

    @Override
    public RecordsResultSet poll(RecordsOffset recordsOffset, long time, TimeUnit unit)
            throws InterruptedException {

        if (recordsOffset.segmentId() < minCommitSegmentId.get()) {
            return new MemoryRecordsResultSet(recordsOffset.skip2TargetHead(lastSegmentId()));
        }
        final ReentrantLock lock = this.lock;
        lock.lock();
        try {
            final RecordsResultSet recordsResultSet = readElements(recordsOffset, elements);
            if (recordsResultSet.hasNext()) {
                return recordsResultSet;
            }
            if (time == 0) {
                notEmpty.await();
            } else if (!notEmpty.await(time, unit)) {
                return new MemoryRecordsResultSet(recordsOffset);
            }
            return readElements(recordsOffset, elements);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public long lastSegmentId() {
        return nextSegmentId.get() - 1;
    }

    @Override
    public long lag(RecordsOffset recordsOffset) {
        long lag = 0;
        final ReentrantLock lock = this.lock;
        lock.lock();
        try {
            final Iterator<Element> it = elements.descendingIterator();
            while (it.hasNext()) {
                final Element element = it.next();
                if (element.recordsOffset().segmentId() >= recordsOffset.segmentId()) {
                    lag += element.length();
                } else {
                    break;
                }
            }
        } finally {
            lock.unlock();
        }
        return lag;
    }

    @Override
    public void flush() throws IOException {}

    @Override
    public String topic() {
        return topic;
    }

    @Override
    public Set<String> groups() {
        return groupManager.groups();
    }

    @Override
    public int activeSegment() {
        final ReentrantLock lock = this.lock;
        lock.lock();
        try {
            return elements.size();
        } finally {
            lock.unlock();
        }
    }

    @Override
    public long writeBytes() {
        return writeBytes.get();
    }

    @Override
    public long readBytes() {
        return readBytes.get();
    }

    @Override
    public long pageCache() {
        return 0;
    }

    @Override
    public void close() throws IOException {
        if (closed.compareAndSet(false, true)) {
            IOUtils.closeQuietly(groupManager);
            elements.clear();
        }
    }
}
