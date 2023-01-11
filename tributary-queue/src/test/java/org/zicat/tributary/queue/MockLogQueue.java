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

import org.zicat.tributary.queue.utils.VIntUtil;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/** MockV2LogQueue. */
public class MockLogQueue implements LogQueue {

    private final List<Element> elements = new ArrayList<>();
    private final Map<Integer, AtomicLong> partitionSegmentIds = new HashMap<>();
    private final Map<GroupPartition, RecordsOffset> groupManager = new HashMap<>();

    private final String topic;
    private final int partition;
    private long writeBytes = 0;
    private long readBytes = 0;

    public MockLogQueue() {
        this(1);
    }

    public MockLogQueue(int partition) {
        this("test", partition);
    }

    public MockLogQueue(String topic, int partition) {
        this.topic = topic;
        this.partition = partition;
    }

    @Override
    public synchronized void append(int partition, byte[] record, int offset, int length) {
        AtomicLong segmentId =
                partitionSegmentIds.computeIfAbsent(partition, k -> new AtomicLong(0));
        elements.add(
                new Element(
                        partition,
                        record,
                        offset,
                        length,
                        new RecordsOffset(segmentId.getAndIncrement(), 0)));
        writeBytes += length;
        notifyAll();
    }

    @Override
    public synchronized RecordsResultSet poll(
            int partition, RecordsOffset logOffset, long time, TimeUnit unit)
            throws InterruptedException {
        boolean hasWait = false;
        if (elements.isEmpty()) {
            wait(unit.toMillis(time));
            hasWait = true;
        }
        long searchSegmentId =
                logOffset.offset() > 0 ? logOffset.segmentId() + 1 : logOffset.segmentId();
        for (int i = elements.size() - 1; i >= 0; i--) {
            final Element element = elements.get(i);
            if (element.recordsOffset.segmentId() < searchSegmentId
                    && element.partition == partition) {
                break;
            }
            if (element.recordsOffset.segmentId() == searchSegmentId
                    && element.partition == partition) {
                ByteBuffer dataBuffer = ByteBuffer.allocate(5 + element.length);
                VIntUtil.putVInt(dataBuffer, element.length);
                dataBuffer.put(element.log, element.offset, element.length).flip();
                readBytes += dataBuffer.remaining();
                return new RecordsResultSetImpl(
                        new BufferRecordsOffset(
                                searchSegmentId, readBytes, null, null, dataBuffer, readBytes));
            }
        }
        if (!hasWait) {
            wait(unit.toMillis(time));
        }
        for (int i = elements.size() - 1; i >= 0; i--) {
            final Element element = elements.get(i);
            if (element.recordsOffset.segmentId() < searchSegmentId
                    && element.partition == partition) {
                break;
            }
            if (element.recordsOffset.segmentId() == searchSegmentId
                    && element.partition == partition) {
                ByteBuffer dataBuffer = ByteBuffer.allocate(5 + element.length);
                VIntUtil.putVInt(dataBuffer, element.length);
                dataBuffer.put(element.log, element.offset, element.length).flip();
                readBytes += dataBuffer.remaining();
                return new RecordsResultSetImpl(
                        new BufferRecordsOffset(
                                searchSegmentId, readBytes, null, null, dataBuffer, readBytes));
            }
        }
        return new RecordsResultSetImpl(BufferRecordsOffset.cast(logOffset).reset());
    }

    @Override
    public synchronized RecordsOffset getRecordsOffset(String groupId, int partition) {
        GroupPartition groupPartition = new GroupPartition(groupId, partition);
        RecordsOffset recordsOffset =
                groupManager.computeIfAbsent(
                        groupPartition, k -> RecordsOffset.startRecordOffset());
        return BufferRecordsOffset.cast(recordsOffset);
    }

    @Override
    public synchronized void commit(String groupId, int partition, RecordsOffset recordsOffset) {
        groupManager.put(new GroupPartition(groupId, partition), recordsOffset);
    }

    @Override
    public RecordsOffset getMinRecordsOffset(int partition) {
        RecordsOffset min = null;
        for (Map.Entry<GroupPartition, RecordsOffset> entry : groupManager.entrySet()) {
            if (entry.getKey().partition != partition) {
                continue;
            }
            min = min == null ? entry.getValue() : RecordsOffset.min(min, entry.getValue());
        }
        return min;
    }

    @Override
    public synchronized void flush() {}

    @Override
    public synchronized String topic() {
        return topic;
    }

    @Override
    public synchronized int partition() {
        return partition;
    }

    @Override
    public synchronized int activeSegment() {
        return elements.size();
    }

    @Override
    public synchronized long lag(int partition, RecordsOffset recordsOffset) {
        long lag = 0;
        long segmentId =
                recordsOffset.offset() > 0
                        ? recordsOffset.segmentId() + 1
                        : recordsOffset.segmentId();
        for (int i = elements.size() - 1; i >= 0; i--) {
            final Element element = elements.get(i);
            if (element.partition != partition) {
                continue;
            }
            if (element.recordsOffset.segmentId() >= segmentId) {
                lag += element.length;
            } else {
                break;
            }
        }
        return lag;
    }

    @Override
    public synchronized long lastSegmentId(int partition) {
        for (int i = elements.size() - 1; i >= 0; i--) {
            final Element element = elements.get(i);
            if (element.partition == partition) {
                return element.recordsOffset.segmentId();
            }
        }
        return 0;
    }

    @Override
    public long writeBytes() {
        return writeBytes;
    }

    @Override
    public long readBytes() {
        return readBytes;
    }

    @Override
    public long pageCache() {
        return 0;
    }

    @Override
    public void close() {
        elements.clear();
        readBytes = 0;
    }

    private static class GroupPartition {
        private final String group;
        private final int partition;

        public GroupPartition(String group, int partition) {
            this.group = group;
            this.partition = partition;
        }

        public String getGroup() {
            return group;
        }

        public int getPartition() {
            return partition;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            GroupPartition that = (GroupPartition) o;
            return partition == that.partition && Objects.equals(group, that.group);
        }

        @Override
        public int hashCode() {
            return Objects.hash(group, partition);
        }
    }

    /** Element. */
    private static class Element {
        private final int partition;
        private final byte[] log;
        private final int offset;
        private final int length;
        private final RecordsOffset recordsOffset;

        Element(int partition, byte[] log, int offset, int length, RecordsOffset recordsOffset) {
            this.partition = partition;
            this.log = log;
            this.offset = offset;
            this.length = length;
            this.recordsOffset = recordsOffset;
        }

        public int getPartition() {
            return partition;
        }

        public byte[] getLog() {
            return log;
        }

        public int getOffset() {
            return offset;
        }

        public int getLength() {
            return length;
        }
    }
}
