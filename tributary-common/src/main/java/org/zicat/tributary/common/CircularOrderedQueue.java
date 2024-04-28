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

package org.zicat.tributary.common;

import java.util.concurrent.atomic.AtomicLong;

/** CircularOrderedQueue. */
public class CircularOrderedQueue<T extends Comparable<T>> {
    private final AtomicLong matchCount = new AtomicLong(0);
    private final AtomicLong totalCount = new AtomicLong(0);
    private final int capacity;
    private final T[] array;
    private volatile HeadElement<T> headElement;

    public CircularOrderedQueue(T[] array) {
        this.array = array;
        this.capacity = array.length;
        this.headElement = new HeadElement<>(0, null);
    }

    /**
     * put value.
     *
     * @param value the value must order
     */
    public synchronized void put(T value) {
        if (value == null) {
            throw new IllegalArgumentException("The value must not null");
        }

        final T maxObject = headElement.object;
        if (maxObject != null && maxObject.compareTo(value) > 0) {
            throw new IllegalArgumentException("The value must order");
        }

        int offset = headElement.offset;
        array[offset] = value;
        offset++;
        if (offset >= capacity) {
            offset = 0;
        }
        headElement = new HeadElement<>(offset, value);
    }

    /**
     * find key.
     *
     * @param key key
     * @param handler handler
     * @return key
     * @param <T2> T2
     */
    public <T2> T find(T2 key, Compare<T, T2> handler) {
        final HeadElement<T> maxElement = this.headElement;
        if (maxElement.object == null) {
            return null;
        }
        final long compare = handler.compare(maxElement.object, key);
        if (compare == 0) {
            totalCount.incrementAndGet();
            matchCount.incrementAndGet();
            return maxElement.object;
        }
        if (compare < 0) {
            return null;
        }
        totalCount.incrementAndGet();
        final T result = findInner(maxElement, key, handler);
        if (result != null) {
            matchCount.incrementAndGet();
        }
        return result;
    }

    /**
     * find inner.
     *
     * @param key key
     * @param handler handler
     * @return T2
     * @param <T2> T
     */
    private <T2> T findInner(final HeadElement<T> maxElement, T2 key, Compare<T, T2> handler) {
        int end = maxElement.offset;
        int start = maxElement.offset - capacity;
        while (start < end) {
            if (end - start == 1) {
                final T current = array[toIndex(start)];
                return current == null || handler.compare(current, key) != 0 ? null : current;
            }
            final int offset = (end + start) / 2;
            final T current = array[toIndex(offset)];
            if (current == null) {
                start = offset + 1;
                continue;
            }
            final long compare = handler.compare(current, key);
            if (compare == 0) {
                return current;
            } else if (compare < 0) {
                start = offset + 1;
            } else {
                if (current.compareTo(maxElement.object) > 0) {
                    return null;
                }
                end = offset;
            }
        }
        return null;
    }

    /**
     * toIndex.
     *
     * @param offset offset
     * @return real index
     */
    private int toIndex(int offset) {
        return offset < 0 ? capacity + offset : offset;
    }

    /**
     * match count.
     *
     * @return match count.
     */
    public long matchCount() {
        return matchCount.get();
    }

    /**
     * total count.
     *
     * @return total.
     */
    public long totalCount() {
        return totalCount.get();
    }

    /**
     * block count.
     *
     * @return block count.
     */
    public int blockCount() {
        return capacity;
    }

    private static class HeadElement<T extends Comparable<T>> {
        private final int offset;
        private final T object;

        public HeadElement(int offset, T object) {
            this.offset = offset;
            this.object = object;
        }
    }

    /**
     * Compare.
     *
     * @param <T1>
     * @param <T2>
     */
    public interface Compare<T1, T2> {

        /**
         * compare.
         *
         * @param t1 t1
         * @param t2 t2
         * @return return 0 if equals, return > 0 if t1 > t2, return < 0 if t1 < t2
         */
        long compare(T1 t1, T2 t2);
    }
}
