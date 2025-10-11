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

import java.util.LinkedHashMap;
import java.util.Map;

/** SimpleLRUCache. */
public class SimpleLRUCache<K, V> extends LinkedHashMap<K, V> {

    private final int maxSize;

    private SimpleLRUCache(int maxSize) {
        // order by put desc
        this(maxSize, false);
    }

    private SimpleLRUCache(int maxSize, boolean accessOrder) {
        super(maxSize, 0.75f, accessOrder);
        this.maxSize = maxSize;
    }

    @Override
    protected boolean removeEldestEntry(Map.Entry<K, V> eldest) {
        return size() > maxSize;
    }

    /**
     * create LRUCache.
     *
     * @param maxSize maxSize
     * @param accessOrder accessOrder
     * @return map
     * @param <K> k
     * @param <V> v
     */
    public static <K, V> Map<K, V> create(int maxSize, boolean accessOrder) {
        return java.util.Collections.synchronizedMap(new SimpleLRUCache<>(maxSize, accessOrder));
    }

    /**
     * create LRUCache.
     *
     * @param maxSize maxSize
     * @return map
     * @param <K> k
     * @param <V> v
     */
    public static <K, V> Map<K, V> create(int maxSize) {
        return java.util.Collections.synchronizedMap(new SimpleLRUCache<>(maxSize));
    }
}
