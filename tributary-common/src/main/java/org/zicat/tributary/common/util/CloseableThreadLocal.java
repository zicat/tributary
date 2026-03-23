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

package org.zicat.tributary.common.util;

import java.io.Closeable;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import java.util.function.Supplier;

/**
 * CloseableThreadLocal.
 *
 * @param <T>
 */
public class CloseableThreadLocal<T extends Closeable> extends ThreadLocal<T> implements Closeable {

    private final Supplier<T> supplier;
    private final Map<Thread, T> allInstances = new ConcurrentHashMap<>();
    private final ScheduledExecutorService es = Executors.newScheduledThreadPool(1);

    public CloseableThreadLocal(Supplier<T> supplier) {
        this.supplier = supplier;
        this.es.scheduleWithFixedDelay(
                () -> evict(thread -> !thread.isAlive()), 1, 1, TimeUnit.SECONDS);
    }

    @Override
    protected T initialValue() {
        final T value = supplier.get();
        allInstances.put(Thread.currentThread(), value);
        return value;
    }

    /**
     * evict .
     *
     * @param predicate predicate
     */
    private synchronized void evict(Predicate<Thread> predicate) {
        final Iterator<Entry<Thread, T>> it = allInstances.entrySet().iterator();
        while (it.hasNext()) {
            final Entry<Thread, T> entry = it.next();
            final Thread t = entry.getKey();
            final T instance = entry.getValue();
            if (predicate.test(t)) {
                it.remove();
                IOUtils.closeQuietly(instance);
            }
        }
    }

    @Override
    public void close() {
        try {
            es.shutdown();
        } finally {
            evict(thread -> true);
        }
    }
}
