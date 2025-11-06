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

import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/** ThreadPoolUtils. */
public class Threads {

    private static final Logger LOG = LoggerFactory.getLogger(Threads.class);

    /**
     * create thread factory by prefix name.
     *
     * @param prefixName prefix name
     * @param daemon daemon
     * @return ThreadFactory
     */
    public static ThreadFactory createThreadFactoryByName(String prefixName, boolean daemon) {
        return createThreadFactoryByName(prefixName, daemon, null);
    }

    /**
     * create thread factory by prefix name.
     *
     * @param prefixName prefix name
     * @param daemon daemon
     * @return ThreadFactory
     */
    public static ThreadFactory createThreadFactoryByName(
            String prefixName, boolean daemon, ThreadGroup threadGroup) {

        return new ThreadFactory() {
            private final AtomicInteger count = new AtomicInteger();

            @Override
            public Thread newThread(@NotNull Runnable r) {
                final Thread t = new Thread(threadGroup, r, prefixName + count.incrementAndGet());
                t.setDaemon(daemon);
                return t;
            }
        };
    }

    /**
     * create thread factory by prefix name.
     *
     * @param prefixName prefix name
     * @return ThreadFactory
     */
    public static ThreadFactory createThreadFactoryByName(String prefixName) {
        return createThreadFactoryByName(prefixName, false);
    }

    /**
     * join quietly.
     *
     * @param ts threads
     */
    public static void joinQuietly(Thread... ts) {
        if (ts == null) {
            return;
        }
        for (Thread t : ts) {
            try {
                t.join();
            } catch (InterruptedException e) {
                LOG.warn("join interrupted", e);
            }
        }
    }

    /**
     * sleep quietly.
     *
     * @param millis millis
     */
    public static void sleepQuietly(long millis) {
        try {
            if (millis > 0) {
                Thread.sleep(millis);
            }
        } catch (InterruptedException e) {
            LOG.info("sleep interrupted", e);
        }
    }

    /**
     * interrupt quietly.
     *
     * @param ts threads
     */
    public static void interruptQuietly(Thread... ts) {
        if (ts == null) {
            return;
        }
        for (Thread t : ts) {
            try {
                t.interrupt();
            } catch (SecurityException e) {
                LOG.warn("interrupt thread {} error", t.getName(), e);
            }
        }
    }

    /**
     * start closeable cron job.
     *
     * @param closed closed
     * @param name name
     * @param runnable runnable
     * @param period period
     * @return Thread
     */
    public static Thread startClosableCronJob(
            AtomicBoolean closed, String name, Runnable runnable, long period) {
        final Runnable task = () -> Functions.loopCloseableFunction(runnable, period, closed);
        final Thread thread = new Thread(task, name);
        thread.start();
        return thread;
    }
}
