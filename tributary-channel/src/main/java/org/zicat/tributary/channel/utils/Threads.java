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

package org.zicat.tributary.channel.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ThreadFactory;
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

        return new ThreadFactory() {
            private final AtomicInteger count = new AtomicInteger();

            @Override
            public Thread newThread(Runnable r) {
                final Thread t = new Thread(r);
                t.setDaemon(daemon);
                t.setName(prefixName + count.incrementAndGet());
                return t;
            }
        };
    }

    /**
     * join quietly.
     *
     * @param t thread
     */
    public static void joinQuietly(Thread t) {
        try {
            if (t != null) {
                t.join();
            }
        } catch (InterruptedException e) {
            LOG.warn("join interrupted", e);
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
}
