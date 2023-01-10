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

package org.zicat.tributary.queue.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

/** Functions. */
public class Functions {

    private static final Logger LOG = LoggerFactory.getLogger(Functions.class);

    /**
     * loopCloseableFunction.
     *
     * @param function function
     * @param period period
     */
    public static void loopCloseableFunction(
            Function<Object, Object> function, long period, AtomicBoolean closed) {
        loopCloseableFunction(function, period, closed, false);
    }

    /**
     * loop closeable function.
     *
     * @param function function
     * @param period period
     * @param closed closed
     * @param breakIfInterrupted breakIfInterrupted
     */
    public static void loopCloseableFunction(
            Function<Object, Object> function,
            long period,
            AtomicBoolean closed,
            boolean breakIfInterrupted) {

        if (sleepQuietly(period) && breakIfInterrupted) {
            return;
        }
        Object result = null;
        while (!closed.get()) {
            final long start = System.currentTimeMillis();
            try {
                result = function.apply(result);
            } catch (Throwable e) {
                LOG.warn("call back error", e);
            }
            if (closed.get()) {
                return;
            }
            final long waitTime = period - (System.currentTimeMillis() - start);
            final long leastWait = waitTime <= 0 ? 10 : waitTime;
            if (sleepQuietly(leastWait) && breakIfInterrupted) {
                return;
            }
        }
    }

    /**
     * sleep ignore interrupted.
     *
     * @param period period
     */
    private static boolean sleepQuietly(long period) {
        try {
            Thread.sleep(period);
            return false;
        } catch (InterruptedException ignore) {
            return true;
        }
    }
}
