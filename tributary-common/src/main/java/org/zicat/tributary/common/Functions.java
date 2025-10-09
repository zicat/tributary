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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

/** Functions. */
public class Functions {

    private static final Logger LOG = LoggerFactory.getLogger(Functions.class);

    /**
     * run with retry.
     *
     * @param runnable runnable
     * @param maxRetries maxRetries
     * @param sleepOnFail sleepOnFail
     * @return Throwable
     */
    public static Exception runWithRetry(Runnable runnable, int maxRetries, long sleepOnFail) {
        int retryCount = 0;
        Exception firstE = null;
        do {
            try {
                runnable.run();
                return null;
            } catch (Exception t) {
                retryCount++;
                if (firstE == null) {
                    firstE = t;
                }
                Threads.sleepQuietly(sleepOnFail);
            }
        } while (retryCount <= maxRetries);
        return firstE;
    }

    /**
     * run with retry, throw exception if run fail.
     *
     * @param runnable runnable
     * @param maxRetries maxRetries
     * @param sleepOnFail sleepOnFail
     * @throws Exception Exception
     */
    public static void runWithRetryThrowException(
            Runnable runnable, int maxRetries, long sleepOnFail) throws Exception {
        final Exception e = runWithRetry(runnable, maxRetries, sleepOnFail);
        if (e != null) {
            throw e;
        }
    }

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
            if (waitTime > 0 && sleepQuietly(waitTime) && breakIfInterrupted) {
                return;
            }
        }
    }

    /**
     * loopCloseableFunction.
     *
     * @param runnable runnable
     * @param period period
     */
    public static void loopCloseableFunction(
            java.lang.Runnable runnable, long period, AtomicBoolean closed) {
        loopCloseableFunction(runnable, period, closed, false);
    }

    /**
     * loop closeable function.
     *
     * @param runnable runnable
     * @param period period
     * @param closed closed
     * @param breakIfInterrupted breakIfInterrupted
     */
    public static void loopCloseableFunction(
            java.lang.Runnable runnable,
            long period,
            AtomicBoolean closed,
            boolean breakIfInterrupted) {
        loopCloseableFunction(
                o -> {
                    runnable.run();
                    return null;
                },
                period,
                closed,
                breakIfInterrupted);
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

    /** Runnable. */
    public interface Runnable {

        /**
         * run method with exception.
         *
         * @throws Exception Exception
         */
        void run() throws Exception;
    }
}
