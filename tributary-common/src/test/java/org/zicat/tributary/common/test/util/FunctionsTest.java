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

package org.zicat.tributary.common.test.util;

import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import static org.zicat.tributary.common.util.Functions.loopCloseableFunction;
import static org.zicat.tributary.common.util.Functions.runWithRetry;

/** FunctionsTest. */
public class FunctionsTest {

    @Test
    public void testRunWithRetry() {

        // success
        final AtomicInteger runTimes = new AtomicInteger();
        final Throwable e = runWithRetry(runTimes::incrementAndGet, 2, 1);
        Assert.assertNull(e);
        Assert.assertEquals(1, runTimes.get());

        // all fail
        final AtomicInteger runTimes2 = new AtomicInteger();
        final Throwable e2 =
                runWithRetry(
                        () -> {
                            runTimes2.incrementAndGet();
                            throw new RuntimeException("test");
                        },
                        2,
                        1);
        Assert.assertTrue(e2 != null && e2.getMessage() != null);
        Assert.assertEquals("test", e2.getMessage());
        Assert.assertEquals(3, runTimes2.get());

        // half fail
        final AtomicInteger runTimes3 = new AtomicInteger();
        final Throwable e3 =
                runWithRetry(
                        () -> {
                            if (runTimes3.incrementAndGet() == 2) {
                                return;
                            }
                            throw new RuntimeException("test");
                        },
                        3,
                        1);
        Assert.assertNull(e3);
        Assert.assertEquals(2, runTimes3.get());
    }

    @Test
    public void testLoopCloseableFunction() throws InterruptedException {
        final CountDownLatch countDownLatch = new CountDownLatch(1);
        final AtomicBoolean closed = new AtomicBoolean(false);
        final int period = 1;
        final Function<Object, Object> function =
                o -> {
                    countDownLatch.countDown();
                    return o;
                };
        final Thread t = new Thread(() -> loopCloseableFunction(function, period, closed));
        t.start();
        Assert.assertTrue(countDownLatch.await(5000, TimeUnit.MILLISECONDS));
        Assert.assertEquals(0, countDownLatch.getCount());
        closed.set(true);
        t.interrupt();
        Thread.sleep(period * 50);
        Assert.assertFalse(t.isAlive());
    }
}
