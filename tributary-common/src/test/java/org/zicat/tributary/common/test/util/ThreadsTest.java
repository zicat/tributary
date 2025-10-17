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
import org.zicat.tributary.common.util.Threads;

import java.util.concurrent.ThreadFactory;

/** ThreadsTest. */
public class ThreadsTest {

    @Test
    public void testJoinQuietly() {

        final Thread sleepThread =
                new Thread(
                        () -> {
                            try {
                                Thread.sleep(500);
                            } catch (InterruptedException ignore) {

                            }
                        });
        final Thread test =
                new Thread(
                        () -> {
                            try {
                                Threads.joinQuietly(sleepThread);
                                Assert.assertTrue(Thread.currentThread().isInterrupted());
                            } catch (Throwable e) {
                                Assert.fail("interrupted not be really caught");
                            }
                        });
        sleepThread.start();
        test.start();
        test.interrupt();
        sleepThread.interrupt();
    }

    @Test
    public void testSleepQuietly() {

        final Thread test =
                new Thread(
                        () -> {
                            try {
                                Threads.sleepQuietly(500);
                                Assert.assertTrue(Thread.currentThread().isInterrupted());
                            } catch (Throwable e) {
                                Assert.fail("interrupted not be really caught");
                            }
                        });
        test.start();
        test.interrupt();
    }

    @Test
    public void testCreateThreadFactoryByName() {

        final String prefixName = "test";
        final ThreadFactory factory = Threads.createThreadFactoryByName(prefixName, true);
        Thread t1 = factory.newThread(() -> {});
        Assert.assertTrue(t1.getName().startsWith(prefixName));
        Assert.assertTrue(t1.isDaemon());
        Assert.assertNotEquals(t1.getName(), factory.newThread(() -> {}).getName());

        final ThreadFactory factory2 = Threads.createThreadFactoryByName(prefixName, false);
        Assert.assertFalse(factory2.newThread(() -> {}).isDaemon());
    }
}
