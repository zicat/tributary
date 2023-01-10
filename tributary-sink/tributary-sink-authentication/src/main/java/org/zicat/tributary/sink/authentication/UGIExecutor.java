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

package org.zicat.tributary.sink.authentication;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.IOException;
import java.security.PrivilegedAction;
import java.security.PrivilegedExceptionAction;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/** UGIExecutor. @ Copy From Flume */
public class UGIExecutor implements PrivilegedExecutor {

    private final UserGroupInformation ugi;
    private static final long MIN_TIME_BEFORE_RELOGIN = 5 * 60 * 1000L;
    private volatile long lastReloginAttempt = 0;

    public UGIExecutor(UserGroupInformation ugi) {
        this.ugi = ugi;
        new ScheduledThreadPoolExecutor(
                        1,
                        new ThreadFactory() {
                            private final AtomicInteger threadId = new AtomicInteger();

                            @Override
                            public Thread newThread(Runnable r) {
                                final String name =
                                        "author_"
                                                + ugi.getUserName()
                                                + "_"
                                                + threadId.getAndIncrement();
                                final Thread t = new Thread(r, name);
                                t.setDaemon(true);
                                return t;
                            }
                        })
                .scheduleAtFixedRate(this::ensureValidAuth, 0, 10, TimeUnit.MINUTES);
    }

    @Override
    public <T> T execute(PrivilegedAction<T> action) {
        return action.run();
    }

    @Override
    public <T> T execute(PrivilegedExceptionAction<T> action) throws Exception {
        return action.run();
    }

    private void ensureValidAuth() {
        reloginUGI(ugi);
        if (ugi.getAuthenticationMethod().equals(UserGroupInformation.AuthenticationMethod.PROXY)) {
            reloginUGI(ugi.getRealUser());
        }
    }

    /*
     * lastReloginAttempt is introduced to avoid making the synchronized call
     *  ugi.checkTGTAndReloginFromKeytab() often, Hence this method is
     *  intentionally not synchronized, so that multiple threads can execute without
     *  the need to lock, which may result in an edge case where multiple threads
     *  simultaneously reading the lastReloginAttempt, and finding it > 5 minutes, can
     *  result in all of them attempting the checkTGT method, which is fine
     */
    private void reloginUGI(UserGroupInformation ugi) {
        try {
            if (ugi.hasKerberosCredentials()) {
                long now = System.currentTimeMillis();
                if (now - lastReloginAttempt < MIN_TIME_BEFORE_RELOGIN) {
                    return;
                }
                lastReloginAttempt = now;
                ugi.checkTGTAndReloginFromKeytab();
            }
        } catch (IOException e) {
            throw new SecurityException(
                    "Error trying to relogin from keytab for user " + ugi.getUserName(), e);
        }
    }

    @VisibleForTesting
    public String getUserName() {
        if (ugi != null) {
            return ugi.getUserName();
        } else {
            return null;
        }
    }
}
