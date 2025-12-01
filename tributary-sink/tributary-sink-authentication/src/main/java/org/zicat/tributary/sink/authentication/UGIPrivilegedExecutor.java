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

import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import static org.zicat.tributary.common.util.Threads.createThreadFactoryByName;

import java.security.PrivilegedExceptionAction;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/** UGIExecutor. */
public class UGIPrivilegedExecutor implements PrivilegedExecutor {

    private static final Logger LOG = LoggerFactory.getLogger(UGIPrivilegedExecutor.class);
    private final UserGroupInformation ugi;
    private final String user;

    public UGIPrivilegedExecutor(UserGroupInformation ugi) {
        this.ugi = ugi;
        this.user = ugi.getUserName();
        final int checkIntervalMinutes = 10;
        final ScheduledExecutorService scheduler =
                Executors.newSingleThreadScheduledExecutor(
                        createThreadFactoryByName("author_" + user + "_"));
        scheduler.scheduleWithFixedDelay(
                () -> {
                    try {
                        LOG.info("start to relogin keytab for user {}", user);
                        ugi.checkTGTAndReloginFromKeytab();
                    } catch (Throwable e) {
                        LOG.warn("checkTGTAndReloginFromKeytab error for user {}", user, e);
                    }
                },
                checkIntervalMinutes,
                checkIntervalMinutes,
                TimeUnit.MINUTES);
        Runtime.getRuntime().addShutdownHook(new Thread(scheduler::shutdownNow));
    }

    @Override
    public <T> T execute(PrivilegedExceptionAction<T> action) throws Exception {
        return ugi.doAs(action);
    }

    /**
     * getUser.
     *
     * @return string
     */
    public String getUser() {
        return user;
    }
}
