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

package org.zicat.tributary.sink.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.RequestController;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;

import java.io.InterruptedIOException;
import java.util.Collection;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

/** custom controller. */
public class SimpleRequestController implements RequestController {

    final AtomicLong tasksInProgress = new AtomicLong(0);

    public SimpleRequestController(final Configuration configuration) {}

    @Override
    public Checker newChecker() {
        return new Checker() {
            @Override
            public ReturnCode canTakeRow(HRegionLocation loc, Row row) {
                return ReturnCode.INCLUDE;
            }

            @Override
            public void reset() {}
        };
    }

    @Override
    public void incTaskCounters(Collection<byte[]> regions, ServerName sn) {
        tasksInProgress.incrementAndGet();
    }

    @Override
    public void decTaskCounters(Collection<byte[]> regions, ServerName sn) {
        tasksInProgress.decrementAndGet();

        synchronized (tasksInProgress) {
            tasksInProgress.notifyAll();
        }
    }

    @Override
    public long getNumberOfTasksInProgress() {
        return tasksInProgress.get();
    }

    @Override
    public void waitForMaximumCurrentTasks(
            long max, long id, int periodToTrigger, Consumer<Long> trigger)
            throws InterruptedIOException {
        long lastLog = EnvironmentEdgeManager.currentTime();
        long currentInProgress, oldInProgress = Long.MAX_VALUE;
        while ((currentInProgress = tasksInProgress.get()) > max) {
            if (oldInProgress != currentInProgress) { // Wait for in progress to change.
                long now = EnvironmentEdgeManager.currentTime();
                if (now > lastLog + periodToTrigger) {
                    lastLog = now;
                    if (trigger != null) {
                        trigger.accept(currentInProgress);
                    }
                }
            }
            oldInProgress = currentInProgress;
            try {
                synchronized (tasksInProgress) {
                    if (tasksInProgress.get() == oldInProgress) {
                        tasksInProgress.wait(10);
                    }
                }
            } catch (InterruptedException e) {
                throw new InterruptedIOException(
                        "#" + id + ", interrupted." + " currentNumberOfTask=" + currentInProgress);
            }
        }
    }

    @Override
    public void waitForFreeSlot(long id, int periodToTrigger, Consumer<Long> trigger) {}
}
