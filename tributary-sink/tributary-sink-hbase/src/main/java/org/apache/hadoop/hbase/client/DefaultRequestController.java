package org.apache.hadoop.hbase.client;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;

import java.io.InterruptedIOException;
import java.util.Collection;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

/** DefaultRequestController. */
public class DefaultRequestController implements RequestController {

    final AtomicLong tasksInProgress = new AtomicLong(0);

    public DefaultRequestController(final Configuration configuration) {}

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
