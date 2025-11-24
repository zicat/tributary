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

package org.apache.hadoop.hbase.client;

import com.conversantmedia.util.concurrent.DisruptorBlockingQueue;
import com.conversantmedia.util.concurrent.SpinPolicy;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ipc.RpcControllerFactory;
import org.apache.hbase.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/** BufferedMutatorImpl2. */
@SuppressWarnings({"unchecked", "rawtypes"})
public class BufferedMutatorImpl2 implements BufferedMutator {

    private static final Logger LOG = LoggerFactory.getLogger(BufferedMutatorImpl2.class);

    private final ExceptionListener listener;

    private final TableName tableName;

    private static final String MAX_PENDING_TASK = "hbase.client.write.pending.task.max";
    private static final int DEFAULT_MAX_PENDING_TASK = 2;

    private final Configuration conf;
    private final DisruptorBlockingQueue<Mutation> writeAsyncBuffer;
    private final AtomicLong currentWriteCounter = new AtomicLong(0);
    private final AtomicInteger undealtMutationCount = new AtomicInteger(0);

    private final long writeBufferSize;

    private final AtomicLong writeBufferPeriodicFlushTimeoutMs = new AtomicLong(0);
    private final AtomicLong writeBufferPeriodicFlushTimerTickMs =
            new AtomicLong(BufferedMutator.MIN_WRITE_BUFFER_PERIODIC_FLUSH_TIMERTICK_MS);
    private Timer writeBufferPeriodicFlushTimer = null;

    private final int maxKeyValueSize;
    private final ExecutorService pool;
    private final AtomicInteger rpcTimeout;
    private final AtomicInteger operationTimeout;
    private final boolean cleanupPoolOnClose;
    private volatile boolean closed = false;
    private final AsyncProcess ap;
    private final Queue<AsyncRequestFuture> pendingTasks = new ConcurrentLinkedQueue<>();
    private final int maxPendingTask;

    @VisibleForTesting
    BufferedMutatorImpl2(ClusterConnection conn, BufferedMutatorParams params, AsyncProcess ap) {
        if (conn == null || conn.isClosed()) {
            throw new IllegalArgumentException("Connection is null or closed.");
        }
        this.tableName = params.getTableName();
        this.conf = conn.getConfiguration();
        this.listener = params.getListener();
        if (params.getPool() == null) {
            this.pool = HTable.getDefaultExecutor(conf);
            cleanupPoolOnClose = true;
        } else {
            this.pool = params.getPool();
            cleanupPoolOnClose = false;
        }
        ConnectionConfiguration tableConf = new ConnectionConfiguration(conf);
        this.writeBufferSize =
                params.getWriteBufferSize() != BufferedMutatorParams.UNSET
                        ? params.getWriteBufferSize()
                        : tableConf.getWriteBufferSize();
        int bufferSize = Math.max((int) writeBufferSize, (int) writeBufferSize * 2);
        writeAsyncBuffer = new DisruptorBlockingQueue<>(bufferSize, SpinPolicy.BLOCKING);
        // Set via the setter because it does value validation and starts/stops the TimerTask
        long newWriteBufferPeriodicFlushTimeoutMs =
                params.getWriteBufferPeriodicFlushTimeoutMs() != BufferedMutatorParams.UNSET
                        ? params.getWriteBufferPeriodicFlushTimeoutMs()
                        : tableConf.getWriteBufferPeriodicFlushTimeoutMs();
        long newWriteBufferPeriodicFlushTimerTickMs =
                params.getWriteBufferPeriodicFlushTimerTickMs() != BufferedMutatorParams.UNSET
                        ? params.getWriteBufferPeriodicFlushTimerTickMs()
                        : tableConf.getWriteBufferPeriodicFlushTimerTickMs();
        this.setWriteBufferPeriodicFlush(
                newWriteBufferPeriodicFlushTimeoutMs, newWriteBufferPeriodicFlushTimerTickMs);

        this.maxKeyValueSize =
                params.getMaxKeyValueSize() != BufferedMutatorParams.UNSET
                        ? params.getMaxKeyValueSize()
                        : tableConf.getMaxKeyValueSize();

        this.rpcTimeout =
                new AtomicInteger(
                        params.getRpcTimeout() != BufferedMutatorParams.UNSET
                                ? params.getRpcTimeout()
                                : conn.getConnectionConfiguration().getWriteRpcTimeout());

        this.operationTimeout =
                new AtomicInteger(
                        params.getOperationTimeout() != BufferedMutatorParams.UNSET
                                ? params.getOperationTimeout()
                                : conn.getConnectionConfiguration().getOperationTimeout());
        this.ap = ap;
        this.maxPendingTask = conf.getInt(MAX_PENDING_TASK, DEFAULT_MAX_PENDING_TASK);
    }

    BufferedMutatorImpl2(
            ClusterConnection conn,
            RpcRetryingCallerFactory rpcCallerFactory,
            RpcControllerFactory rpcFactory,
            BufferedMutatorParams params) {
        this(
                conn,
                params,
                // puts need to track errors globally due to how the APIs currently work.
                new AsyncProcess(conn, conn.getConfiguration(), rpcCallerFactory, rpcFactory));
    }

    private void checkClose() {
        if (closed) {
            throw new IllegalStateException("Cannot put when the BufferedMutator is closed.");
        }
    }

    @VisibleForTesting
    ExecutorService getPool() {
        return pool;
    }

    @VisibleForTesting
    AsyncProcess getAsyncProcess() {
        return ap;
    }

    @Override
    public TableName getName() {
        return tableName;
    }

    @Override
    public Configuration getConfiguration() {
        return conf;
    }

    @Override
    public void mutate(Mutation m)
            throws InterruptedIOException, RetriesExhaustedWithDetailsException {
        mutate(Collections.singletonList(m));
    }

    @Override
    public void mutate(List<? extends Mutation> ms)
            throws InterruptedIOException, RetriesExhaustedWithDetailsException {
        checkClose();

        int toAddCount = 0;
        for (Mutation m : ms) {
            if (m instanceof Put) {
                ConnectionUtils.validatePut((Put) m, maxKeyValueSize);
            }
            ++toAddCount;
        }

        if (currentWriteCounter.get() == 0) {
            firstRecordInBufferTimestamp.set(System.currentTimeMillis());
        }
        currentWriteCounter.addAndGet(toAddCount);
        for (Mutation m : ms) {
            try {
                writeAsyncBuffer.put(m);
            } catch (InterruptedException e) {
                throw new InterruptedIOException(e.toString());
            }
        }
        undealtMutationCount.addAndGet(toAddCount);
        doFlush(false);
    }

    @VisibleForTesting
    protected long getExecutedWriteBufferPeriodicFlushes() {
        return executedWriteBufferPeriodicFlushes.get();
    }

    private final AtomicLong firstRecordInBufferTimestamp = new AtomicLong(0);
    private final AtomicLong executedWriteBufferPeriodicFlushes = new AtomicLong(0);

    private void timerCallbackForWriteBufferPeriodicFlush() {
        if (currentWriteCounter.get() == 0) {
            return; // Nothing to flush
        }
        long now = System.currentTimeMillis();
        if (firstRecordInBufferTimestamp.get() + writeBufferPeriodicFlushTimeoutMs.get() > now) {
            return; // No need to flush yet
        }
        // The first record in the writebuffer has been in there too long --> flush
        try {
            executedWriteBufferPeriodicFlushes.incrementAndGet();
            checkClose();
            doFlush(true);
        } catch (InterruptedIOException | RetriesExhaustedWithDetailsException e) {
            LOG.error("Exception during timerCallbackForWriteBufferPeriodicFlush", e);
        }
    }

    @Override
    public synchronized void close() throws IOException {
        if (closed) {
            return;
        }
        // Stop any running Periodic Flush timer.
        disableWriteBufferPeriodicFlush();
        try {
            // As we can have an operation in progress even if the buffer is empty, we call
            // doFlush at least one time.
            doFlush(true);
            checkPendingTasks(0);
        } finally {
            if (cleanupPoolOnClose) {
                this.pool.shutdown();
                try {
                    if (!pool.awaitTermination(600, TimeUnit.SECONDS)) {
                        LOG.warn(
                                "close() failed to terminate pool after 10 minutes. Abandoning pool.");
                    }
                } catch (InterruptedException e) {
                    LOG.warn("waitForTermination interrupted");
                    Thread.currentThread().interrupt();
                }
            }
            closed = true;
        }
    }

    private AsyncProcessTask createTask(QueueRowAccess access) {
        return new AsyncProcessTask(
                AsyncProcessTask.newBuilder()
                        .setPool(pool)
                        .setTableName(tableName)
                        .setRowAccess(access)
                        .setSubmittedRows(AsyncProcessTask.SubmittedRows.AT_LEAST_ONE)
                        .build()) {
            @Override
            public int getRpcTimeout() {
                return rpcTimeout.get();
            }

            @Override
            public int getOperationTimeout() {
                return operationTimeout.get();
            }
        };
    }

    @Override
    public void flush() throws InterruptedIOException, RetriesExhaustedWithDetailsException {
        checkClose();
        doFlush(true);
        checkPendingTasks(0);
    }

    /**
     * Send the operations in the buffer to the servers.
     *
     * @param flushAll - if true, sends all the writes and wait for all of them to finish before
     *     returning. Otherwise, flush until buffer size is smaller than threshold
     */
    private void doFlush(boolean flushAll)
            throws InterruptedIOException, RetriesExhaustedWithDetailsException {
        List<RetriesExhaustedWithDetailsException> errors = new ArrayList<>();
        while (true) {
            if (!flushAll && currentWriteCounter.get() <= writeBufferSize) {
                // There is the room to accept more mutations.
                break;
            }
            AsyncRequestFuture asf;
            try (QueueRowAccess access = createQueueRowAccess()) {
                if (access.isEmpty()) {
                    // It means someone has gotten the ticker to run the flush.
                    break;
                }
                asf = ap.submit(createTask(access));
            }
            // DON'T do the wait in the try-with-resources. Otherwise, the undealt mutations won't
            // be released.
            if (!flushAll) {
                pendingTasks.add(asf);
            } else {
                asf.waitUntilDone();
                if (asf.hasError()) {
                    errors.add(asf.getErrors());
                }
            }
            checkPendingTasks(maxPendingTask);
        }

        RetriesExhaustedWithDetailsException exception = makeException(errors);
        if (exception == null) {
            return;
        }
        if (listener == null) {
            throw exception;
        } else {
            listener.onException(exception, this);
        }
    }

    private void checkPendingTasks(int checkCount)
            throws InterruptedIOException, RetriesExhaustedWithDetailsException {
        List<RetriesExhaustedWithDetailsException> errors = new ArrayList<>();
        while (pendingTasks.size() > checkCount) {
            AsyncRequestFuture pendingFuture = pendingTasks.poll();
            if (pendingFuture == null) {
                break;
            }
            pendingFuture.waitUntilDone();
            if (pendingFuture.hasError()) {
                if (isBackPressure(pendingFuture.getErrors())) {
                    if (pendingFuture.getFailedOperations() != null) {
                        for (Row row : pendingFuture.getFailedOperations()) {
                            if (row instanceof Mutation) {
                                mutate((Mutation) row);
                            }
                        }
                    }
                    checkCount = Math.max(0, checkCount - 1);
                } else {
                    errors.add(pendingFuture.getErrors());
                }
            }
        }
        RetriesExhaustedWithDetailsException exception = makeException(errors);
        if (exception == null) {
            return;
        }
        if (listener == null) {
            throw exception;
        } else {
            listener.onException(exception, this);
        }
    }

    /**
     * check whether server back pressure.
     *
     * @param exception exception
     * @return true if back pressure
     */
    private static boolean isBackPressure(RetriesExhaustedWithDetailsException exception) {
        return exception.getMessage() != null
                && exception.getMessage().contains("CallQueueTooBigException");
    }

    private static RetriesExhaustedWithDetailsException makeException(
            List<RetriesExhaustedWithDetailsException> errors) {
        switch (errors.size()) {
            case 0:
                return null;
            case 1:
                return errors.get(0);
            default:
                List<Throwable> exceptions = new ArrayList<>();
                List<Row> actions = new ArrayList<>();
                List<String> hostnameAndPort = new ArrayList<>();
                errors.forEach(
                        e -> {
                            exceptions.addAll(e.exceptions);
                            actions.addAll(e.actions);
                            hostnameAndPort.addAll(e.hostnameAndPort);
                        });
                return new RetriesExhaustedWithDetailsException(
                        exceptions, actions, hostnameAndPort);
        }
    }

    /** {@inheritDoc} */
    @Override
    public long getWriteBufferSize() {
        return this.writeBufferSize;
    }

    @Override
    public synchronized void setWriteBufferPeriodicFlush(long timeoutMs, long timerTickMs) {
        long originalTimeoutMs = this.writeBufferPeriodicFlushTimeoutMs.get();
        long originalTimerTickMs = this.writeBufferPeriodicFlushTimerTickMs.get();

        // Both parameters have minimal values.
        writeBufferPeriodicFlushTimeoutMs.set(Math.max(0, timeoutMs));
        writeBufferPeriodicFlushTimerTickMs.set(
                Math.max(
                        BufferedMutator.MIN_WRITE_BUFFER_PERIODIC_FLUSH_TIMERTICK_MS, timerTickMs));

        // If something changed we stop the old Timer.
        if (writeBufferPeriodicFlushTimeoutMs.get() != originalTimeoutMs
                || writeBufferPeriodicFlushTimerTickMs.get() != originalTimerTickMs) {
            if (writeBufferPeriodicFlushTimer != null) {
                writeBufferPeriodicFlushTimer.cancel();
                writeBufferPeriodicFlushTimer = null;
            }
        }

        // If we have the need for a timer and there is none we start it
        if (writeBufferPeriodicFlushTimer == null && writeBufferPeriodicFlushTimeoutMs.get() > 0) {
            writeBufferPeriodicFlushTimer = new Timer(true); // Create Timer running as Daemon.
            writeBufferPeriodicFlushTimer.schedule(
                    new TimerTask() {
                        @Override
                        public void run() {
                            BufferedMutatorImpl2.this.timerCallbackForWriteBufferPeriodicFlush();
                        }
                    },
                    writeBufferPeriodicFlushTimerTickMs.get(),
                    writeBufferPeriodicFlushTimerTickMs.get());
        }
    }

    @Override
    public long getWriteBufferPeriodicFlushTimeoutMs() {
        return writeBufferPeriodicFlushTimeoutMs.get();
    }

    @Override
    public long getWriteBufferPeriodicFlushTimerTickMs() {
        return writeBufferPeriodicFlushTimerTickMs.get();
    }

    @Override
    public void setRpcTimeout(int rpcTimeout) {
        this.rpcTimeout.set(rpcTimeout);
    }

    @Override
    public void setOperationTimeout(int operationTimeout) {
        this.operationTimeout.set(operationTimeout);
    }

    @VisibleForTesting
    long getCurrentWriteCounter() {
        return currentWriteCounter.get();
    }

    /**
     * Count the mutations which haven't been processed.
     *
     * @return count of undealt mutation
     */
    @VisibleForTesting
    int size() {
        return undealtMutationCount.get();
    }

    /**
     * Count the mutations which haven't been flushed.
     *
     * @return count of unflushed mutation
     */
    @VisibleForTesting
    int getUnflushedSize() {
        return writeAsyncBuffer.size();
    }

    @VisibleForTesting
    QueueRowAccess createQueueRowAccess() {
        return new QueueRowAccess();
    }

    @VisibleForTesting
    class QueueRowAccess implements RowAccess<Row>, Closeable {
        private int remainder = undealtMutationCount.getAndSet(0);
        private Mutation last = null;

        private void restoreLastMutation() {
            // restore the last mutation since it isn't submitted
            if (last != null) {
                try {
                    writeAsyncBuffer.put(last);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                currentWriteCounter.addAndGet(1);
                last = null;
            }
        }

        @Override
        public void close() {
            restoreLastMutation();
            if (remainder > 0) {
                undealtMutationCount.addAndGet(remainder);
                remainder = 0;
            }
        }

        @Override
        @NotNull
        public Iterator<Row> iterator() {
            return new Iterator<Row>() {
                private int countDown = remainder;

                @Override
                public boolean hasNext() {
                    return countDown > 0;
                }

                @Override
                public Row next() {
                    restoreLastMutation();
                    if (!hasNext()) {
                        throw new NoSuchElementException();
                    }
                    last = writeAsyncBuffer.poll();
                    if (last == null) {
                        throw new NoSuchElementException();
                    }
                    currentWriteCounter.addAndGet(-1);
                    --countDown;
                    return last;
                }

                @Override
                public void remove() {
                    if (last == null) {
                        throw new IllegalStateException();
                    }
                    --remainder;
                    last = null;
                }
            };
        }

        @Override
        public int size() {
            return remainder;
        }

        @Override
        public boolean isEmpty() {
            return remainder <= 0;
        }
    }
}
