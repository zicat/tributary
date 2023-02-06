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

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.time.StopWatch;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zicat.tributary.common.TributaryRuntimeException;

import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;

/** write data to specific hbase table. */
public abstract class AbstractHBaseWriter implements HBaseWriter {

    private static final Logger LOG = LoggerFactory.getLogger(AbstractHBaseWriter.class);
    private final HTableEntity hTableEntity;
    private Connection connection;
    private BufferedMutator mutator;

    /**
     * This is set from inside the {@link BufferedMutator.ExceptionListener} if a {@link Throwable}
     * was thrown.
     *
     * <p>Errors will be checked and rethrown before processing each input element, and when the
     * sink is closed.
     */
    private final AtomicReference<Throwable> failureThrowable = new AtomicReference<>();

    public AbstractHBaseWriter(HTableEntity hTableEntity) {
        this.hTableEntity = hTableEntity;
    }

    /**
     * open hbase writer.
     *
     * @return HbaseWriter
     */
    public AbstractHBaseWriter open() {
        try {
            connection = ConnectionFactory.createConnection(createConfiguration(hTableEntity));
            mutator =
                    connection.getBufferedMutator(
                            createBufferedMutatorParams(hTableEntity).listener(this));
            LOG.info("Create hbase writer succeed {} ", hTableEntity);
            return this;
        } catch (IOException ioe) {
            throw new TributaryRuntimeException(
                    "open HBaseWriter fail, table = " + hTableEntity, ioe);
        }
    }

    @Override
    public boolean appendData(Mutation mutation) throws IOException {
        checkErrorAndRethrow();
        mutator.mutate(mutation);
        return true;
    }

    /**
     * init configuration.
     *
     * @param hTableEntity hTableEntity.
     * @return Configuration
     */
    public abstract Configuration createConfiguration(HTableEntity hTableEntity);

    /**
     * create BufferedMutatorParams.
     *
     * @param hTableEntity hTableEntity
     * @return BufferedMutatorParams
     */
    public abstract BufferedMutatorParams createBufferedMutatorParams(HTableEntity hTableEntity);

    @Override
    public void close() throws IOException {
        try {
            flush();
        } finally {
            IOUtils.closeQuietly(mutator, connection);
            mutator = null;
            connection = null;
        }
    }

    /** check error and rethrow. */
    private void checkErrorAndRethrow() {
        final Throwable cause = failureThrowable.get();
        if (cause != null && failureThrowable.compareAndSet(cause, null)) {
            throw new TributaryRuntimeException("An error occurred in HBaseSink.", cause);
        }
    }

    @Override
    public void flush() throws IOException {
        if (Objects.isNull(mutator)) {
            return;
        }
        final StopWatch stopWatch = new StopWatch();
        stopWatch.start();
        mutator.flush();
        stopWatch.stop();
        final int overTime = overTimeFlushPrintWarn();
        if (stopWatch.getTime() > overTime) {
            LOG.warn(
                    "hbase sink cost too long, over time:{}, cost time:{}",
                    overTime,
                    stopWatch.getTime());
        }
    }

    /**
     * print warn over time.
     *
     * @return time ms
     */
    protected int overTimeFlushPrintWarn() {
        return 5000;
    }

    @Override
    public void onException(
            RetriesExhaustedWithDetailsException exception, BufferedMutator bufferedMutator) {
        if (!failureThrowable.compareAndSet(null, exception)) {
            LOG.error("hbase hook exception", exception);
        }
    }
}
