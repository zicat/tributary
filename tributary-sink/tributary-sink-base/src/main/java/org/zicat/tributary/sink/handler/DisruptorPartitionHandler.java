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

package org.zicat.tributary.sink.handler;

import com.lmax.disruptor.TimeoutBlockingWaitStrategy;
import com.lmax.disruptor.WorkHandler;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
import lombok.Data;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zicat.tributary.queue.LogQueue;
import org.zicat.tributary.queue.RecordsOffset;
import org.zicat.tributary.sink.SinkGroupConfig;
import org.zicat.tributary.sink.function.AbstractFunction;
import org.zicat.tributary.sink.function.Function;
import org.zicat.tributary.sink.function.Trigger;
import org.zicat.tributary.sink.utils.Threads;

import java.io.Closeable;
import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static org.zicat.tributary.sink.utils.Collections.copy;

/**
 * Disruptor sink handler.
 *
 * <p>Multi Threads mode, One ${@link DisruptorPartitionHandler} instance bind with at least one
 * {@link Function} instance
 *
 * <p>Set threads and {@link Function} count by ${@link DisruptorPartitionHandler#KEY_THREADS} .
 */
public class DisruptorPartitionHandler extends AbstractPartitionHandler {

    private static final Logger LOG = LoggerFactory.getLogger(DisruptorPartitionHandler.class);

    public static final String KEY_THREADS = "threads";
    public static final String KEY_BUFFER_SIZE = "bufferSize";

    private static final int MAX_CAPACITY = 128;
    private static final int MIN_CAPACITY = 4;
    private static final int DEFAULT_THREADS = 2;

    private Disruptor<Block> disruptor;
    private DataHandler[] handlers;
    private final AtomicReference<Throwable> error = new AtomicReference<>(null);

    public DisruptorPartitionHandler(
            String groupId, LogQueue logQueue, int partitionId, SinkGroupConfig sinkGroupConfig) {
        super(groupId, logQueue, partitionId, sinkGroupConfig);
    }

    @Override
    public void open() {

        final int workerNumber = sinkGroupConfig.getCustomProperty(KEY_THREADS, DEFAULT_THREADS);
        if (workerNumber <= 1) {
            throw new IllegalStateException(KEY_THREADS + " must over 1");
        }
        this.disruptor = createDisruptor(workerNumber);
        this.handlers = new DataHandler[workerNumber];
        for (int i = 0; i < workerNumber; i++) {
            handlers[i] = new DataHandler(createFunction(createFunctionId(i)), error);
            LOG.info("Disruptor Data handler initialed, GroupId:{}, Id:{}", groupId, i);
        }
        disruptor.handleEventsWithWorkerPool(handlers);
        disruptor.start();
    }

    /**
     * create function id.
     *
     * @param offset offset
     * @return string
     */
    public String createFunctionId(int offset) {
        return getSinHandlerId() + "_" + offset;
    }

    /**
     * return handlers.
     *
     * @return handler array
     */
    public DataHandler[] handlers() {
        return handlers;
    }

    /**
     * create disruptor, subclass can override this function.
     *
     * @param workNumber work number
     * @return disruptor
     */
    protected Disruptor<Block> createDisruptor(int workNumber) {
        return new Disruptor<>(
                Block::new,
                formatCap(sinkGroupConfig.getCustomProperty(KEY_BUFFER_SIZE, workNumber * 3)),
                Threads.createThreadFactoryByName(threadName() + "-", true),
                ProducerType.SINGLE,
                new TimeoutBlockingWaitStrategy(30, TimeUnit.SECONDS));
    }

    @Override
    public void process(RecordsOffset recordsOffset, Iterator<byte[]> iterator) throws Exception {
        checkProcessError();
        disruptor.publishEvent(
                (block, sequence) -> {
                    /*
                     *  After process finished, source iterator will fill next data from log queue.
                     *  Put iterator to memory queue may cause data lost or exception,
                     *  because Consumer Thread may deal with the iterator later than next process.
                     */
                    final Iterator<byte[]> copy = copy(iterator);
                    block.setIterator(copy);
                    block.setOffset(recordsOffset);
                });
    }

    /**
     * check process error.
     *
     * @throws Exception Exception
     */
    private void checkProcessError() throws Exception {
        final Throwable e = error.get();
        if (e != null) {
            error.set(null);
            throw (e instanceof Exception ? (Exception) e : new Exception(e));
        }
    }

    @Override
    public void closeCallback() throws IOException {
        try {
            if (handlers != null) {
                IOException e = null;
                for (DataHandler handler : handlers) {
                    try {
                        handler.close();
                    } catch (IOException ioException) {
                        e = ioException;
                    }
                }
                handlers = null;
                if (e != null) {
                    throw e;
                }
            }
        } finally {
            if (disruptor != null) {
                disruptor.shutdown();
                disruptor = null;
            }
        }
    }

    @Override
    public RecordsOffset committableOffset() {

        RecordsOffset min = handlers[0].function.committableOffset();
        for (int i = 1; i < handlers.length; i++) {
            min = RecordsOffset.min(min, handlers[i].function.committableOffset());
        }
        return min;
    }

    @Override
    public long idleTimeMillis() {
        return Arrays.stream(handlers).mapToLong(DataHandler::idleTimeMillis).min().orElse(-1);
    }

    /**
     * get all handler. for unit test.
     *
     * @return list process functions
     */
    public List<AbstractFunction> getFunctions() {
        return Arrays.stream(handlers).map(v -> v.function).collect(Collectors.toList());
    }

    @Override
    public void idleTrigger() {
        for (DataHandler handler : handlers) {
            handler.idleTrigger();
        }
    }

    /** data handler. */
    public static class DataHandler implements WorkHandler<Block>, Closeable, Trigger {

        private final AbstractFunction function;
        private final Trigger trigger;
        private final AtomicReference<Throwable> error;

        public DataHandler(AbstractFunction function, AtomicReference<Throwable> error) {
            this.function = function;
            this.trigger = function instanceof Trigger ? (Trigger) function : null;
            this.error = error;
        }

        @Override
        public void onEvent(Block block) {
            if (Objects.nonNull(block) && block.getIterator() != null) {
                synchronized (this) {
                    try {
                        function.process(block.offset, block.getIterator());
                    } catch (Throwable e) {
                        if (!error.compareAndSet(null, e)) {
                            LOG.error("process function error", e);
                        }
                    }
                }
            }
        }

        @Override
        public void close() throws IOException {
            function.close();
        }

        public String functionId() {
            return function.context().id();
        }

        @Override
        public long idleTimeMillis() {
            return trigger == null ? -1 : trigger.idleTimeMillis();
        }

        @Override
        public void idleTrigger() {
            if (trigger != null) {
                try {
                    synchronized (this) {
                        trigger.idleTrigger();
                    }
                } catch (Throwable e) {
                    LOG.warn("trigger fail", e);
                }
            }
        }
    }

    /** block. */
    @Data
    static class Block {
        private Iterator<byte[]> iterator;
        private RecordsOffset offset;
    }

    /**
     * format cap as a power of 2.
     *
     * @param cap cap
     * @return new cap
     */
    static int formatCap(int cap) {
        int n = cap - 1;
        n |= n >>> 1;
        n |= n >>> 2;
        n |= n >>> 4;
        n |= n >>> 8;
        n |= n >>> 16;
        return (n < MIN_CAPACITY) ? MIN_CAPACITY : (n >= MAX_CAPACITY) ? MAX_CAPACITY : n + 1;
    }
}
