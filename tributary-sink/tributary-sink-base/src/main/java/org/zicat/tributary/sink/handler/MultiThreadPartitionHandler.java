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

import static org.zicat.tributary.sink.utils.Collections.copy;

import com.lmax.disruptor.TimeoutBlockingWaitStrategy;
import com.lmax.disruptor.WorkHandler;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zicat.tributary.channel.Channel;
import org.zicat.tributary.channel.Offset;
import org.zicat.tributary.common.ConfigOption;
import org.zicat.tributary.common.ConfigOptions;
import org.zicat.tributary.common.Threads;
import org.zicat.tributary.common.records.RecordsIterator;
import org.zicat.tributary.sink.SinkGroupConfig;
import org.zicat.tributary.sink.function.AbstractFunction;
import org.zicat.tributary.sink.function.Function;

import java.io.Closeable;
import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

/**
 * Disruptor sink handler.
 *
 * <p>Multi Threads mode, One ${@link MultiThreadPartitionHandler} instance bind with at least one
 * {@link Function} instance
 *
 * <p>Set threads and {@link Function} count by ${@link MultiThreadPartitionHandler#OPTION_THREADS}
 * .
 */
public class MultiThreadPartitionHandler extends AbstractPartitionHandler {

    private static final Logger LOG = LoggerFactory.getLogger(MultiThreadPartitionHandler.class);

    public static final ConfigOption<Integer> OPTION_THREADS =
            ConfigOptions.key("threads")
                    .integerType()
                    .description("consume threads per partition")
                    .defaultValue(2);

    public static final ConfigOption<Integer> OPTION_BUFFER_SIZE =
            ConfigOptions.key("buffer.size")
                    .integerType()
                    .description("the buffer size of memory queue")
                    .defaultValue(128);

    private static final int MAX_CAPACITY = 128;
    private static final int MIN_CAPACITY = 4;

    private Disruptor<Block> disruptor;
    private DataHandler[] handlers;
    private final AtomicReference<Throwable> error = new AtomicReference<>(null);
    private final int workerNumber;

    public MultiThreadPartitionHandler(
            String groupId,
            Channel channel,
            int partitionId,
            int workerNumber,
            SinkGroupConfig sinkGroupConfig) {
        super(groupId, channel, partitionId, sinkGroupConfig);
        this.workerNumber = workerNumber;
    }

    @Override
    public void open() {
        if (workerNumber <= 1) {
            throw new IllegalStateException(OPTION_THREADS.key() + " must over 1");
        }
        this.disruptor = createDisruptor();
        this.handlers = new DataHandler[workerNumber];
        for (int i = 0; i < workerNumber; i++) {
            handlers[i] = new DataHandler(createFunction(createFunctionId(i)), error);
            LOG.info("MultiThread Data handler initialed, GroupId:{}, Id:{}", groupId, i);
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
     * @return disruptor
     */
    protected Disruptor<Block> createDisruptor() {
        return new Disruptor<>(
                Block::new,
                formatCap(sinkGroupConfig.get(OPTION_BUFFER_SIZE)),
                Threads.createThreadFactoryByName(threadName() + "-", true),
                ProducerType.SINGLE,
                new TimeoutBlockingWaitStrategy(30, TimeUnit.SECONDS));
    }

    @Override
    public void process(Offset offset, Iterator<byte[]> iterator) throws Exception {
        checkProcessError();
        disruptor.publishEvent(
                (block, sequence) -> {
                    /*
                     *  After process finished, source iterator will fill next data from channel.
                     *  Put iterator to memory queue may cause data lost or exception,
                     *  because Consumer Thread may deal with the iterator later than next process.
                     */
                    final Iterator<byte[]> copy = copy(iterator);
                    block.setIterator(copy);
                    block.setOffset(offset);
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
    public Offset committableOffset() {

        Offset min = handlers[0].function.committableOffset();
        for (int i = 1; i < handlers.length; i++) {
            min = Offset.min(min, handlers[i].function.committableOffset());
        }
        return min;
    }

    @Override
    public List<AbstractFunction> getFunctions() {
        return Arrays.stream(handlers).map(v -> v.function).collect(Collectors.toList());
    }

    @Override
    public void snapshot() throws Exception {
        for (DataHandler handler : handlers) {
            handler.snapshot();
        }
    }

    /** data handler. */
    public static class DataHandler implements WorkHandler<Block>, Closeable {

        private final AbstractFunction function;
        private final AtomicReference<Throwable> error;

        public DataHandler(AbstractFunction function, AtomicReference<Throwable> error) {
            this.function = function;
            this.error = error;
        }

        @Override
        public void onEvent(Block block) {
            if (Objects.nonNull(block) && block.getIterator() != null) {
                synchronized (this) {
                    try {
                        function.process(block.offset, RecordsIterator.wrap(block.getIterator()));
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

        public void snapshot() throws Exception {
            function.snapshot();
        }

        public String functionId() {
            return function.context().id();
        }
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

    /** block. */
    public static class Block {

        private Iterator<byte[]> iterator;
        private Offset offset;

        public Iterator<byte[]> getIterator() {
            return iterator;
        }

        public void setIterator(Iterator<byte[]> iterator) {
            this.iterator = iterator;
        }

        public Offset getOffset() {
            return offset;
        }

        public void setOffset(Offset offset) {
            this.offset = offset;
        }
    }
}
